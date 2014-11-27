#!/usr/bin/python

# -*- coding: utf-8 -*-

# Copyright (C) 2009-2012:
#    Gabes Jean, naparuba@gmail.com
#    Gerhard Lausser, Gerhard.Lausser@consol.de
#    Gregory Starck, g.starck@gmail.com
#    Hartmut Goebel, h.goebel@goebel-consult.de
#
# This file is part of Shinken.
#
# Shinken is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Shinken is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with Shinken.  If not, see <http://www.gnu.org/licenses/>.

"""This Class is a plugin for the Shinken Broker. It is in charge
to brok information of the service/host perfdatas into the Graphite
backend. http://graphite.wikidot.com/start
"""

# TODO : Also buffering raw data, not only cPickle
# TODO : Better buffering like FIFO Buffer

import re
from socket import socket
import cPickle
import struct

from shinken.basemodule import BaseModule
from shinken.log import logger
from shinken.misc.perfdata import PerfDatas

properties = {
    'daemons': ['broker'],
    'type': 'graphite_perfdata',
    'external': False,
}


# Called by the plugin manager to get a broker
def get_instance(mod_conf):
    logger.info("[Graphite broker] Get a graphite data module for plugin %s" % mod_conf.get_name())
    instance = Graphite_broker(mod_conf)
    return instance


# Class for the Graphite Broker
# Get broks and send them to a Carbon instance of Graphite
class Graphite_broker(BaseModule):
    def __init__(self, modconf):
        BaseModule.__init__(self, modconf)
        self.host = getattr(modconf, 'host', 'localhost')
        self.use_pickle = getattr(modconf, 'use_pickle', '0') == '1'
        if self.use_pickle:
            self.port = int(getattr(modconf, 'port', '2004'))
        else:
            self.port = int(getattr(modconf, 'port', '2003'))
        self.tick_limit = int(getattr(modconf, 'tick_limit', '300'))
        # Used to reset check time into the scheduled time.
        # Carbon/graphite does not like latency data and creates blanks in graphs
        # Every data with "small" latency will be considered create at scheduled time
        self.ignore_latency_limit = \
            int(getattr(modconf, 'ignore_latency_limit', '0'))
        if self.ignore_latency_limit < 0:
            self.ignore_latency_limit = 0
        self.buffer = []
        self.ticks = 0
        self.host_dict = {}
        self.svc_dict = {}
        self.multival = re.compile(r'_(\d+)$')
        self.chunk_size = 200
        self.max_chunk_size = 100000

        # optional "sub-folder" in graphite to hold the data of a specific host
        self.graphite_data_source = \
            self.illegal_char.sub('_', getattr(modconf, 'graphite_data_source', ''))


    # Called by Broker so we can do init stuff
    # TODO: add conf param to get pass with init
    # Conf from arbiter!
    def init(self):
        logger.info("[Graphite broker] I init the %s server connection to %s:%d" %
                    (self.get_name(), str(self.host), self.port))
        try:
            self.con = socket()
            self.con.connect((self.host, self.port))
        except IOError, err:
                logger.error("[Graphite broker] Graphite Carbon instance network socket!"
                             " IOError:%s" % str(err))
                raise
        logger.info("[Graphite broker] Connection successful to  %s:%d"
                    % (str(self.host), self.port))

    # Sending data to Carbon. In case of failure, try to reconnect and send again.
    # If carbon instance is down, data are buffered.
    def send_packet(self, p):
        try:
            self.con.sendall(p)
        except IOError:
            logger.error("[Graphite broker] Failed sending data to the Graphite Carbon instance !"
                         " Trying to reconnect ... ")
            try:
                self.init()
                self.con.sendall(p)
            except IOError:
                raise

    # For a perf_data like /=30MB;4899;4568;1234;0  /var=50MB;4899;4568;1234;0 /toto=
    # return ('/', '30'), ('/var', '50')
    def get_metric_and_value(self, perf_data):
        res = []
        metrics = PerfDatas(perf_data)

        for e in metrics:
            #try:
            #    logger.debug("[Graphite broker] Groking: %s" % str(e))
            #except UnicodeEncodeError:
            #    pass

            name = self.illegal_char.sub('_', e.name)
            name = self.multival.sub(r'.\1', name)

            # get metric value and its thresholds values if they exist
            name_value = {name: e.value}
            if e.warning and e.critical:
                name_value[name + '_warn'] = e.warning
                name_value[name + '_crit'] = e.critical
            # bailout if need
            if name_value[name] == '':
                continue

            #try:
            #    logger.debug("[Graphite broker] End of grok: %s, %s" % (name, str(e.value)))
            #except UnicodeEncodeError:
            #    pass
            for key, value in name_value.items():
                res.append((key, value))
        return res

    # Prepare service custom vars
    def manage_initial_service_status_brok(self, b):
        self.svc_dict[(b.data['host_name'], b.data['service_description'])] = b.data['customs']

    # Prepare host custom vars
    def manage_initial_host_status_brok(self, b):
        # store custom variables
        # as well as the host check_command
        self.host_dict[b.data['host_name']] = {
            'customs': b.data['customs'],
            'check_command': b.data['check_command'].command,
            'address': b.data['address']
        }

    # returns the proper graphite metrics name
    def build_metrics_name(self, host_name, service_description, check_type):
        # if no customs for host found we exit
        if not host_name in self.host_dict:
            logger.error("[Graphite broker] Failed to find custom variables for Host %s." % (host_name,))
            return

        # If this is a service check we should also check for
        # service custom data and if no customs for service found we exit
        if check_type == 'service' and not (host_name, service_description) in self.svc_dict:
            logger.error("[Graphite broker] Failed to find custom variables for Service %s:%s." % (host_name, service_description))
            return

        try:
            # always get host custom data
            custom_host_data = self.host_dict[host_name]
            customs = custom_host_data['customs']
            custom_service_data = {}

            # if this is a service check we also get service custom data
            if check_type == 'service':
                custom_service_data = self.svc_dict[(host_name, service_description)]

            region = 'region=%s' % customs.get('_AWS_REGION', 'no-region-found')
            az = 'az=%s' % customs.get('_AWS_AZ', 'no-az-found')
            asg = 'asg=%s' % customs.get('_AWS_ASG', 'no-asg-found')
            ip = 'ip=%s' % self.illegal_char.sub('-', custom_host_data['address'])
            ami = 'ami=%s' % customs.get('_AWS_AMI_ID', 'no-ami-found')
            m_type = custom_service_data.get('_METRIC_TYPE', 'gauges')
            service = customs.get('_AWS_SERVICE', host_name)

            hname = '.'.join(('host', ip, region, az, asg, ami, m_type, service))
        except:
            logger.error("[Graphite broker] Failed to Build Key For %s:%s." % (host_name, service_description))
            return

        desc = self.illegal_char.sub('_', service_description)

        if self.graphite_data_source:
            path = '.'.join((hname, self.graphite_data_source, desc))
        else:
            path = '.'.join((hname, desc))

        return path

    # returs the check time for the given data
    def get_time(self, data):
        if self.ignore_latency_limit >= data['latency'] > 0:
            check_time = int(data['last_chk']) - int(data['latency'])

            # have nice-ish logger message based in check type
            if 'service_description' in data:
                check_type = 'service'
                check_name = data['service_description']
            else:
                check_type = 'host'
                check_name = data['host_name']

            logger.info("[Graphite broker] Ignoring latency for %s %s. Latency : %s",
                check_type, check_name, data['latency'])
        else:
            check_time = int(data['last_chk'])

        return check_time

    # sends the metric to graphite
    def send_metrics(self, couples, path, check_time):
        if self.use_pickle:
            # Buffer the performance data lines
            for (metric, value) in couples:
                self.buffer.append(("%s.%s" % (path, metric),
                                   ("%d" % check_time, "%s" % value)))
        else:
            lines = []
            # Send a bulk of all metrics at once
            for (metric, value) in couples:
                lines.append("%s.%s %s %d" % (path, metric, value, check_time))
            packet = '\n'.join(lines) + '\n'  # Be sure we put \n every where
            try:
                self.send_packet(packet)
            except IOError:
                logger.error("[Graphite broker] Failed sending to the Graphite Carbon."
                             " Data are lost")

    # A service check result brok has just arrived, we UPDATE data info with this
    def manage_service_check_result_brok(self, b):
        data = b.data
        perf_data = data['perf_data']
        couples = self.get_metric_and_value(perf_data)

        # If no values, we can exit now
        if len(couples) == 0:
            return

        path = self.build_metrics_name(host_name=data['hostname'], service_description=data['service_description'], check_type='service')
        if not path:
            logger.error('Could not build metrics name for %s:%s' % (data['hostname'], data['service_description']))
            return

        check_time = self.get_time(data)
        self.send_metrics(couples, path, check_time)

    # A host check result brok has just arrived, we UPDATE data info with this
    def manage_host_check_result_brok(self, b):
        data = b.data
        perf_data = data['perf_data']
        couples = self.get_metric_and_value(perf_data)

        # If no values, we can exit now
        if len(couples) == 0:
            return

        # instead of service description we will use the host check_command
        host_name = data['host_name']
        if host_name not in self.host_dict:
            logger.error('Not custom data available for %s' % (host_name,))
            return

        service_description = self.host_dict[host_name]['check_command']
        path = self.build_metrics_name(host_name=host_name, service_description=service_description, check_type='host')
        if not path:
            logger.error('Could not build metrics name for %s:%s' % (data['hostname'], data['service_description']))
            return

        check_time = self.get_time(data)
        self.send_metrics(couples, path, check_time)

    def hook_tick(self, brok):
        """Each second the broker calls the hook_tick function
           Every tick try to flush the buffer
        """
        if self.use_pickle:
            if self.ticks >= self.tick_limit:
                # If the number of ticks where data was not
                # sent successfully to Graphite reaches the bufferlimit.
                # Reset the buffer and reset the ticks
                logger.error("[Graphite broker] Buffering time exceeded. Freeing buffer")
                self.buffer = []
                self.ticks = 0
                return

            while len(self.buffer) > 0:
                try:
                    self.chunk_size = int(self.chunk_size)
                    buf2 = self.buffer[:self.chunk_size]
                    self.con.sendall(self.create_pack(buf2))
                    self.buffer = self.buffer[self.chunk_size:]
                    self.chunk_size = min(self.max_chunk_size, self.chunk_size * 1.5)
                except IOError:
                    self.max_chunk_size = self.chunk_size
                    self.chunk_size /= 1.5
                    self.con.close()

                    try:
                        self.init()
                    except IOError:
                        logger.error("[Graphite broker] Sending data Failed. Buffering state : %s / %s"
                                     % (self.ticks, self.tick_limit))
                        self.ticks += 1
                        return

            self.ticks = 0

    def create_pack(self, buff):
        payload = cPickle.dumps(buff)
        header = struct.pack("!L", len(payload))
        packet = header + payload
        return packet
