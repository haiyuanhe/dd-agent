#!/usr/bin/env python
# This file is part of tcollector.
# Copyright (C) 2010  The tcollector Authors.
#
# This program is free software: you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version.  This program is distributed in the hope that it
# will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
# of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
# General Public License for more details.  You should have received a copy
# of the GNU Lesser General Public License along with this program.  If not,
# see <http://www.gnu.org/licenses/>.

import time

try:
    import json
except ImportError:
    json = None

from utils import http
from utils.hadoop.hadoop_http import HadoopHttp
from datadog_checks.base import AgentCheck

EXCLUDED_CONTEXTS = ('regionserver', 'regions', )

METRICS = (
           'hbase.master.assignmentmanger.ritOldestAge',
           'hbase.master.assignmentmanger.ritCountOverThreshold',
           'hbase.master.assignmentmanger.ritCount',
           'hbase.master.assignmentmanger.Assign_min',
           'hbase.master.assignmentmanger.Assign_max',
           'hbase.master.assignmentmanger.Assign_mean',
           'hbase.master.assignmentmanger.Assign_median',
           'hbase.master.assignmentmanger.Assign_99th_percentile',
           'hbase.master.ipc.queueSize',
           'hbase.master.ipc.numCallsInGeneralQueue',
           'hbase.master.ipc.numCallsInReplicationQueue',
           'hbase.master.ipc.numCallsInPriorityQueue',
           'hbase.master.ipc.numOpenConnections',
           'hbase.master.ipc.numActiveHandler',
           'hbase.master.ipc.TotalCallTime_max',
           'hbase.master.ipc.TotalCallTime_mean',
           'hbase.master.ipc.TotalCallTime_median',
           'hbase.master.ipc.TotalCallTime_99th_percentile',
           'hbase.master.ipc.receivedBytes',
           'hbase.master.ipc.sentBytes',
           'hbase.master.ipc.QueueCallTime_99th_percentile',
           'hbase.master.ipc.ProcessCallTime_99th_percentile',
           'hbase.master.Memory.HeapMemoryUsage.used',
           'hbase.master.Memory.HeapMemoryUsage.max',
           'hbase.master.jvmmetrics.GcTimeMillis',
           'hbase.master.jvmmetrics.GcTimeMillisParNew',
           'hbase.master.jvmmetrics.GcTimeMillisConcurrentMarkSweep',
           'hbase.master.jvmmetrics.GcCount',
           'hbase.master.jvmmetrics.GcCountConcurrentMarkSweep',
           'hbase.master.jvmmetrics.GcCountParNew',
           'hbase.master.jvmmetrics.LogError',
           'hbase.master.jvmmetrics.LogFatal',
           'hbase.master.server.averageLoad',
           'hbase.master.server.clusterRequests',
           'hbase.master.server.numRegionServers',
           'hbase.master.server.numDeadRegionServers'
           )


class HbaseMaster(AgentCheck):

    def __init__(self, name, init_config, agentConfig, instances=None):
        AgentCheck.__init__(self, name, init_config, agentConfig, instances)
        self.default_ip = agentConfig.get('ip', '127.0.0.1')
        self.service = "hbase"
        self.daemon = "master"

    def check(self, instance):
        custom_tags = instance.get('tags', [])
        host = instance.get('host', self.default_ip)
        port = instance.get('port', 16010)
        metric = "hbase.master.state"
        try:
            with http.lower_privileges(self.log):
                if json:
                    self.emit(host, port, METRICS, custom_tags)
                    self.gauge(metric, 0)
                else:
                    self.gauge(metric, 1)
                    self.log.error("This collector requires the `json' Python module.")
        except Exception, e:
            self.gauge(metric, 1)
            self.log.error("metric is %s error is %s" % (metric, str(e)))

    def emit(self, host, port, filter_list=None, custom_tags=None):
        current_time = int(time.time())
        metrics = HadoopHttp(self.service, self.daemon, host, port, self.log).poll()
        for context, metric_name, value in metrics:
            if any(c in EXCLUDED_CONTEXTS for c in context):
                continue
            if filter_list:
                check = "{0}.{1}.{2}.{3}".format(self.service, self.daemon, ".".join(context), metric_name)
                if check not in filter_list:
                    continue
            self.emit_metric(context, current_time, metric_name, value, custom_tags)

    def emit_metric(self, context, current_time, metric_name, value, tag_dict=None):
        metric = "%s.%s.%s.%s" % (self.service, self.daemon, ".".join(context), metric_name)
        if not tag_dict:
            self.gauge(metric, value, timestamp=current_time)
        else:
            tag_string = ([k + ":" + v for k, v in tag_dict.iteritems()])
            self.gauge(metric, value, tag_string, timestamp=current_time)
