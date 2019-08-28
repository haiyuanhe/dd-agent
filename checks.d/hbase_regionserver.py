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
import re

try:
    import json
except ImportError:
    json = None

from utils import http
from utils.hadoop.hadoop_http import HadoopHttp
from datadog_checks.base import AgentCheck

EMIT_REGION = True

EXCLUDED_CONTEXTS = ("master")
REGION_METRIC_PATTERN = re.compile(r"[N|n]amespace_(.*)_table_(.*)_region_(.*)_metric_(.*)")


METRICS = (
           'hbase.regionserver.ipc.queueSize',
           'hbase.regionserver.ipc.numOpenConnections',
           'hbase.regionserver.ipc.numActiveHandler',
           'hbase.regionserver.ipc.TotalCallTime_max',
           'hbase.regionserver.ipc.TotalCallTime_mean',
           'hbase.regionserver.ipc.TotalCallTime_median',
           'hbase.regionserver.ipc.TotalCallTime_99th_percentile',
           'hbase.regionserver.replication.sink.ageOfLastAppliedOp',
           'hbase.regionserver.replication.sink.appliedOps',
           'hbase.regionserver.replication.sink.appliedBatches'
           'hbase.regionserver.metricssystem.stats.NumAllSources',
           'hbase.regionserver.replication.sink.appliedOps',
           'hbase.regionserver.server.regionCount',
           'hbase.regionserver.server.storeCount',
           'hbase.regionserver.server.hlogFileSize',
           'hbase.regionserver.server.hlogFileCount',
           'hbase.regionserver.regions.memStoreSize'
           'hbase.regionserver.server.storeFileSize',
           'hbase.regionserver.server.totalRequestCount',
           'hbase.regionserver.server.readRequestCount',
           'hbase.regionserver.server.writeRequestCount',
           'hbase.regionserver.server.checkMutateFailedCount',
           'hbase.regionserver.server.checkMutatePassedCount',
           'hbase.regionserver.server.storeFileIndexSize',
           'hbase.regionserver.server.staticIndexSize',
           'hbase.regionserver.server.staticBloomSize',
           'hbase.regionserver.server.mutationsWithoutWALCount',
           'hbase.regionserver.server.mutationsWithoutWALSize',
           'hbase.regionserver.server.percentFilesLocal',
           'hbase.regionserver.server.percentFilesLocalSecondaryRegions',
           'hbase.regionserver.server.splitQueueLength',
           'hbase.regionserver.server.compactionQueueLength',
           'hbase.regionserver.server.flushQueueLength',
           'hbase.regionserver.server.blockCacheFreeSize',
           'hbase.regionserver.server.blockCacheCount',
           'hbase.regionserver.server.blockCacheSize',
           'hbase.regionserver.server.blockCacheHitCount',
           'hbase.regionserver.server.blockCacheMissCount',
           'hbase.regionserver.server.blockCacheEvictionCount',
           'hbase.regionserver.server.blockCacheExpressHitPercent',
           'hbase.regionserver.server.blockCacheCountHitPercent',
           'hbase.regionserver.server.updatesBlockedTime',
           'hbase.regionserver.server.compactedCellsCount',
           'hbase.regionserver.server.flushedCellsCount',
           'hbase.regionserver.server.compactedCellsSize',
           'hbase.regionserver.server.blockedRequestCount',
           'hbase.regionserver.server.Mutate_num_ops',
           'hbase.regionserver.server.Mutate_min',
           'hbase.regionserver.server.Mutate_max',
           'hbase.regionserver.server.Mutate_mean',
           'hbase.regionserver.server.Mutate_median',
           'hbase.regionserver.server.Mutate_99th_percentile',
           'hbase.regionserver.server.slowDeleteCount',
           'hbase.regionserver.server.Append_num_ops',
           'hbase.regionserver.server.Increment_num_ops',
           'hbase.regionserver.server.Increment_min',
           'hbase.regionserver.server.Increment_max',
           'hbase.regionserver.server.Increment_median',
           'hbase.regionserver.server.Increment_mean',
           'hbase.regionserver.server.Increment_99th_percentile',
           'hbase.regionserver.server.Replay_num_ops',
           'hbase.regionserver.server.Replay_min',
           'hbase.regionserver.server.Replay_max',
           'hbase.regionserver.server.Replay_mean',
           'hbase.regionserver.server.Replay_median',
           'hbase.regionserver.server.Replay_99th_percentile',
           'hbase.regionserver.server.FlushTime_num_ops',
           'hbase.regionserver.server.FlushTime_min',
           'hbase.regionserver.server.FlushTime_max',
           'hbase.regionserver.server.FlushTime_mean',
           'hbase.regionserver.server.FlushTime_median',
           'hbase.regionserver.server.FlushTime_99th_percentile',
           'hbase.regionserver.server.Delete_num_ops',
           'hbase.regionserver.server.Delete_min',
           'hbase.regionserver.server.Delete_max',
           'hbase.regionserver.server.Delete_mean',
           'hbase.regionserver.server.Delete_median',
           'hbase.regionserver.server.Delete_99th_percentile',
           'hbase.regionserver.server.splitRequestCount',
           'hbase.regionserver.server.splitSuccessCount',
           'hbase.regionserver.server.slowGetCount',
           'hbase.regionserver.server.Get_num_ops',
           'hbase.regionserver.server.Get_min',
           'hbase.regionserver.server.Get_max',
           'hbase.regionserver.server.Get_mean',
           'hbase.regionserver.server.Get_median',
           'hbase.regionserver.server.Get_99th_percentile',
           'hbase.regionserver.server.ScanNext_99th_percentile',
           'hbase.regionserver.server.ScanNext_num_ops',
           'hbase.regionserver.server.ScanNext_min',
           'hbase.regionserver.server.ScanNext_max',
           'hbase.regionserver.server.ScanNext_mean',
           'hbase.regionserver.server.ScanNext_median',
           'hbase.regionserver.server.ScanNext_99th_percentile',
           'hbase.regionserver.server.slowPutCount',
           'hbase.regionserver.server.slowIncrementCount',
           'hbase.regionserver.server.SplitTime_num_ops',
           'hbase.regionserver.server.SplitTime_min',
           'hbase.regionserver.server.SplitTime_max',
           'hbase.regionserver.server.SplitTime_mean',
           'hbase.regionserver.server.SplitTime_median',
           'hbase.regionserver.server.SplitTime_99th_percentile',
           'hbase.regionserver.wal.AppendSize_num_ops',
           'hbase.regionserver.wal.AppendSize_min',
           'hbase.regionserver.wal.AppendSize_max',
           'hbase.regionserver.wal.AppendSize_mean',
           'hbase.regionserver.wal.AppendSize_median',
           'hbase.regionserver.wal.AppendSize_99th_percentile',
           'hbase.regionserver.wal.SyncTime_num_ops',
           'hbase.regionserver.wal.SyncTime_min',
           'hbase.regionserver.wal.SyncTime_max',
           'hbase.regionserver.wal.SyncTime_mean',
           'hbase.regionserver.wal.SyncTime_median',
           'hbase.regionserver.wal.SyncTime_99th_percentile',
           'hbase.regionserver.wal.slowAppendCount',
           'hbase.regionserver.wal.rollRequest',
           'hbase.regionserver.wal.appendCount',
           'hbase.regionserver.wal.lowReplicaRollRequest',
           'hbase.regionserver.wal.AppendTime_num_ops',
           'hbase.regionserver.wal.AppendTime_min',
           'hbase.regionserver.wal.AppendTime_max',
           'hbase.regionserver.wal.AppendTime_mean',
           'hbase.regionserver.wal.AppendTime_median',
           'hbase.regionserver.wal.AppendTime_99th_percentile',
           'hbase.regionserver.jvmmetrics.MemNonHeapUsedM',
           'hbase.regionserver.jvmmetrics.MemNonHeapCommittedM',
           'hbase.regionserver.jvmmetrics.MemHeapMaxM',
           'hbase.regionserver.jvmmetrics.MemHeapUsedM',
           'hbase.regionserver.jvmmetrics.MemNonHeapMaxM',
           'hbase.regionserver.jvmmetrics.MemHeapCommittedM',
           'hbase.regionserver.jvmmetrics.MemMaxM',
           'hbase.regionserver.jvmmetrics.GcCountParNew',
           'hbase.regionserver.jvmmetrics.GcTimeMillisParNew',
           'hbase.regionserver.jvmmetrics.GcTimeMillisConcurrentMarkSweep',
           'hbase.regionserver.jvmmetrics.GcCountConcurrentMarkSweep',
           'hbase.regionserver.jvmmetrics.GcCount',
           'hbase.regionserver.info.readRequestsCount',
           'hbase.regionserver.info.writeRequestsCount',
           'hbase.regionserver.info.storefileSizeMB',
           'hbase.regionserver.jvmmetrics.GcTimeMillis',
           )


class HbaseRegionserver(AgentCheck):
    def __init__(self, name, init_config, agentConfig, instances=None):
        AgentCheck.__init__(self, name, init_config, agentConfig, instances)
        self.default_ip = agentConfig.get('ip', '127.0.0.1')
        self.service = "hbase"
        self.daemon = "regionserver"

    def check(self, instance):
        custom_tags = instance.get('tags', [])
        host = instance.get('host', self.default_ip)
        port = instance.get('port', 16030)
        metric = "hbase.regionserver.state"
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

    def emit_region_metric(self, context, current_time, full_metric_name, value):
        match = REGION_METRIC_PATTERN.match(full_metric_name)
        if not match:
            self.log.error("Error splitting %s" % full_metric_name)
            return

        namespace = match.group(1)
        table = match.group(2)
        region = match.group(3)
        metric_name = match.group(4)
        tag_dict = {"namespace": namespace, "table": table, "region": region}

        if any(not v for k,v in tag_dict.iteritems()):
            self.log.error("Error splitting %s" % full_metric_name)
        else:
            self.emit_metric(context, current_time, metric_name, value, tag_dict)

    def emit(self, host, port, filter_list=None, custom_tags=None):
        """
        Emit metrics from a HBase regionserver.

        This will only emit per region metrics is EMIT_REGION is set to true
        """
        current_time = int(time.time())
        metrics = HadoopHttp(self.service, self.daemon, host, port, self.log).poll()
        for context, metric_name, value in metrics:
            if any(c in EXCLUDED_CONTEXTS for c in context):
                continue

            if filter_list:
                check = "{0}.{1}.{2}.{3}".format(self.service, self.daemon, ".".join(context), metric_name)
                if check not in filter_list:
                    continue
            if any(c == "regions" for c in context):
                if EMIT_REGION:
                    self.emit_region_metric(context, current_time, metric_name, value)
            else:
                self.emit_metric(context, current_time, metric_name, value)

    def emit_metric(self, context, current_time, metric_name, value, tag_dict=None):
        metric = "%s.%s.%s.%s" % (self.service, self.daemon, ".".join(context), metric_name)
        if not tag_dict:
            self.gauge(metric, value, timestamp=current_time)
        else:
            tag_string = ([k + ":" + v for k, v in tag_dict.iteritems()])
            self.gauge(metric, value, tag_string, timestamp=current_time)
