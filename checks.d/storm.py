#!/usr/bin/python

import time
import requests
from datadog_checks.base import AgentCheck

REST_API = {"cluster": "/api/v1/cluster/summary",
            "supervisor": "/api/v1/supervisor/summary",
            "topology": "/api/v1/topology/summary",
            "topology_details": "/api/v1/topology/"}
CLUSTER = ['supervisors', 'slotsTotal', 'slotsUsed', 'slotsFree', 'executorsTotal', 'tasksTotal']
SUPERVISOR = ['uptimeSeconds', 'slotsTotal', 'slotsUsed', 'totalMem', 'totalCpu', 'usedMem', 'usedCpu']
TOPOLOGY = ['uptimeSeconds', 'tasksTotal', 'workersTotal', 'executorsTotal', 'replicationCount', 'requestedMemOnHeap',
            'requestedMemOffHeap', 'requestedTotalMem', 'requestedCpu', 'assignedMemOnHeap', 'assignedMemOffHeap',
            'assignedTotalMem', 'assignedCpu']
TOPOLOGY_DETAILS = {
    'topologyStats': ['emitted', 'transferred','acked', 'failed'],
    'spouts': ['executors', 'emitted', 'transferred', 'acked', 'tasks', 'failed'],
    'bolts': ['executors', 'emitted', 'transferred', 'acked', 'tasks', 'executed', 'failed']
}


class Storm(AgentCheck):

    def __init__(self, name, init_config, agentConfig, instances=None):
        AgentCheck.__init__(self, name, init_config, agentConfig, instances)

    def check(self, instance):
        self.host = instance.get("host", 'localhost')
        self.port = instance.get("port", 8080)
        self.http_prefix = 'http://%s:%s' % (self.host, self.port)
        
        try:
            topology_ids = self._topology_loader()
            self._cluster_loader()
            self._supervisor_loader()
            self._topology_deatails_loader(topology_ids)
            self.gauge('storm.state', 0)
        except Exception as e:
            self.gauge('storm.state', 1)
            self.log.exception('exception collecting storm metrics %s' % e)
            
    def _cluster_loader(self):
        try:
            summary = self.request(REST_API["cluster"])
            ts = time.time()
            for metric in CLUSTER:
                self.gauge('storm.cluster.%s' % metric, summary[metric], timestamp=ts)
        except Exception as e:
            self.gauge('storm.state', 1)
            self.log.exception('exception collecting storm cluster metric form : %s \n %s' % ('%s%s' % (self.http_prefix, REST_API["cluster"]), e))

    def _supervisor_loader(self):
        try:
            jdata = self.request(REST_API["supervisor"])
            ts = time.time()
            for supervisor in jdata['supervisors']:
                for metric in SUPERVISOR:
                    tags = ['host:%s' % supervisor['host']]
                    self.gauge('storm.supervisor.%s' % metric, supervisor[metric], tags, timestamp=ts)
        except Exception as e:
            self.gauge('storm.state', 1)
            self.log.exception('exception collecting storm supervisor metric form : %s \n %s' % ('%s%s' % (self.http_prefix, REST_API["supervisor"]), e))

    def _topology_loader(self):
        ids =[]
        try:
            jdata = self.request(REST_API["topology"])
            ts = time.time()
            for topology in jdata['topologies']:
                ids.append(topology['id'])
                for metric in TOPOLOGY:
                    tags = ['host:%s' % self.host, 'name:%s' % topology['name']]
                    self.gauge('storm.topology.%s' % metric, topology[metric], tags, timestamp=ts)
        except Exception as e:
            self.gauge('storm.state', 1)
            self.log.exception('exception collecting storm topology metric form : %s \n %s' % ('%s%s' % (self.http_prefix, REST_API["supervisor"]), e))

        return ids

    def _topology_deatails_loader(self, ids):
        try:
            for id in ids:
                jdata = self.request('%s%s?%s' % (REST_API["topology_details"], id, "window=600"))
                ts = time.time()
                if jdata:
                    for topology in jdata['topologyStats']:
                        for metric in TOPOLOGY_DETAILS['topologyStats']:
                            # time window range
                            if topology['window'] == '600':
                                tags = ['topology_id:%s', self.remove_invalid_characters(id)]
                                self.gauge('storm.topology.topologyStats.%s' %  metric, topology[metric], tags, timestamp=ts)

                    for spouts in jdata['spouts']:
                        for metric in TOPOLOGY_DETAILS['spouts']:
                            tags = ['topology_id:%s', self.remove_invalid_characters(id), 'id:%s' % spouts['spoutId']]
                            self.gauge('storm.topology.spouts.%s' % metric, spouts[metric], tags, timestamp=ts)

                    for bolts in jdata['bolts']:
                        for metric in TOPOLOGY_DETAILS['bolts']:
                            tags = ['topology_id:%s', self.remove_invalid_characters(id), 'id:%s' % bolts['boltsId']]
                            self.gauge('storm.topology.bolts.%s' % metric, bolts[metric], tags, timestamp=ts)
        except Exception as e:
            self.gauge('storm.state', 1)
            self.log.exception('exception collecting storm topology details metric \n %s' % e)


    def request(self,uri):
        resp = requests.get('%s%s' % (self.http_prefix, uri))
        if resp.status_code != 200:
            raise HTTPError('%s%s' % (self.http_prefix, uri))

        return resp.json()

    def remove_invalid_characters(self, str):
        """removes characters unacceptable by opentsdb"""
        replaced = False
        lstr = list(str)
        for i, c in enumerate(lstr):
            if not (('a' <= c <= 'z') or ('A' <= c <= 'Z') or ('0' <= c <= '9') or c == '-' or c == '_' or
                    c == '.' or c == '/' or c.isalpha()):
                lstr[i] = '_'
                replaced = True
        if replaced:
            return "".join(lstr)
        else:
            return str


class HTTPError(RuntimeError):
    def __init__(self, resp):
        RuntimeError.__init__(self, str(resp))
        self.resp = resp