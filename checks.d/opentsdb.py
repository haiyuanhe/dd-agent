#!/usr/bin/python

import requests
from datadog_checks.base import AgentCheck

STATS_HTTP_API = "/api/stats"


class Opentsdb(AgentCheck):

    def __init__(self, name, init_config, agentConfig, instances=None):
        AgentCheck.__init__(self, name, init_config, agentConfig, instances)

    def check(self, instance):
        host = instance.get("host", 'localhost')
        port = instance.get("port", 4242)
        http_prefix = 'http://%s:%s' % (host, port)

        try:
            url = '%s%s' % (http_prefix, STATS_HTTP_API)
            stats = self.request(url)
            if stats:
                for metric in stats:
                    self.printmetric(metric['metric'], metric['timestamp'], metric['value'], **metric['tags'])
            self.gauge("opentsdb.state", 0)
        except Exception as e:
            self.gauge("opentsdb.state", 1)
            self.log.error("opentsdb collector except exception , abort %s" % e)

    def request(self, url):
        resp = requests.get(url)
        if resp.status_code != 200:
            raise RuntimeError(url)

        return resp.json()

    def printmetric(self, metric, ts, value, **tags):
        new_tags = []
        if tags:
            for name, value in tags.iteritems():
                new_tags = new_tags + ['%s:%s' % (name, value)]
        self.gauge(metric, value, new_tags, timestamp=ts)
