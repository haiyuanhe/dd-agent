#!/usr/bin/python

import time
import urllib2
import re
from utils import http
from datadog_checks.base import AgentCheck


class Cloudmon(AgentCheck):

    def __init__(self, name, init_config, agentConfig, instances=None):
        AgentCheck.__init__(self, name, init_config, agentConfig, instances)

    def check(self, instance):
        with http.lower_privileges(self.log):
            try:
                urls = instance.get('stats_urls')
                for url in urls:
                    response = urllib2.urlopen(url)
                    content = response.read()
                    self.process(content, url)
            except Exception as e:
                self.log.error("unexpected error is %s" % str(e))

    def process(self, content, url=None):
        ts = time.time()
        stype = ''
        for s in [tk.strip() for tk in content.splitlines()]:
            if s == 'counters:':
                stype = 'counters'
            elif s == 'metrics:':
                stype = 'metrics'
            elif s == 'gauges:':
                stype = 'gauges'
            elif s == 'labels:':
                stype = 'labels'
            else:
                comps = [ss.strip() for ss in s.split(':')]
                metric_name = comps[0]
                if stype == 'counters' or stype == 'gauges':
                    val = int(comps[1])
                    if stype == 'counters':
                        tags = ['metric_type:counter', 'source:%s' % url]
                        self.gauge(metric_name, val, tags, timestamp=ts)
                    else:
                        tags = ['source:%s' % url]
                        self.gauge(metric_name, val, tags, timestamp=ts)
                elif stype == 'metrics':
                    vals = [sss.strip(" ()") for sss in re.split(',|=', comps[1])]

                    tags = ['source:%s' % url]
                    if "average" in vals:
                        val_avg = self.get_metric_value("average", vals)
                        self.gauge(metric_name, val_avg, tags, timestamp=ts)

                    if "maximum" in vals:
                        val_max = self.get_metric_value("maximum", vals)
                        self.gauge("%s.%s" % (metric_name, "max"), val_max, tags, timestamp=ts)

                    if "minimum" in vals:
                        val_min = self.get_metric_value("minimum", vals)
                        self.gauge("%s.%s" % (metric_name, "min"), val_min, tags, timestamp=ts)

                    if "p99" in vals:
                        val_p99 = self.get_metric_value("p99", vals)
                        self.gauge("%s.%s" % (metric_name, "p99"), val_p99, tags, timestamp=ts)

                    if "p999" in vals:
                        val_p999 = self.get_metric_value("p999", vals)
                        self.gauge("%s.%s" % (metric_name, "p999"), val_p999, tags, timestamp=ts)
                else:
                    self.log.warning('unexpected metric type %s', stype)
                    pass

    def get_metric_value(self, agg, vals):
        idx = vals.index(agg)
        return int(vals[idx + 1])