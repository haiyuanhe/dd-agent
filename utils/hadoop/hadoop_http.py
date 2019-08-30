#!/usr/bin/python
# This file is part of tcollector.
# Copyright (C) 2011-2013  The tcollector Authors.
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
import sys
try:
    import json
    import requests
except ImportError:
    json = None
try:
    from collections import OrderedDict  # New in Python 2.7
except ImportError:
    from ordereddict import OrderedDict  # Can be easy_install'ed for <= 2.6

EXCLUDED_KEYS = (
    "Name",
    "name"
)


class HadoopHttp(object):
    def __init__(self, service, daemon, host, port, logger, uri="/jmx"):
        self.service = service
        self.daemon = daemon
        self.port = port
        self.host = host
        self.uri = uri
        self.http_prefix = 'http://%s:%s' % (self.host, self.port)
        self.logger = logger

    def request(self):
        try:
            req=requests.get('%s%s' % (self.http_prefix, self.uri))
            req.raise_for_status()
            resp =req.content
        except:
            self.logger.exception('hadoop_http request failed: %s from %s', sys.exc_info()[0], '%s%s' % (self.http_prefix, self.uri))
            return None
        return json.loads(resp)

    def is_numeric(self, value):
        return isinstance(value, (int, long, float)) and (not isinstance(value, bool))

    def poll(self):
        """
        Get metrics from the http server's /jmx page, and transform them into normalized tupes

        @return: array of tuples ([u'Context', u'Array'], u'metricName', value)
        """
        json_arr = self.request().get('beans', [])
        kept = []
        for bean in json_arr:
            try:
                if (bean['name']) and (bean['name'].startswith('java.lang:type=GarbageCollector')):
                    self.process_gc_collector(bean, kept)
                elif (bean['name']) and (bean['name'].startswith('java.lang:type=')):
                    self.process_java_lang_metrics(bean, kept)
                elif (bean['name']) and ("name=" in bean['name']):
                    # split the name string
                    context = bean['name'].split("name=")[1].split(",sub=")
                    # Create a set that keeps the first occurrence
                    context = OrderedDict.fromkeys(context).keys()
                    # lower case and replace spaces.
                    context = [c.lower().replace(" ", "_") for c in context]
                    # don't want to include the service or daemon twice
                    context = [c for c in context if c != self.service and c != self.daemon]
                    for key, value in bean.iteritems():
                        if key in EXCLUDED_KEYS:
                            continue
                        if not self.is_numeric(value):
                            continue
                        kept.append((context, key, value))
            except Exception as e:
                self.logger.exception("exception in HadoopHttp when collecting %s", bean['name'])

        return kept

    def emit(self):
        pass

    def process_gc_collector(self, bean, kept):
        context = bean['name'].split("java.lang:type=")[1].split(",name=")
        for key, value in bean.iteritems():
            if key in EXCLUDED_KEYS:
                continue
            if value is None:
                continue
            if key == 'LastGcInfo':
                context.append(key)
                for lastgc_key, lastgc_val in bean[key].iteritems():
                    if lastgc_key == 'memoryUsageAfterGc' or lastgc_key == 'memoryUsageBeforeGc':
                        context.append(lastgc_key)
                        for memusage in lastgc_val:      # lastgc_val is a list
                            context.append(memusage["key"])
                            for final_key, final_val in memusage["value"].iteritems():
                                safe_context, safe_final_key = self.safe_replace(context, final_key)
                                kept.append((safe_context, safe_final_key, final_val))
                            context.pop()
                        context.pop()
                    elif self.is_numeric(lastgc_val):
                        safe_context, safe_lastgc_key = self.safe_replace(context, lastgc_key)
                        kept.append((safe_context, safe_lastgc_key, lastgc_val))
                context.pop()
            elif self.is_numeric(value):
                safe_context, safe_key = self.safe_replace(context, key)
                kept.append((safe_context, safe_key, value))

    def process_java_lang_metrics(self, bean, kept):
        context = bean['name'].split("java.lang:type=")[1].split(",name=")
        for key, value in bean.iteritems():
            if key in EXCLUDED_KEYS:
                continue
            if value is None:
                continue
            if self.is_numeric(value):
                safe_context, safe_key = self.safe_replace(context, key)
                kept.append((safe_context, safe_key, value))
            elif isinstance(value, dict):   # only go one level deep since there is no other level empirically
                for subkey, subvalue in value.iteritems():
                    if self.is_numeric(subvalue):
                        safe_context, final_key = self.safe_replace(context, key + "." + subkey)
                        kept.append((safe_context, final_key, subvalue))

    def safe_replace(self, context, key):
        context = [c.replace(" ", "_") for c in context]
        key = key.replace(" ", "_")
        return context, key
