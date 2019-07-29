# (C) Datadog, Inc. 2018
# All rights reserved
# Licensed under Simplified BSD License (see LICENSE)
import re
import time
from datetime import datetime
from itertools import chain

import requests
import simplejson as json
from six import PY3, iteritems, text_type
from six.moves.urllib.parse import urlparse

from datadog_checks.base import AgentCheck, ConfigurationError
from datadog_checks.base.utils.headers import headers
# (C) Datadog, Inc. 2018
# All rights reserved
# Licensed under a 3-clause BSD style license (see LICENSE)


# Map metrics from vhost_traffic_status to metrics from NGINX Plus
VTS_METRIC_MAP = {
    'nginx.loadMsec': 'nginx.load_timestamp',
    'nginx.nowMsec': 'nginx.timestamp',
    'nginx.connections.accepted': 'nginx.connections.accepted',
    'nginx.connections.active': 'nginx.connections.active',
    'nginx.connections.reading': 'nginx.net.reading',
    'nginx.connections.writing': 'nginx.net.writing',
    'nginx.connections.waiting': 'nginx.net.waiting',
    'nginx.connections.requests': 'nginx.requests.total',
    'nginx.server_zone.requestCounter': 'nginx.server_zone.requests',
    'nginx.server_zone.responses.1xx': 'nginx.server_zone.responses.1xx',
    'nginx.server_zone.responses.2xx': 'nginx.server_zone.responses.2xx',
    'nginx.server_zone.responses.3xx': 'nginx.server_zone.responses.3xx',
    'nginx.server_zone.responses.4xx': 'nginx.server_zone.responses.4xx',
    'nginx.server_zone.responses.5xx': 'nginx.server_zone.responses.5xx',
    'nginx.server_zone.inBytes': 'nginx.server_zone.received',
    'nginx.server_zone.outBytes': 'nginx.server_zone.sent',
    'nginx.upstream.requestCounter': 'nginx.upstream.peers.requests',
    'nginx.upstream.inBytes': 'nginx.upstream.peers.received',
    'nginx.upstream.outBytes': 'nginx.upstream.peers.sent',
    'nginx.upstream.responses.1xx': 'nginx.upstream.peers.responses.1xx',
    'nginx.upstream.responses.2xx': 'nginx.upstream.peers.responses.2xx',
    'nginx.upstream.responses.3xx': 'nginx.upstream.peers.responses.3xx',
    'nginx.upstream.responses.4xx': 'nginx.upstream.peers.responses.4xx',
    'nginx.upstream.responses.5xx': 'nginx.upstream.peers.responses.5xx',
    'nginx.upstream.weight': 'nginx.upstream.peers.weight',
    'nginx.upstream.backup': 'nginx.upstream.peers.backup',
    'nginx.upstream.down': 'nginx.upstream.peers.health_checks.last_passed',
}

METRICS_SEND_AS_COUNT = [
    'nginx.upstream.peers.responses.1xx',
    'nginx.upstream.peers.responses.2xx',
    'nginx.upstream.peers.responses.3xx',
    'nginx.upstream.peers.responses.4xx',
    'nginx.upstream.peers.responses.5xx',
    'nginx.upstream.peers.received',
    'nginx.upstream.peers.sent',
    'nginx.server_zone.responses.1xx',
    'nginx.server_zone.responses.2xx',
    'nginx.server_zone.responses.3xx',
    'nginx.server_zone.responses.4xx',
    'nginx.server_zone.responses.5xx',
    'nginx.server_zone.received',
    'nginx.server_zone.sent',
    'nginx.cache.bypass.bytes',
    'nginx.cache.bypass.bytes_written',
    'nginx.cache.bypass.responses',
    'nginx.cache.bypass.responses_written',
    'nginx.cache.expired.bytes',
    'nginx.cache.expired.bytes_written',
    'nginx.cache.expired.responses',
    'nginx.cache.expired.responses_written',
    'nginx.cache.hit.bytes',
    'nginx.cache.hit.responses',
    'nginx.cache.miss.bytes',
    'nginx.cache.miss.bytes_written',
    'nginx.cache.miss.responses',
    'nginx.cache.miss.responses_written',
    'nginx.cache.revalidated.bytes',
    'nginx.cache.revalidated.response',
    'nginx.cache.stale.bytes',
    'nginx.cache.stale.responses',
    'nginx.cache.updating.bytes',
    'nginx.cache.updating.responses',
    'nginx.connections.accepted',
    'nginx.connections.dropped',
    'nginx.generation',
    'nginx.processes.respawned',
    'nginx.requests.total',
    'nginx.server_zone.discarded',
    'nginx.server_zone.requests',
    'nginx.server_zone.responses.total',
    'nginx.slab.slots.fails',
    'nginx.slab.slots.reqs',
    'nginx.ssl.handshakes',
    'nginx.ssl.handshakes_failed',
    'nginx.ssl.session_reuses',
    'nginx.stream.server_zone.connections',
    'nginx.stream.server_zone.discarded',
    'nginx.stream.server_zone.received',
    'nginx.stream.server_zone.sent',
    'nginx.stream.server_zone.sessions.2xx',
    'nginx.stream.server_zone.sessions.4xx',
    'nginx.stream.server_zone.sessions.5xx',
    'nginx.stream.server_zone.sessions.total',
    'nginx.stream.upstream.peers.connections',
    'nginx.stream.upstream.peers.fails',
    'nginx.stream.upstream.peers.health_checks.checks',
    'nginx.stream.upstream.peers.health_checks.fails',
    'nginx.stream.upstream.peers.health_checks.unhealthy',
    'nginx.stream.upstream.peers.received',
    'nginx.stream.upstream.peers.sent',
    'nginx.stream.upstream.peers.unavail',
    'nginx.upstream.peers.fails',
    'nginx.upstream.peers.health_checks.checks',
    'nginx.upstream.peers.health_checks.fails',
    'nginx.upstream.peers.health_checks.unhealthy',
    'nginx.upstream.peers.requests',
    'nginx.upstream.peers.responses.total',
    'nginx.upstream.peers.unavail',
]

if PY3:
    long = int

PLUS_API_ENDPOINTS = {
    "nginx": [],
    "http/requests": ["requests"],
    "http/server_zones": ["server_zones"],
    "http/upstreams": ["upstreams"],
    "http/caches": ["caches"],
    "processes": ["processes"],
    "connections": ["connections"],
    "ssl": ["ssl"],
    "slabs": ["slabs"],
}

PLUS_API_STREAM_ENDPOINTS = {
    "stream/server_zones": ["stream", "server_zones"],
    "stream/upstreams": ["stream", "upstreams"],
}

TAGGED_KEYS = {
    'caches': 'cache',
    'server_zones': 'server_zone',
    'serverZones': 'server_zone',  # VTS
    'upstreams': 'upstream',
    'upstreamZones': 'upstream',  # VTS
    'slabs': 'slab',
    'slots': 'slot'
}


class Nginx(AgentCheck):
    """Tracks basic nginx metrics via the status module
    * number of connections
    * number of requets per second

    Requires nginx to have the status option compiled.
    See http://wiki.nginx.org/HttpStubStatusModule for more details

    $ curl http://localhost:81/nginx_status/
    Active connections: 8
    server accepts handled requests
     1156958 1156958 4491319
    Reading: 0 Writing: 2 Waiting: 6

    """
    def check(self, instance):
        if 'nginx_status_url' not in instance:
            raise ConfigurationError('NginX instance missing "nginx_status_url" value.')

        tags = instance.get('tags', [])

        url, ssl_validation, auth, use_plus_api, plus_api_version = self._get_instance_params(instance)

        if not use_plus_api:
            response, content_type = self._get_data(instance, url, ssl_validation, auth)
            self.log.debug(u"Nginx status `response`: {}".format(response))
            self.log.debug(u"Nginx status `content_type`: {}".format(content_type))

            if content_type.startswith('application/json'):
                metrics = self.parse_json(response, tags)
            else:
                metrics = self.parse_text(response, tags)
        else:
            metrics = []
            self._perform_service_check(instance, '{}/{}'.format(url, plus_api_version), ssl_validation, auth)

            # These are all the endpoints we have to call to get the same data as we did with the old API
            # since we can't get everything in one place anymore.
            for endpoint, nest in chain(iteritems(PLUS_API_ENDPOINTS), iteritems(PLUS_API_STREAM_ENDPOINTS)):
                response = self._get_plus_api_data(instance, url, ssl_validation, auth,
                                                   plus_api_version, endpoint, nest)
                self.log.debug(u"Nginx Plus API version {} `response`: {}".format(plus_api_version, response))
                metrics.extend(self.parse_json(response, tags))

        funcs = {
            'gauge': self.gauge,
            'rate': self.rate,
            'count': self.monotonic_count
        }
        conn = None
        handled = None
        for row in metrics:
            try:
                name, value, tags, metric_type = row

                # Translate metrics received from VTS
                if instance.get('use_vts', False):
                    # Requests per second
                    if name == 'nginx.connections.handled':
                        handled = value
                    if name == 'nginx.connections.accepted':
                        conn = value
                        self.rate('nginx.net.conn_opened_per_s', conn, tags)
                    if handled is not None and conn is not None:
                        self.rate('nginx.net.conn_dropped_per_s', conn - handled, tags)
                        handled = None
                        conn = None
                    if name == 'nginx.connections.requests':
                        self.rate('nginx.net.request_per_s', value, tags)

                    name = VTS_METRIC_MAP.get(name)
                    if name is None:
                        continue

                if name in METRICS_SEND_AS_COUNT:
                    func_count = funcs['count']
                    func_count(name + "_count", value, tags)
                func = funcs[metric_type]
                func(name, value, tags)
            except Exception as e:
                self.log.error(u'Could not submit metric: %s: %s' % (repr(row), str(e)))

    @classmethod
    def _get_instance_params(cls, instance):
        url = instance.get('nginx_status_url')
        ssl_validation = instance.get('ssl_validation', True)

        auth = None
        if 'user' in instance and 'password' in instance:
            auth = (instance['user'], instance['password'])

        use_plus_api = instance.get("use_plus_api", False)
        plus_api_version = str(instance.get("plus_api_version", 2))

        return url, ssl_validation, auth, use_plus_api, plus_api_version

    def _get_data(self, instance, url, ssl_validation, auth):

        r = self._perform_service_check(instance, url, ssl_validation, auth)

        body = r.content
        resp_headers = r.headers
        return body, resp_headers.get('content-type', 'text/plain')

    def _perform_request(self, instance, url, ssl_validation, auth):
        r = requests.get(url, auth=auth, headers=headers(self.agentConfig),
                         verify=ssl_validation, timeout=self.default_integration_http_timeout,
                         proxies=self.get_instance_proxy(instance, url))
        r.raise_for_status()
        return r

    def _perform_service_check(self, instance, url, ssl_validation, auth):
        # Submit a service check for status page availability.
        parsed_url = urlparse(url)
        nginx_host = parsed_url.hostname
        nginx_port = parsed_url.port or 80
        custom_tags = instance.get('tags', [])
        if custom_tags is None:
            custom_tags = []

        service_check_name = 'nginx.can_connect'
        service_check_tags = ['host:%s' % nginx_host, 'port:%s' % nginx_port] + custom_tags
        try:
            self.log.debug(u"Querying URL: {}".format(url))
            r = self._perform_request(instance, url, ssl_validation, auth)
        except Exception:
            self.service_check(service_check_name, AgentCheck.CRITICAL, tags=service_check_tags)
            raise
        else:
            self.service_check(service_check_name, AgentCheck.OK, tags=service_check_tags)
        return r

    def _nest_payload(self, keys, payload):
        """
        Nest a payload in a dict under the keys contained in `keys`
        """
        if len(keys) == 0:
            return payload

        return {
            keys[0]: self._nest_payload(keys[1:], payload)
        }

    def _get_plus_api_data(self, instance, api_url, ssl_validation, auth, plus_api_version, endpoint, nest):
        # Get the data from the Plus API and reconstruct a payload similar to what the old API returned
        # so we can treat it the same way

        url = "/".join([api_url, plus_api_version, endpoint])
        payload = {}
        try:
            self.log.debug(u"Querying URL: {}".format(url))
            r = self._perform_request(instance, url, ssl_validation, auth)
            payload = self._nest_payload(nest, r.json())
        except Exception as e:
            if endpoint in PLUS_API_STREAM_ENDPOINTS:
                self.log.warning("Stream may not be initialized. "
                                 "Error querying {} metrics at {}: {}".format(endpoint, url, e))
            else:
                self.log.exception("Error querying {} metrics at {}: {}".format(endpoint, url, e))

        return payload

    @classmethod
    def parse_text(cls, raw, tags=None):
        # Thanks to http://hostingfu.com/files/nginx/nginxstats.py for this code
        # Connections
        if tags is None:
            tags = []
        output = []
        parsed = re.search(br'Active connections:\s+(\d+)', raw)
        if parsed:
            connections = int(parsed.group(1))
            output.append(('nginx.net.connections', connections, tags, 'gauge'))

        # Requests per second
        parsed = re.search(br'\s*(\d+)\s+(\d+)\s+(\d+)', raw)
        if parsed:
            conn = int(parsed.group(1))
            handled = int(parsed.group(2))
            request = int(parsed.group(3))
            output.extend([('nginx.net.conn_opened_per_s', conn, tags, 'rate'),
                           ('nginx.net.conn_dropped_per_s', conn - handled, tags, 'rate'),
                           ('nginx.net.request_per_s', request, tags, 'rate')])

        # Connection states, reading, writing or waiting for clients
        parsed = re.search(br'Reading: (\d+)\s+Writing: (\d+)\s+Waiting: (\d+)', raw)
        if parsed:
            reading, writing, waiting = parsed.groups()
            output.extend([
                ("nginx.net.reading", int(reading), tags, 'gauge'),
                ("nginx.net.writing", int(writing), tags, 'gauge'),
                ("nginx.net.waiting", int(waiting), tags, 'gauge'),
            ])
        return output

    @classmethod
    def parse_json(cls, raw, tags=None):
        if tags is None:
            tags = []
        if isinstance(raw, dict):
            parsed = raw
        else:
            parsed = json.loads(raw)
        metric_base = 'nginx'

        return cls._flatten_json(metric_base, parsed, tags)

    @classmethod
    def _flatten_json(cls, metric_base, val, tags):
        """
        Recursively flattens the nginx json object. Returns the following: [(metric_name, value, tags)]
        """
        output = []

        if isinstance(val, dict):
            # Pull out the server as a tag instead of trying to read as a metric
            if 'server' in val and val['server']:
                server = 'server:%s' % val.pop('server')
                if tags is None:
                    tags = [server]
                else:
                    tags = tags + [server]
            for key, val2 in iteritems(val):
                if key in TAGGED_KEYS:
                    metric_name = '%s.%s' % (metric_base, TAGGED_KEYS[key])
                    for tag_val, data in iteritems(val2):
                        tag = '%s:%s' % (TAGGED_KEYS[key], tag_val)
                        output.extend(cls._flatten_json(metric_name, data, tags + [tag]))
                else:
                    metric_name = '%s.%s' % (metric_base, key)
                    output.extend(cls._flatten_json(metric_name, val2, tags))

        elif isinstance(val, list):
            for val2 in val:
                output.extend(cls._flatten_json(metric_base, val2, tags))

        elif isinstance(val, bool):
            # Turn bools into 0/1 values
            if val:
                val = 1
            else:
                val = 0
            output.append((metric_base, val, tags, 'gauge'))

        elif isinstance(val, (int, float, long)):
            output.append((metric_base, val, tags, 'gauge'))

        elif isinstance(val, (text_type, str)):
            # In the new Plus API, timestamps are now formatted strings, some include microseconds, some don't...
            try:
                timestamp = time.mktime(datetime.strptime(val, "%Y-%m-%dT%H:%M:%S.%fZ").timetuple())
                output.append((metric_base, timestamp, tags, 'gauge'))
            except ValueError:
                try:
                    timestamp = time.mktime(datetime.strptime(val, "%Y-%m-%dT%H:%M:%SZ").timetuple())
                    output.append((metric_base, timestamp, tags, 'gauge'))
                except ValueError:
                    pass

        return output
