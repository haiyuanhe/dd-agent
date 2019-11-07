#!/user/bin/python

import re
import json
import time
import yaml
from utils import http
from datadog_checks.base import AgentCheck
try:
    import pymysql
except ImportError:
    pymysql = None  # This is handled gracefully in main()

try:
    import cx_Oracle
    import os
    os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
except ImportError:
    cx_Oracle = None  # This is handled gracefully in main()

CONNECT_TIMEOUT = 2


class Cloudmon(AgentCheck):

    def __init__(self, name, init_config, agentConfig, instances=None):
        AgentCheck.__init__(self, name, init_config, agentConfig, instances)

    def check(self, instance):
        self.conn_info = json.loads(instance.get('info', ""))
        self.database = instance.get("database", "mysql")
        # 10080000 7day
        self.offset = int(instance.get("offset", 0))
        self.yaml_config = instance.get("config_path")
        self.last_time = int(time.time() - self.offset)
        self.interval = 100
        self.disallow = re.compile('[^a-zA-Z0-9\-_\.]')
        self.bucket = {}
        self.yaml = yaml.safe_load(open(self.yaml_config))
        self.key_patterns = {prepare_regex(metric['key']): metric for metric in self.yaml.get('metrics', [])}
        self.uppercase = instance.get("uppercase", "False")
        
        if self.database == "oracle":
            self.hostmap = 'SELECT i."itemid", i."key_", h."host", h2."host" AS "proxy" FROM "items" i JOIN "hosts" h ON i."hostid"=h."hostid" LEFT JOIN "hosts" h2 ON h2."hostid"=h."proxy_hostid"'
            self.history = 'select "itemid", "clock", "value" from "history" where "history"."clock" > %d UNION ALL select "itemid", "clock", "value" from "history_uint" where "history_uint"."clock" > %d'
            self.scan = 'select h."host" as "hostname", f."ip", f."type" as "interface_type" , i.*  from "host_inventory" i JOIN "hosts" h ON i."hostid" = h."hostid" LEFT JOIN "interface" f ON f."hostid" = i."hostid" where f."useip" = 1'
            self.group = 'select "groups"."name" from "hosts_groups" left join "groups" on "hosts_groups"."groupid" = "groups"."groupid" where "hostid" = %s'
            self.alert = 'select DISTINCT h."host" AS "monitoredEntity", t."description", f."triggerid" AS "id", i."name", e."clock" AS "creationTime", t."priority", e."value" AS "triggeredValue", i."itemid",t."lastchange" FROM "triggers" t INNER JOIN "functions" f ON f."triggerid" = t."triggerid" INNER JOIN "items" i ON i."itemid" = f."itemid" INNER JOIN "hosts" h ON i."hostid" = h."hostid" INNER JOIN "events" e ON e."objectid" = t."triggerid" WHERE h."status" =0 AND i."status" =0 AND t."status" =0  '
        elif self.database == "mysql":
            self.hostmap = 'SELECT i."itemid", i."key_", h."host", h2."host" AS "proxy" FROM "items" i JOIN "hosts" h ON i."hostid"=h."hostid" LEFT JOIN "hosts" h2 ON h2."hostid"=h."proxy_hostid"'
            self.history = 'select "itemid", "clock", "value" from "history" where "history"."clock" > %d UNION ALL select "itemid", "clock", "value" from "history_uint" where "history_uint"."clock" > %d'
            self.scan = 'select h."host" as "hostname", f."ip", f."type" as "interface_type" , i.*  from "host_inventory" i JOIN "hosts" h ON i."hostid" = h."hostid" LEFT JOIN "interface" f ON f."hostid" = i."hostid" where f."useip" = 1'
            self.group = 'select "groups"."name" from "hosts_groups" left join "groups" on "hosts_groups"."groupid" = "groups"."groupid" where "hostid" = %s'
            self.alert = 'select DISTINCT h."host" AS "monitoredEntity", t."description", f."triggerid" AS "id", i."name", e."clock" AS "creationTime", t."priority", e."value" AS "triggeredValue", i."itemid" FROM "triggers" t INNER JOIN "functions" f ON f."triggerid" = t."triggerid" INNER JOIN "items" i ON i."itemid" = f."itemid" INNER JOIN "hosts" h ON i."hostid" = h."hostid" INNER JOIN "events" e ON e."objectid" = t."triggerid" WHERE h."status" =0 AND i."status" =0 AND t."status" =0 GROUP BY f."triggerid" ORDER BY t."lastchange" DESC'

        else:
            print 'database type error!'

        if self.uppercase == "True":
            self.hostmap = self.hostmap.upper()
            self.history = self.history.upper().replace("%D","%d")
            self.scan = self.scan.upper().replace("%S","%s")
            self.alert = self.alert.upper()

        conn = self.get_conn()
        try:
            hostmap = self.getHostmapWithConn(conn)
            self.sendMetric(conn, hostmap)
            self.sendAlert(conn, hostmap)
        finally:
            conn.close()
        self.interval = self.interval + 1
        if self.interval > 100:
            self.hostScanMysql()
    
    def get_conn(self):
        self.log.info("use %s database", self.database)
        if self.database == "mysql":
            self.hostmap = self.hostmap.replace('"', "")
            self.history = self.history.replace('"', "")
            self.scan = self.scan.replace('"', "")
            self.group = self.group.replace('"', "")
            self.alert = self.alert.replace('"', "")

            return pymysql.connect(
                host=self.conn_info["host"],
                port=self.conn_info["port"],
                user=self.conn_info["user"],
                passwd=self.conn_info["passwd"],
                ssl=None,
                connect_timeout=CONNECT_TIMEOUT,
                db=self.conn_info["db"],
                charset='utf8',
            )
        elif self.database == "oracle":
            return cx_Oracle.connect(self.conn_info["user"], self.conn_info["passwd"], '%s:%s/%s' % (
            self.conn_info["host"], self.conn_info["port"], self.conn_info["db"]))
        else:
            raise ValueError("Can't parse database")

    def hostScanMysql(self):
        self.log.info("scan from zabbix")
        self.interval = 1
        try:
            conn = self.get_conn()
            cur = conn.cursor()
            hostmap = {}
            try:
                cur.execute(self.scan)
                for row in cur:
                    hostname = re.sub(self.disallow, '_', row[0])
                    ip = row[1]
                    interface_type = row[2]
                    hostid = row[3]
                    try:
                        hostmap[hostid]["interfaces"].append({
                            "ip": ip,
                            "type": "Interface",
                            "name": self.getInterfacesType(interface_type),
                            "key": "%s_%s_interface_%s" % (ip, hostname, self.getInterfacesType(interface_type))
                        })
                        if interface_type == 1:
                            hostmap[hostid]["defaultIp"] = ip
                            hostmap[hostid]["key"] = "%s_%s" % (ip, hostname)
                    except KeyError:
                        hostmap[hostid] = {
                            "defaultIp": ip,
                            "hostname": hostname,
                            "key": "%s_%s" % (ip, hostname),
                            "type": "Host",
                            "interfaces": [
                                {
                                    "ip": ip,
                                    "type": "Interface",
                                    "name": self.getInterfacesType(interface_type),
                                    "key": "%s_%s_interface_%s" % (ip, hostname, self.getInterfacesType(interface_type))
                                }
                            ]
                        }
                for hostid, host_dict in hostmap.iteritems():
                    self.log.info(host_dict)
                    data = http.alertd_post_sender('/cmdb/agent/host/scan', host_dict)
                    res = json.loads(data.content)
                    self.addTag(conn, hostid, res["hostId"])
                return hostmap
            finally:
                cur.close()
        finally:
            conn.close()

    def sendMetric(self, conn, hostmap):
        cur = conn.cursor()
        try:
            cur.execute(self.history % (self.last_time, self.last_time))
            for row in cur:
                itemid = row[0]
                ts = row[1]
                value = row[2]
                if ts > self.last_time:
                    self.last_time = ts
                try:
                    metric = self.process_metric(itemid, hostmap)
                    if metric is None:
                        continue
                    tags = []
                    if metric["labels_mapping"]:
                        for name, value in metric["labels_mapping"].iteritems():
                            tags = tags + ['%s:%s' % (name, value)]

                    self.gauge(metric['name'], value, tags, int(ts))
                except KeyError:
                    # TODO: Consider https://wiki.python.org/moin/PythonDecoratorLibrary#Retry
                    self.log.warn("error: Key lookup miss for %s" % itemid)
                except Exception as e:
                    self.log.error("error: Key lookup miss for %s", e)

        finally:
            self.log.info("timestamp have change is %s" % self.last_time)
            cur.close()

    def sendAlert(self, conn, hostmap):
        cur = conn.cursor()
        try:
            cur.execute(self.alert)
            alerts = []
            for row in cur:
                monitoredEntity = row[0]
                description = row[1]
                id = row[2]
                name = row[3]
                creationTime = row[4]*1000
                priority = row[5]
                triggeredValue = row[6]
                itemid = row[7]
                metric = self.process_metric(itemid, hostmap)
                if metric is not None:
                    definition = {}
                    definition['id'] = id
                    definition['name'] = name
                    definition['description'] = description
                    definition['alertDetails'] = {
                        "alertType": "ZABBIX_ALERT"
                    }
                    definition['creationTime'] = int(time.time())
                    definition['modificationTime'] = definition['creationTime']
                    status = {}
                    status['alertId'] = id
                    status['monitoredEntity'] = monitoredEntity
                    status['level'] = self.get_level(priority)
                    status['creationTime'] = creationTime
                    status['levelChangedTime'] = creationTime
                    status['triggeredValue'] = triggeredValue
                    tags = {"host": monitoredEntity}
                    for name, value in metric["labels_mapping"].iteritems():
                        tags[name] = value
                    status["alertEntity"] = {
                        "tags": tags
                    }
                    alert = {
                        "definition": definition,
                        "status": status,
                        "metric": metric["name"]
                    }
                    alerts.append(alert)
            http.alertd_post_sender("/alert/import", alerts)
        finally:
            cur.close()

    def process_metric(self, itemid, hostmap):
        host = hostmap[itemid]['host']
        _key = hostmap[itemid]['key']
        metric = hostmap[itemid]['key']

        labels_mapping = SortedDict()
        for pattern, attrs in self.key_patterns.items():
            match = re.match(pattern, _key)
            if match:
                # process metric name
                metric = attrs.get('name', metric)

                def repl(m):
                    asterisk_index = int(m.group(1))
                    return match.group(asterisk_index)

                metric = re.sub('\$(\d+)', repl, metric)

                # ignore metrics with rejected placeholders
                rejected_matches = [r for r in attrs.get('reject', []) if re.search(r, _key)]
                if rejected_matches:
                    self.log.info('Rejecting metric %s (matched %s)', rejected_matches[0], metric)
                    continue  # allow to process metric by another rule

                # create labels
                for label_name, match_group in attrs.get('labels', {}).items():
                    if match_group[0] == '$':
                        label_value = match.group(int(match_group[1]))
                    else:
                        label_value = match_group
                    labels_mapping[label_name] = label_value
                break
        else:
            if self.yaml.get('explicit_metrics', False):
                self.log.warn('Dropping implicit metric name %s', _key)
                return None

        labels_mapping['host'] = host
        return {
            'name': sanitize_key(metric),
            'labels_mapping': labels_mapping,
        }

    def getHostmapWithConn(self, conn):
        cur = conn.cursor()
        try:
            cur.execute(self.hostmap)
            # Translation of item key_
            # Note: http://opentsdb.net/docs/build/html/user_guide/writing.html#metrics-and-tags

            hostmap = {}
            for row in cur:
                hostmap[row[0]] = {
                    'key': row[1],
                    'host': row[2]
                }
            return hostmap
        finally:
            cur.close()

    def addTag(self, conn, hostId, ciId):
        # {key: "groups", value: "1"}
        cur = conn.cursor()
        try:
            cur.execute(self.group % hostId)
            for row in cur:
                group = row[0]
                self.log.info(group)
                http.alertd_post_sender('/host/tag', {"key": "groups", "value": group}, {"hostId": ciId})
        finally:
            cur.close()

    def getInterfacesType(self, interface_type):
        if interface_type == 1:
            return "Host"
        elif interface_type == 2:
            return "SNMP"
        elif interface_type == 3:
            return "IPMI"
        elif interface_type == 4:
            return "JMX"

    def get_level(self, priority):
        # Not classified
        # Information
        #
        # Warning
        # Average
        #
        # High
        # Disaster
        if priority > 1:
            return "WARNING"
        elif priority >3:
            return "CRITICAL"
        else:
            return "NORMAL"
             
        
def sanitize_key(string):
    return re.sub('[^a-zA-Z0-9\-_\.]+', '_', string)


def prepare_regex(key_pattern):
    return re.escape(key_pattern).replace('\*', '([^,]*?)')


class SortedDict(dict):
    """Hackish container to guarantee consistent label sequence for prometheus"""

    def keys(self):
        return sorted(super(SortedDict, self).keys())

    def values(self):
        return [self[key] for key in self.keys()]
