# -*- coding: utf-8 -*-
import subprocess

from utils import http
from datadog_checks.base import AgentCheck

SYSTEM_OS = 'LINUX'
COMMANDS = dict(
    ps=['ps', '-ef'],
    netstat=['netstat', '-antp']
)
ALERTD_APIS = dict(
    ps='/svc_topo/proc',
    netstat='/svc_topo/socket'
)
LINE_SPLIT_CHAR = '\n'


class LinuxSvctopoBase(AgentCheck):
    """
    LinuxSvctopoBase: Get the service topo base data(ps and netstat) info.
    this source data will send to alertd.
    `ps -ef` data format:
    UID        PID  PPID  C STIME TTY          TIME CMD
    root         1     0  0  2018 ?        00:23:20 /lib/systemd/systemd --system --deserialize 23
    root         2     0  0  2018 ?        00:00:02 [kthreadd]
    root         3     2  0  2018 ?        00:09:36 [ksoftirqd/0]
    `netstat -antp` data format:
    Active Internet connections (servers and established)
    Proto Recv-Q Send-Q Local Address           Foreign Address         State       PID/Program name
    tcp        0      0 0.0.0.0:10050           0.0.0.0:*               LISTEN      20664/zabbix_agentd
    tcp        0      0 0.0.0.0:10051           0.0.0.0:*               LISTEN      9832/zabbix_server
    """
    def __init__(self, name, init_config, agentConfig, instances=None):
        AgentCheck.__init__(self, name, init_config, agentConfig, instances)
        # 获取当前主机 IP 地址, 用于标识这些数据来源于那台主机
        self.host_ip = agentConfig.get('ip', '')
        self.alertd_url = agentConfig.get('alertd_url', '')
        self.skip_ssl_validation = agentConfig.get('skip_ssl_validation', False)
        self.api_key = agentConfig.get('api_key', '')

    def check(self, instance):
        for cmd in COMMANDS.keys():
            self.sender_src_data(cmd)

    def sender_src_data(self, cmd):
        try:
            lines = self.get_process_info(cmd)
            if self.check_data(lines):
                http.alertd_post_sender("%s%s" % (self.alertd_url, ALERTD_APIS[cmd]), {"os": SYSTEM_OS, "host": self.host_ip, "data": lines}, token=self.api_key, skip_ssl_validation=self.skip_ssl_validation)
            else:
                self.log.warning('no validate data get for [{}].'.format(cmd))
        except Exception as e:
            self.log.error('caught exception on collect topo info of [{}]: {}'.format(cmd, e))

    def get_process_info(self, v=None):
        """
        执行终端指令, 获取终端返回结果
        :param v: 指令所属名称(ps netstat)
        :return:
        """
        output = None
        try:
            output = subprocess.check_output(' '.join(COMMANDS[v]), shell=True)
        except Exception as e:
            self.log.error('caught exception on running cmmand[{}]: {}'.format(v, e))
        return output

    @staticmethod
    def check_data(data):
        """
        output data simple check.
        :return:
        """
        if not data:
            return False
        return len(data.split(LINE_SPLIT_CHAR)) > 1

