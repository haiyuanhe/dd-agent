# -*- coding: utf-8 -*-
import subprocess
import json
import psutil
import re
from utils import http
from datadog_checks.base import AgentCheck

SYSTEM_OS = 'WINDOWS'
ALERTD_APIS = dict(
    ps='/svc_topo/proc',
    netstat='/svc_topo/socket'
)


class Win32SvctopoBase(AgentCheck):
    """
    Win32SvctopoBase: Get the service topo source data info(ps and netstat) on Windows paltorm.
    this source data will send to alertd.
    """
    def __init__(self, name, init_config, agentConfig, instances=None):
        AgentCheck.__init__(self, name, init_config, agentConfig, instances)
        # 获取当前主机 IP 地址, 用于标识这些数据来源于那台主机
        self.host_ip = agentConfig.get('ip', '')
        self.alertd_url = agentConfig.get('alertd_url', '')
        self.skip_ssl_validation = agentConfig.get('skip_ssl_validation', False)
        self.api_key = agentConfig.get('api_key', '')

    def check(self, instance):
        self.alertd_ps_sender()
        self.alertd_netstat_sender()

    def alertd_ps_sender(self):
        try:
            out = self.get_process_info()
            if out and len(out) > 0:
                lines = json.dumps(out, ensure_ascii=False)
                http.alertd_post_sender("%s%s" % (self.alertd_url, ALERTD_APIS['ps']), {"os": SYSTEM_OS, "host": self.host_ip, "data": lines}, token=self.api_key, skip_ssl_validation=self.skip_ssl_validation)
            else:
                self.log.warning('no validate data get for [ps]')
        except Exception as e:
            self.log.error("caught exception on collect topo info of [ps]: {}", e)

    def alertd_netstat_sender(self):
        try:
            lines = self.get_netstat_info()
            if lines:
                http.alertd_post_sender("%s%s" % (self.alertd_url, ALERTD_APIS['netstat']), {"os": SYSTEM_OS, "host": self.host_ip, "data": lines}, token=self.api_key, skip_ssl_validation=self.skip_ssl_validation)
            else:
                self.log.warning('some wrong when get the data for [netstat]')
        except Exception as e:
            self.log.error("caught exception on collect topo info of [netstat]: {}", e)

    def get_netstat_info(self):
        """
        执行终端指令netstat, 获取终端返回结果
        无法拿到: Send-Q Recv-Q 等类Linux上数据
        :return:
        """
        lines = list()
        try:
            output = subprocess.check_output('netstat -ano', shell=True)
            for ln in output.split('\r\n'):
                if 'TCP' in ln or 'UDP' in ln:
                    lines.append(ln)
            return '\r\n'.join(lines)
        except Exception as e:
            self.log.error('caught exception on running cmmand[netstat]: {}'.format(e))
        return None

    def get_process_info(self):
        """
        使用psutil获取进程信息,需要执行此脚本用户权限比较高,以便拿到进程cmdline信息
        :return:
        """
        pattern = re.compile(r'[^\u4e00-\u9fa5]')
        processes = []
        for proc in psutil.process_iter():
            try:
                cmd = psutil.Process(proc.pid).cmdline()
                for ind in range(0, len(cmd)):
                    cmd[ind] = re.sub(pattern, '', cmd[ind])

                process = dict(
                    uid=proc.username(),
                    pid=proc.pid,
                    ppid=proc.ppid(),
                    name=proc.name(),
                    cmd=cmd
                )
                processes.append(process)
            except Exception as e:
                self.log.error("{}".format(e))
        return processes

