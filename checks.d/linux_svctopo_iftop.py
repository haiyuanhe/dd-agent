# -*- coding: utf-8 -*-
import subprocess

from utils import http
from datadog_checks.base import AgentCheck

SYSTEM_OS = 'LINUX'
COMMAND = ['iftop', '-b', '-t', '-s', '10', '-nPN']
ALERTD_API = '/svc_topo/nw'


class LinuxSvctopoIftop(AgentCheck):
    """
    LinuxSvctopoIftop: Get the service topo network data(iftop) info.
    this source data will send to alertd.
    `iftop -b -t -s 10 -nPN` data format:
    interface: eth0
    IP address is: 172.19.103.233
    MAC address is: 00:16:3e:02:95:3c
    Listening on eth0
       # Host name (port/service if enabled)            last 2s   last 10s   last 40s cumulative
    --------------------------------------------------------------------------------------------
       1 172.19.103.233:38828                     =>     14.8Kb     2.96Kb     2.96Kb     3.70KB
         115.239.210.27:443                       <=      658Kb      132Kb      132Kb      164KB
       2 172.19.103.233:37732                     =>         0b       624b       624b       780B
         100.100.30.25:80                         <=         0b       104b       104b       130B
       3 172.19.103.233:45764                     =>         0b       281b       281b       351B
         129.204.81.138:10050                     <=         0b       190b       190b       238B
       4 172.19.103.233:45748                     =>         0b       279b       279b       349B
         129.204.81.138:10050                     <=         0b       190b       190b       238B
       5 172.19.103.233:45766                     =>         0b       270b       270b       337B
         129.204.81.138:10050                     <=         0b       184b       184b       230B
       6 172.19.103.233:45800                     =>     1.31Kb       268b       268b       335B
         129.204.81.138:10050                     <=       920b       184b       184b       230B
       7 172.19.103.233:45778                     =>         0b       258b       258b       323B
         129.204.81.138:10050                     <=         0b       190b       190b       238B
       8 172.19.103.233:45742                     =>         0b       258b       258b       322B
         129.204.81.138:10050                     <=         0b       184b       184b       230B
       9 172.19.103.233:45768                     =>         0b       252b       252b       315B
         129.204.81.138:10050                     <=         0b       184b       184b       230B
      10 172.19.103.233:45760                     =>         0b       251b       251b       314B
         129.204.81.138:10050                     <=         0b       184b       184b       230B
    --------------------------------------------------------------------------------------------
    Total send rate:                                     21.4Kb     9.15Kb     9.15Kb
    Total receive rate:                                   663Kb      136Kb      136Kb
    Total send and receive rate:                          684Kb      145Kb      145Kb
    --------------------------------------------------------------------------------------------
    Peak rate (sent/received/total):                     21.4Kb      663Kb      684Kb
    Cumulative (sent/received/total):                    11.4KB      170KB      181KB
    ============================================================================================
    """
    def __init__(self, name, init_config, agentConfig, instances=None):
        AgentCheck.__init__(self, name, init_config, agentConfig, instances)
        # 获取当前主机 IP 地址, 用于标识这些数据来源于那台主机
        self.host_ip = agentConfig.get('ip', '')
        self.alertd_url = agentConfig.get('alertd_url', '')
        self.skip_ssl_validation = agentConfig.get('skip_ssl_validation', False)
        self.api_key = agentConfig.get('api_key', '')

    def check(self, instance):
        try:
            lines = self.get_process_info()
            if self.check_data(lines):
                http.alertd_post_sender("%s%s" % (self.alertd_url, ALERTD_API), {"os": SYSTEM_OS, "host": self.host_ip, "data": lines}, token=self.api_key, skip_ssl_validation=self.skip_ssl_validation)
            else:
                self.log.warning('no validate data get for [iftop].')
        except Exception as e:
            self.log.error('caught exception on collect topo info of [iftop]: {}'.format(e))

    def get_process_info(self):
        """
        执行终端指令iftop, 获取终端返回结果
        :return:
        """
        output = None
        try:
            output = subprocess.check_output(' '.join(COMMAND), shell=True)
        except Exception as e:
            self.log.error('caught exception on running cmmand[iftop]: {}'.format(e))
        return output

    @staticmethod
    def check_data(data):
        """
        output data simple check.
        :return:
        """
        if not data:
            return False
        return "=>" in data

