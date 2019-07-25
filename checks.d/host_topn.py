# (C) Cloudwiz, Inc.
# All rights reserved
from __future__ import division

from time import localtime, strftime

try:
    import psutil
    import urlparse
except ImportError:
    psutil = None
import requests

from datadog_checks.base import AgentCheck


class HostTopn(AgentCheck):

    def __init__(self, name, init_config, agentConfig, instances=None):
        AgentCheck.__init__(self, name, init_config, agentConfig, instances)
        self.alertd_url = agentConfig.get('alertd_url', '')
        self.ip = agentConfig.get('ip', '')
        self.hostname = agentConfig.get('hostname', '')
        self.skip_ssl_validation = agentConfig.get('skip_ssl_validation', False)
        self.api_key = agentConfig.get('api_key', '')
        self.preprocess = None
        self.interval = int(agentConfig.get('check_freq'))
        self.cpu_count = psutil.cpu_count()

    def check(self, instance):
        if self.alertd_url and self.api_key:
            start_time = strftime("%Y-%m-%d %H:%M:%S", localtime())
            custom_tags = instance.get('tags', [])
            N = instance.get('N', 10)
            self.gauge("collector.state", 0)
            self.gauge("host.boot.state", 0)
            sorted_procs = self.sort_process()
            cur_processes = self.get_processes(sorted_procs)
            processes = self.cal_disk_io_rate(cur_processes)
            if len(processes) <= 0:
                return
            host_state = self.get_host_state(processes, start_time)
            try:
                self.alertd_post_sender("/host/state", host_state)
                self.gauge("topn.state", 0)
            except Exception as e:
                self.log.error("can't send host state result to alertd %s" % e)
                self.gauge("topn.state", 1)

    def alertd_post_sender(self, url, data, payload={}, token=None):
        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
        metrics_server = self.alertd_url

        cookies = dict(_token=self.api_key)
        # print '%s%s?token=%s' % (metrics_server, url, token)
        if len(payload):
            payload["token"] = self.api_key
        else:
            payload = {'token': self.api_key}
        req = requests.post(urlparse.urljoin(metrics_server, url), params=payload, json=data, headers=headers,
                            cookies=cookies, timeout=20, verify=(not self.skip_ssl_validation))
        return req

    def cal_disk_io_rate(self, cur_processes):
        processes = []
        if self.preprocess is not None:
            for pid in cur_processes:
                try:
                    cur_proc = cur_processes[pid]
                    if pid in self.preprocess:
                        pre_proc = self.preprocess[pid]
                        if pre_proc is not None:
                            try:
                                cur_proc['diskIoRead'] = (cur_proc['disk_io_read'] - pre_proc[
                                    'disk_io_read']) / self.interval
                                cur_proc['diskIoWrite'] = (cur_proc['disk_io_write'] - pre_proc[
                                    'disk_io_write']) / self.interval
                            except:
                                pass
                    else:
                        cur_proc['diskIoRead'] = 0
                        cur_proc['diskIoWrite'] = 0

                    processes.append(cur_proc)
                except:
                    self.log.warn("Can't get correct pid %s", pid)

        self.preprocess = cur_processes
        return processes

    def sort_process(self):
        procs = []
        for p in psutil.process_iter():
            try:
                p.dict = p.as_dict(['pid', 'name', 'username',
                                    'memory_percent', 'cpu_percent',
                                    'create_time'])
                try:
                    p.dict['command'] = (" ".join(p.cmdline())).encode('utf-8').strip()
                except:
                    p.dict['command'] = p.dict['name']

            except psutil.NoSuchProcess:
                pass
            else:
                procs.append(p)

        # return processes sorted by CPU percent usage
        processes = sorted(procs, key=lambda p: p.dict['cpu_percent'],
                           reverse=True)
        return processes

    def get_host_state(self, processes, start_time):
        if len(processes) <= 0:
            return
        host_state = {}
        host_state['ip'] = self.ip
        host_state['version'] = ''
        host_state['commit'] = ''
        host_state['startTime'] = start_time
        host_state['host'] = self.hostname
        host_state['key'] = host_state['ip'] + '_' + host_state['host']
        host_state['processes'] = processes
        host_state['processes_size'] = len(processes)

        return host_state

    def get_processes(self, procs):
        processes = {}
        for i, proc in enumerate(procs):
            try:
                process = {}
                process['pid'] = proc.dict['pid']
                process['name'] = proc.dict['name']
                process['command'] = proc.dict['command']
                process['user'] = proc.dict['username']
                process['memPercent'] = proc.dict['memory_percent']
                try:
                    process['cpuPercent'] = proc.dict['cpu_percent'] / self.cpu_count
                except:
                    process['cpuPercent'] = ""
                    self.log.debug("Can't get cpu usage")
                process['createTime'] = int(round(proc.dict['create_time']))
                try:
                    # in macos it doesn't work. in win and linux is work
                    process['disk_io_read'] = proc.io_counters().read_bytes
                    process['disk_io_write'] = proc.io_counters().write_bytes
                except:
                    pass

            except psutil.AccessDenied:
                continue
            except psutil.NoSuchProcess:
                continue
            processes[process['pid']] = process

        return processes
