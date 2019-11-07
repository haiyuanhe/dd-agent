#!/usr/bin/python
# -*- coding: utf-8 -*-

import time
import atexit
import json
import re
import base64
import uuid
import os

from pyVim import connect
from pyVmomi import vim
from datetime import timedelta
from utils import http
from datadog_checks.base import AgentCheck

POWER_OFF = 1
POWER_ON = 0

CLUSTER_METRIC_BEGIN = 'cluster.'
VH_METRIC_BEGIN = 'vh.'
VM_METRIC_BEGIN = 'vm.'
METRIC_UNDERLINE = '_'


class Vcenter(AgentCheck):

    def __init__(self, name, init_config, agentConfig, instances=None):
        AgentCheck.__init__(self, name, init_config, agentConfig, instances)

    def check(self, instance):
        self.time_file_path = instance.get('time_file_path')
        self.interval = instance.get('interval', 30)
        self.last_data = {}

        self.read_start_time()
        # 取得vcenter列表
        vcs = self.get_vcenter_list()

        for vc in vcs:
            self.log_info("vcenter agent start")
            try:
                # 连接vcenter
                si = connect.SmartConnectNoSSL(host=vc['host'],
                                               user=vc['user'],
                                               pwd=vc['pwd'],
                                               port=int(vc['port']))
                if si:
                    # 注册到atexit，当运行结束后会自动关闭连接等等后续处理
                    atexit.register(connect.Disconnect, si)
                    # 收集vcenter上管理的服务器，包括服务器上的vmware的一些host这种固定信息，并且保存到CMDB
                    self.log.info("start parsing host data ", vc['host'])
                    self.dispose_cluster(si, vc['host'])
                    self.log.info("end parsing host data ", vc['host'])
                    # 收集vcenter上管理的服务器和vmare的指标信息，保存到TSDB
                    self.log.info("start parsing metrics")
                    self.dispose_metrics(si)
                    self.log.info("end parsing metrics")
                    # nput方法就是向tsdb中保存数据
                    tags = ['host:%s' % vc['host']]
                    self.gauge('vcenter.state', 0, tags)
                else:
                    tags = ['host:%s' % vc['host']]
                    self.gauge('vcenter.state', 1, tags)
                    self.log.error('Could not connect to the specified host using ip:%s, user:%s' % (vc['host'], vc['user']))
            except Exception as e:
                # self._readq.nput('vcenter.state %s %s host=%s' % (int(time.time()), 1, vc['host']))
                self.log.error('exception: %s' % e)
                print ('exception: %s' % e)
            self.log.info("vcenter agent end")

    def read_start_time(self):
        if os.path.isfile(self.time_file_path):
            with open(self.time_file_path, 'r') as fp:
                for line in fp:
                    self.last_data = json.loads(line.__str__())
                    return
        else:
            self.update_start_time({})
            return

    def update_start_time(self, new_data):
        with open(self.time_file_path, 'wb') as fp:
            fp.write(json.dumps(new_data))

    def get_vcenter_list(self):
        try:
            uri = '/vmware/vcenter/getvcenter'
            req = http.alertd_post_sender(uri, [])
            req.raise_for_status()
            if req.status_code == 200:
                req_data = req.content
                data = json.loads(req_data)
                return data
            else:
                return []
        except Exception as e:
            self.log.exception(e)
            return []

    def get_views(self, si):
        # 集群列表
        cluster_objs = []
        # 资源池列表
        resource_objs = []
        # 定义物理机列表的数组
        vh_objs = {}
        # 定义vmware虚拟街列表的字典
        vm_objs = {}
        content = si.RetrieveContent()
        container = content.rootFolder
        viewType = [vim.VirtualMachine]
        # 这个参数用来描述是否递归的查找（True是用递归的方式查找）
        recursive = True  # whether we should look into it recursively
        # 获取硬件视图（视图是分层次的，最上层的是硬件服务器，然后下层是vmware）
        containerView = content.viewManager.CreateContainerView(container, viewType, recursive)
        view = containerView.view
        if view and len(view) > 0:
            for v in view:
                if v.resourcePool is not None:
                    cluster_name = v.resourcePool.owner.name
                    cluster_name = self.parse_tag(cluster_name)
                    cluster_uuid = str(uuid.uuid3(uuid.NAMESPACE_DNS, cluster_name))
                    # 获取硬件的hostname，
                    hostname = v.summary.runtime.host.name
                    if cluster_uuid in vh_objs:
                        if hostname in vm_objs[cluster_uuid]:
                            vm_objs[cluster_uuid][hostname].append(v)
                        else:
                            vh_objs[cluster_uuid].append(v.summary.runtime.host)
                            vm_objs[cluster_uuid][hostname] = [v]
                    else:
                        cluster_objs.append(v)

                        vh_objs[cluster_uuid] = [v.summary.runtime.host]
                        vm_objs[cluster_uuid] = {}
                        vm_objs[cluster_uuid][hostname] = [v]

        return cluster_objs, vh_objs, vm_objs

    def dispose_cluster(self, si, vc_host):
        error_code = '1'
        try:
            perf_dict = {}
            content = si.RetrieveContent()
            for counter in content.perfManager.perfCounter:
                perf_dict[counter.key] = counter

            # 获取当前连接的Vcenter上的物理服务器列表和服务器上安装的vmware虚拟机列表
            cluster_objs, vh_objs, vm_objs = self.get_views(si)
            if cluster_objs and len(cluster_objs) > 0:
                # 集群列表
                cluster_list = []
                no_cluster_list = []
                for cluster_obj in cluster_objs:
                    if cluster_obj.resourcePool.owner.name != cluster_obj.summary.runtime.host.name:
                        cluster = HostParser(cluster_obj, vc_host, cluster_obj.resourcePool.owner.name, 0, 0,
                                             "cluster", perf_dict)
                        cluster_dict = cluster.__dict__
                        cluster_list.append(cluster_dict)
                    else:
                        no_cluster_list.append(cluster_obj)

                if any(cluster_list) and len(cluster_list) > 0:
                    cluster_results = http.alertd_post_sender(
                        '/vmware/agent/cluster?vcenterHost=%s&type=%s' % (vc_host, "cluster"), cluster_list)
                    if cluster_results and cluster_results.status_code == 200:
                        cluster_req_data = cluster_results.content
                        cluster_data = json.loads(cluster_req_data)
                        for cluster_result in cluster_data:
                            cluster_id = long(cluster_result['id'])
                            cluster_name = cluster_result['cluster']
                            error_code = self.dispose_host(vc_host, vh_objs, vm_objs, cluster_id, cluster_name,
                                                           perf_dict, error_code)

                if any(no_cluster_list) and len(no_cluster_list) > 0:
                    for cluster_obj in no_cluster_list:
                        cluster_id = 0
                        cluster_name = cluster_obj.resourcePool.owner.name
                        cluster_name = self.parse_tag(cluster_name)
                        error_code = self.dispose_host(vc_host, vh_objs, vm_objs, cluster_id, cluster_name,
                                                       perf_dict,
                                                       error_code)
            else:
                error_code = self.dispose_host(vc_host, vh_objs, vm_objs, 0, '', perf_dict,
                                               error_code)
            self.gauge('vmware.scan.state', error_code)
            '''
            for vh_obj in vh_objs:
                # 处理物理机的信息
                vh = HostParser(vh_obj, vc_host, 0, True, perf_dict)
                vh_dict = vh.__dict__
                # 调用alert-d中的接口，向cmdb中登录或者更新物理机信息
                result = utils.alertd_post_sender('/vmware/agent/vh', vh_dict)
                if result and result.status_code == 200:
                    vh_id = long(result.text)
                    vm_list = []
                    for vm_obj in vm_objs[vh_obj.name]:
                        # 处理物理机上安装的vmware虚拟机的信息
                        vm = HostParser(vm_obj, vc_host, vh_id, False, perf_dict)
                        vm_dict = vm.__dict__
                        vm_list.append(vm_dict)
                    # 调用alert-d中的接口，向cmdb中登录或者更新vmware虚拟机的信息
                    utils.alertd_post_sender('/vmware/agent/vms?parentId=%s' % vh_id, vm_list)
                    error_code = '0'
            self._readq.nput('vmware.scan.state %s %s' % (int(time.time()), error_code))
            '''
        except Exception as e:
            print ('exception: %s' % e)
            self.log.error('cannot send vmware scan result to alertd %s' % e)
            self.gauge('vmware.scan.state', error_code)

    def dispose_host(self, vc_host, vh_objs, vm_objs, cluster_id, cluster_name, perf_dict, error_code):
        # 物理机列表
        vh_list = []
        new_vh_objs = vh_objs
        new_vm_objs = vm_objs
        if cluster_name:
            cluster_uuid = str(uuid.uuid3(uuid.NAMESPACE_DNS, str(cluster_name)))
            new_vh_objs = vh_objs[cluster_uuid]
            new_vm_objs = vm_objs[cluster_uuid]

        for vh_obj in new_vh_objs:
            # 处理物理机的信息
            vh = HostParser(vh_obj, vc_host, cluster_name, cluster_id, cluster_id, "vh", perf_dict)
            vh_dict = vh.__dict__
            vh_list.append(vh_dict)

        # 调用alert-d中的接口，向cmdb中登录或者更新物理机信息
        results = http.alertd_post_sender(
            '/vmware/agent/vh?parentId=%s&vcenterHost=%s&type=%s' % (cluster_id, vc_host, "vh"), vh_list)
        if results and results.status_code == 200:
            req_data = results.content
            data = json.loads(req_data)
            for result in data:
                vh_id = long(result['id'])
                if cluster_id == 0 and (cluster_name == '' or cluster_name == str(result['host'])):
                    cluster_id = vh_id
                # vh_id = result
                vm_list = []
                # for vm_obj in vm_objs[vh_obj.name]:
                for vm_obj in new_vm_objs[result['host']]:
                    try:
                        # 处理物理机上安装的vmware虚拟机的信息
                        vm = HostParser(vm_obj, vc_host, cluster_name, cluster_id, vh_id, "vm", perf_dict)
                        vm_dict = vm.__dict__
                        vm_list.append(vm_dict)
                    except Exception as e:
                        self.log.error("fail to parse vm info %s" % vm_obj.summary.config.name, e)
                # 调用alert-d中的接口，向cmdb中登录或者更新vmware虚拟机的信息
                http.alertd_post_sender(
                    '/vmware/agent/vms?parentId=%s&vcenterHost=%s&type=%s' % (vh_id, vc_host, "vm"), vm_list)
                error_code = '0'
        return error_code

    def dispose_metrics(self, si):
        content = si.RetrieveContent()
        vchtime = si.CurrentTime()
        perf_dict = {}
        perfList = content.perfManager.perfCounter
        for counter in perfList:
            counter_full = "{}.{}.{}".format(counter.groupInfo.key, counter.nameInfo.key, counter.unitInfo.key)
            perf_dict[counter_full] = counter.key

        # 获取当前连接的Vcenter上的物理服务器列表和服务器上安装的vmware虚拟机列表
        cluster_objs, vh_objs, vm_objs = self.get_views(si)
        timestamp = int(time.time())
        self.update_start_time({'timestamp': timestamp})
        if cluster_objs and len(cluster_objs) > 0:
            for cluster_obj in cluster_objs:
                self.is_cluster_metrics_send = False
                cluster_name = cluster_obj.resourcePool.owner.name
                cluster_name = self.parse_tag(cluster_name)
                cluster_uuid = str(uuid.uuid3(uuid.NAMESPACE_DNS, cluster_name))
                if cluster_obj.resourcePool.owner.name != cluster_obj.summary.runtime.host.name:
                    cluster = self.get_cluster(cluster_obj, content, vchtime, perf_dict, interval=20)
                    self.dispose_host_metrics(timestamp, vchtime, perf_dict, content, vh_objs[cluster_uuid],
                                              vm_objs[cluster_uuid], cluster)
                else:
                    self.dispose_host_metrics(timestamp, vchtime, perf_dict, content, vh_objs[cluster_uuid],
                                              vm_objs[cluster_uuid],
                                              None)
        else:
            self.dispose_host_metrics(timestamp, vchtime, perf_dict, content, vh_objs, vm_objs, None)

    def dispose_host_metrics(self, timestamp, vchtime, perf_dict, content, vh_objs, vm_objs, cluster):
        for vh_obj in vh_objs:
            # 获取物理机的具体指标信息
            vh = self.get_vh(vh_obj, content, vchtime, perf_dict, interval=20)
            vms = []
            for vm_obj in vm_objs[vh_obj.name]:
                # 获取vmware虚拟机的具体指标信息
                vm = self.get_vm(vm_obj, content, vchtime, perf_dict, interval=20)
                vms.append(vm)

            self.send_metrics(timestamp, cluster, vh, vms)

    # send vh & vms detail to tsdb
    def send_metrics(self, timestamp, cluster, vh, vms):
        cluster_name = None
        if cluster:
            cluster_name = self.parse_tag(cluster['name'])
            cluster_tag = 'cluster=%s host=%s' % (cluster_name, cluster_name)
            if not self.is_cluster_metrics_send:
                for (key, value) in cluster.items():
                    if key.find(CLUSTER_METRIC_BEGIN, 0) == 0:
                        self.print_metric(key, timestamp, value, cluster_tag)
                for datastore in cluster['datastore']:
                    for (key, value) in datastore.items():
                        if key.find(CLUSTER_METRIC_BEGIN, 0) == 0:
                            self.print_metric(key, timestamp, value, cluster_tag)
                self.is_cluster_metrics_send = True

        vh_name = self.parse_tag(vh['name'])
        host_tag = 'cluster=%s host=%s hostname=%s' % (cluster_name, vh_name, vh_name)
        for (key, value) in vh.items():
            # if key is begin by 'host.' then send it to tsdb
            if key.find(VH_METRIC_BEGIN, 0) == 0:
                self.print_metric(key, timestamp, value, host_tag)
        for vm in vms:
            # vm_tag = 'host=%s' % (vh['name'] + METRIC_UNDERLINE + vm['name'])
            vm_tag = 'cluster=%s host=%s hostname=%s' % (cluster_name, vm['ip'], self.parse_tag(vm['name']))
            for (key, value) in vm.items():
                if key.find(VM_METRIC_BEGIN, 0) == 0:
                    self.print_metric(key, timestamp, value, vm_tag)
        for datastore in vh['datastore']:
            ds_tag = 'cluster=%s host=%s hostname=%s name=%s' % (
            cluster_name, vh_name, vh_name, self.parse_tag(datastore['name']))
            for (key, value) in datastore.items():
                if key.find(VH_METRIC_BEGIN, 0) == 0:
                    self.print_metric(key, timestamp, value, ds_tag)

    def print_metric(self, metric, ts, value, tags=""):
        new_tags = []
        if tags:
            tags = tags.split(" ")
            for tag in tags:
                new_tags = new_tags + [tag.replace('=', ':')]

        try:
            if value is not None:
                if 'timestamp' in self.last_data:
                    timestamp = long(self.last_data['timestamp'])

                    mins = (ts - timestamp) / self.interval
                    if mins > 1:
                        # 这里是为了补全两个时间段之间的指标数据，+6是为了多补几个点，之后会被下一次的指标覆盖
                        for x in range(1, mins + 6, 1):
                            ots = ts + (x * 30)
                            self.gauge(metric, value, new_tags, timestamp=ots)
                self.gauge(metric, value, new_tags, timestamp=ts)

        except Exception as e:
            self.log.error("vmware collector except exception , abort %s" % e)

    def parse_tag(self, tag):
        tag = tag.replace(" ", "_")
        tag = tag.replace(":", "_")
        p = re.compile('([^a-zA-Z0-9/_.-]+)')
        return re.sub(p, "", tag)

    # get cluster detail
    def get_cluster(self, cluster_obj, content, vchtime, perf_dict, interval):
        cluster = {}
        host_datastore_info = []
        owner = cluster_obj.resourcePool.owner
        owner_name = self.parse_tag(owner.name)
        vhost = owner.host
        cpu_used = 0.0
        memory_used = 0.0
        is_power_on = False
        for host in vhost:
            if host.summary.runtime.powerState == 'poweredOn':
                is_power_on = True
                break
        if is_power_on:
            for host in vhost:
                cpu_used += float(host.summary.quickStats.overallCpuUsage)
                memory_used += (float(host.summary.quickStats.overallMemoryUsage) / 1024)

        # 已经使用的磁盘
        storage_used = 0.0
        # 磁盘总容量
        storage_total = 0.0
        datastore_list = self.get_properties(content, [vim.Datastore], None, vim.Datastore)
        for datastore in datastore_list:
            datastore_info = {}
            datastore_summary = datastore['moref'].summary
            dfree = datastore_summary.freeSpace
            dcapacity = datastore_summary.capacity
            dused = 0.0
            dpercent = 0.0
            if int(dcapacity) != 0:
                dused = dcapacity - dfree
                dpercent = float('%.2f' % ((float(dused) / dcapacity) * 100))
            dtotal = float('%.2f' % (float(dcapacity) / 1024 / 1024 / 1024))
            datastore_info['name'] = datastore_summary.name
            datastore_info['cluster.datastore.used'] = float('%.2f' % (float(dused) / 1024 / 1024 / 1024))
            datastore_info['cluster.datastore.usage.percent'] = dpercent
            host_datastore_info.append(datastore_info)
            storage_used += datastore_info['cluster.datastore.used']
            storage_total += dtotal
        cluster['name'] = owner_name
        cluster['ip'] = owner_name
        if is_power_on:
            cluster['cluster.powered'] = POWER_ON
        else:
            cluster['cluster.powered'] = POWER_OFF

        # vh['vh.powered'] = host.runtime.powerState == "poweredOn" and POWER_ON or POWER_OFF
        # 已使用的CPU
        cluster['cluster.cpu.used'] = cpu_used
        # 已使用的内存
        cluster['cluster.mem.used'] = float('%.2f' % memory_used)
        # 已使用的磁盘
        cluster['cluster.storage.used'] = storage_used
        # 磁盘使用率
        cluster['cluster.storage.usage.percent'] = 0.0
        if int(storage_total) != 0:
            cluster['cluster.storage.usage.percent'] = float('%.2f' % ((float(storage_used) / storage_total) * 100))
        # 磁盘信息
        cluster['datastore'] = host_datastore_info

        metrics = {
            # cpu使用率
            'cpu.usage.percent': 100,
            # 内存使用率
            'mem.usage.percent': 100,
        }
        self.append_metrics(cluster, CLUSTER_METRIC_BEGIN, metrics, vhost, content, vchtime, perf_dict, interval)

        return cluster

    # get host detail
    def get_vh(self, host, content, vchtime, perf_dict, interval):
        vh = {}
        host_datastore_info = []
        summary = host.summary

        if host.runtime.powerState == "poweredOn":
            vh['vh.powered'] = POWER_ON
        else:
            vh['vh.powered'] = POWER_OFF

        cpu_used = 0
        mem_used = 0.0
        if vh['vh.powered'] == POWER_ON:
            # [Host] CPU total used (MHz) & used percent
            # cpu_total = summary.hardware.cpuMhz * summary.hardware.numCpuCores
            cpu_used = summary.quickStats.overallCpuUsage
            # cpu_percent = float('%.2f' % (float(cpu_used)/cpu_total * 100))
            # [Host] Mem used (GB) & used percent
            mem_used = float(summary.quickStats.overallMemoryUsage) / 1024

        # mem_total = float(summary.hardware.memorySize) / 1024 / 1024 / 1024
        # mem_percent = float('%.2f' % (float(mem_used/mem_total) * 100))
        # [Host] datastore info (GB)

        # 已经使用的磁盘
        storage_used = 0.0
        # 磁盘总容量
        storage_total = 0.0
        datastore_list = host.datastore
        for datastore in datastore_list:
            datastore_info = {}
            datastore_summary = datastore.summary
            dfree = datastore_summary.freeSpace
            dcapacity = datastore_summary.capacity
            dused = 0.0
            dpercent = 0.0
            if int(dcapacity) != 0:
                dused = dcapacity - dfree
                dpercent = float('%.2f' % ((float(dused) / dcapacity) * 100))
            dtotal = float('%.2f' % (float(dcapacity) / 1024 / 1024 / 1024))
            datastore_info['name'] = datastore_summary.name
            datastore_info['vh.datastore.used'] = float('%.2f' % (float(dused) / 1024 / 1024 / 1024))
            datastore_info['vh.datastore.usage.percent'] = dpercent
            host_datastore_info.append(datastore_info)
            storage_used += datastore_info['vh.datastore.used']
            storage_total += dtotal

        vh['name'] = host.name
        vh['ip'] = host.name

        # vh['vh.powered'] = host.runtime.powerState == "poweredOn" and POWER_ON or POWER_OFF
        # 已使用的CPU
        vh['vh.cpu.used'] = cpu_used
        # 已使用的内存
        vh['vh.mem.used'] = float('%.2f' % mem_used)
        # 已使用的磁盘
        vh['vh.storage.used'] = storage_used
        # 磁盘使用率
        vh['vh.storage.usage.percent'] = 0.0
        if int(storage_total) != 0:
            vh['vh.storage.usage.percent'] = float('%.2f' % ((float(storage_used) / storage_total) * 100))
        # 磁盘信息
        vh['datastore'] = host_datastore_info

        metrics = {
            # cpu使用率
            'cpu.usage.percent': 100,
            # 内存使用率
            'mem.usage.percent': 100,
            # 网络传输数据 Kbps
            'net.transmitted.kiloBytesPerSecond': 1,  # [VM]Network (Tx/Rx) Kbps
            # 网络接收数据 Kbps
            'net.received.kiloBytesPerSecond': 1,
            # 硬盘读 Kbps
            'disk.read.kiloBytesPerSecond': 1,
            # 硬盘写 Kbps
            'disk.write.kiloBytesPerSecond': 1
        }
        self.append_metrics(vh, VH_METRIC_BEGIN, metrics, host, content, vchtime, perf_dict, interval)

        return vh

    # get vm detail
    def get_vm(self, vmware, content, vchtime, perf_dict, interval):
        vm = {}

        if vmware.runtime.powerState == "poweredOn":
            vm['vm.powered'] = POWER_ON
        else:
            vm['vm.powered'] = POWER_OFF

        cpu_used = 0
        mem_used = 0.0
        if vm['vm.powered'] == POWER_ON:
            cpu_used = vmware.summary.quickStats.overallCpuUsage
            # cpu_total = vmware.config.cpuAllocation.shares.shares
            # cpu_percent = float('%.2f' % (float(cpu_used / cpu_total) * 100))
            mem_used = float(vmware.summary.quickStats.guestMemoryUsage) / 1024
            # mem_total = float(vmware.config.hardware.memoryMB) / 1024
            # mem_percent = float('%.2f' % (float(mem_used / mem_total) * 100))

        store_used = float('%.2f' % (float(vmware.summary.storage.committed) / 1024 / 1024 / 1024))
        store_total = float('%.2f' % (float(
            vmware.summary.storage.committed + vmware.summary.storage.uncommitted + vmware.summary.storage.unshared) / 1024 / 1024 / 1024))
        store_percent = 0.0
        if int(store_total) != 0:
            store_percent = float('%.2f' % (float(store_used / store_total) * 100))

        vm['name'] = vmware.name
        ipv4 = vmware.summary.guest.ipAddress
        hostname = vmware.summary.config.name
        if ipv4 is not None and hostname is not None:
            vm['ip'] = self.parse_tag(ipv4)
        elif vmware.name is not None:
            vm['ip'] = self.parse_tag(hostname)
        else:
            return

        # vm['vm.powered'] = vmware.runtime.powerState == "poweredOn" and POWER_ON or POWER_OFF
        # 使用的cpu
        vm['vm.cpu.used'] = cpu_used
        # 使用的内存
        vm['vm.mem.used'] = float('%.2f' % mem_used)
        # 使用的磁盘
        vm['vm.storage.used'] = store_used
        # 磁盘使用率
        vm['vm.storage.usage.percent'] = store_percent

        if vm['vm.powered'] == POWER_ON:
            metrics = {
                # cpu使用率
                'cpu.usage.percent': 100,
                # 内存使用率
                'mem.usage.percent': 100,
                # 使用的硬盘 kb
                'disk.used.kiloBytes': 1,
                # 网络传输数据 Kbps
                'net.transmitted.kiloBytesPerSecond': 1,  # [VM]Network (Tx/Rx) Kbps
                # 网络接收数据 Kbps
                'net.received.kiloBytesPerSecond': 1,
                # 硬盘读 Kbps
                'disk.read.kiloBytesPerSecond': 1,
                # 硬盘写 Kbps
                'disk.write.kiloBytesPerSecond': 1
            }
            self.append_metrics(vm, VM_METRIC_BEGIN, metrics, vmware, content, vchtime, perf_dict, interval)

        return vm

    def append_metrics(self, vdict, vstart, metrics, vmware, content, vchtime, perf_dict, interval):
        stat_int = interval * 3
        for metric, convert in metrics.items():
            # 循环取得每一个指标的值（其中metric就是指标名，[cpu.usage.percent]......）
            result = self.build_query(content, vchtime, perf_dict, metric, "", vmware, interval)
            if result:
                value = (float(sum(result[0].value[0].value)) / stat_int) / convert
                # 类似vdict[vh.cpu.usage.percent]或者vdict[vm.cpu.usage.percent] = .......
                vdict[vstart + metric] = float('%.2f' % value) if value >= 0 else 0.0

    # query metric value in content
    def build_query(self, content, vchtime, dict, name, instance, vm, interval):
        perfManager = content.perfManager
        metricId = vim.PerformanceManager.MetricId(counterId=dict[name], instance=instance)
        startTime = vchtime - timedelta(minutes=(interval + 1))
        endTime = vchtime - timedelta(minutes=1)
        query = vim.PerformanceManager.QuerySpec(intervalId=20, entity=vm, metricId=[metricId], startTime=startTime,
                                                 endTime=endTime)
        perfResults = perfManager.QueryPerf(querySpec=[query])
        if perfResults:
            return perfResults
        else:
            # self.log_error('[%s] build query fail, %s[%s] results is empty.\n' % (format(vchtime), vm.name, name))
            return None

    # get vmware info by viewtype
    def get_properties(self, content, viewType, props, specType):
        # Build a view and get basic properties for all Virtual Machines
        objView = content.viewManager.CreateContainerView(content.rootFolder, viewType, True)
        tSpec = vim.PropertyCollector.TraversalSpec(name='tSpecName', path='view', skip=False,
                                                    type=vim.view.ContainerView)
        pSpec = vim.PropertyCollector.PropertySpec(all=False, pathSet=props, type=specType)
        oSpec = vim.PropertyCollector.ObjectSpec(obj=objView, selectSet=[tSpec], skip=False)
        pfSpec = vim.PropertyCollector.FilterSpec(objectSet=[oSpec], propSet=[pSpec],
                                                  reportMissingObjectsInResults=False)
        retOptions = vim.PropertyCollector.RetrieveOptions()
        totalProps = []
        retProps = content.propertyCollector.RetrievePropertiesEx(specSet=[pfSpec], options=retOptions)
        totalProps += retProps.objects
        while retProps.token:
            retProps = content.propertyCollector.ContinueRetrievePropertiesEx(token=retProps.token)
            totalProps += retProps.objects
        objView.Destroy()
        # Turn the output in retProps into a usable dictionary of values
        gpOutput = []
        for eachProp in totalProps:
            propDic = {}
            for prop in eachProp.propSet:
                propDic[prop.name] = prop.val
            propDic['moref'] = eachProp.obj
            gpOutput.append(propDic)
        return gpOutput


class HostParser:
    def __init__(self, obj, vc_host, cluster_name, root_id, parent_id, is_host, perf_dict):
        self.key = None
        self.vcenterHost = vc_host
        self.parentId = parent_id
        self.rootId = root_id
        self.resourcePool = None
        self.clusterName = self.parse_tag(cluster_name)
        self.type = is_host
        self.server = None
        self.startTime = None
        self.architecture = None
        self.biosDate = None
        self.biosVersion = None
        self.cpuMhz = None
        self.cpuNum = None
        self.totalCPU = None
        self.totalMemory = None
        self.totalStorage = None
        self.hostname = None
        self.domain = None
        self.fqdn = None
        self.defaultIp = None
        self.osFamily = None
        self.osName = None
        self.osVersion = None
        self.systemVendor = None
        self.productSerial = None
        self.productVersion = None
        self.productUuid = None
        self.productName = None
        self.cpu = []
        self.status = None
        self.alarms = None
        if is_host == "vh":
            self.parse_vh(obj)
            self.alarms = json.dumps(self.get_alarms(obj, perf_dict))
        elif is_host == "vm":
            self.parse_vm(obj)
            self.alarms = json.dumps(self.get_alarms(obj, perf_dict))
        elif is_host == "cluster":
            self.parse_cluster(obj)
            self.alarms = json.dumps(self.get_alarms(obj.resourcePool.owner, perf_dict))

    def get_osname(self, gFullName):
        if gFullName:
            index = gFullName.find('(')
            if index != -1:
                return gFullName[0:index - 1]
        return None

    def get_alarms(self, obj, perf_dict):
        alarms = []
        for state in obj.triggeredAlarmState:
            time = state.time
            if time:
                time = state.time.strftime("%Y-%m-%d %H:%M:%S")
            alarm = {
                "status": str(state.overallStatus).upper(),
                "time": time,
                "name": state.alarm.info.name.encode("utf-8"),
                "description": state.alarm.info.description.encode("utf-8")
            }
            expression = []
            for exp in state.alarm.info.expression.expression:
                redInterval = 0
                try:
                    redInterval = exp.redInterval
                except Exception as e:
                    pass
                yellowInterval = 0
                try:
                    yellowInterval = exp.yellowInterval
                except Exception as e:
                    pass
                operator = ""
                try:
                    operator = str(exp.operator)
                except Exception as e:
                    pass

                try:
                    expression.append({
                        "operator": operator,
                        "red": exp.red,
                        "redInterval": redInterval,
                        "yellow": exp.yellow,
                        "yellowInterval": yellowInterval,
                        "metric": {
                            "key": exp.metric.counterId,
                            "level": perf_dict[exp.metric.counterId].level,
                            "groupInfo": {
                                "key": perf_dict[exp.metric.counterId].groupInfo.key,
                                "label": perf_dict[exp.metric.counterId].groupInfo.label,
                                "summary": perf_dict[exp.metric.counterId].groupInfo.summary.encode("utf-8")
                            },
                            "nameInfo": {
                                "key": perf_dict[exp.metric.counterId].nameInfo.key,
                                "label": perf_dict[exp.metric.counterId].nameInfo.label,
                                "summary": perf_dict[exp.metric.counterId].nameInfo.summary.encode("utf-8")
                            },
                            "unitInfo": {
                                "key": perf_dict[exp.metric.counterId].unitInfo.key,
                                "label": perf_dict[exp.metric.counterId].unitInfo.label,
                                "summary": perf_dict[exp.metric.counterId].unitInfo.summary.encode("utf-8")
                            }
                        }
                    })
                except Exception as e:
                    pass
            alarm["expression"] = expression
            alarms.append(alarm)
        return alarms

    def parse_cluster(self, host):
        owner = host.resourcePool.owner
        owner_name = self.parse_tag(owner.name)
        ip = owner_name
        self.server = host.runtime.host.summary.managementServerIp
        self.productUuid = str(uuid.uuid3(uuid.NAMESPACE_DNS, ip))
        self.key = '_'.join([self.server, self.productUuid])
        self.hostname = owner_name
        self.cpuNum = owner.summary.numCpuCores
        self.totalCPU = owner.summary.totalCpu
        self.totalMemory = ('%.2f' % (float(owner.summary.totalMemory) / 1024 / 1024 / 1024))
        self.totalStorage = None
        self.fqdn = owner_name
        self.defaultIp = ip
        self.osName = owner_name
        self.osVersion = None
        self.productName = owner_name
        self.cpu = []
        self.status = str(owner.overallStatus).upper()

    def parse_tag(self, tag):
        tag = tag.replace(" ", "_")
        tag = tag.replace(":", "_")
        p = re.compile('([^a-zA-Z0-9/_.-]+)')
        return re.sub(p, "", tag)

    def parse_vh(self, host):
        ip = host.name
        self.server = host.summary.managementServerIp
        self.productUuid = host.summary.hardware.uuid if host.summary.hardware.uuid is not None else ''
        if ip is not None:
            ip = self.parse_tag(ip)
        if host.name is not None:
            self.key = '_'.join([self.vcenterHost, self.server, str(ip), str(host.name), self.productUuid])
        else:
            return
        total_storage = 0.0
        for ds in host.datastore:
            total_storage += float(ds.summary.capacity) / 1024 / 1024 / 1024
        self.status = str(host.overallStatus).upper()
        self.defaultIp = ip
        self.architecture = None
        self.biosDate = None
        self.biosVersion = None
        self.hostname = host.summary.config.product.name
        self.cpuMhz = host.summary.hardware.cpuMhz
        self.cpuNum = host.summary.hardware.numCpuCores
        self.totalCPU = self.cpuMhz * self.cpuNum
        self.totalMemory = float('%.2f' % (float(host.summary.hardware.memorySize) / 1024 / 1024 / 1024))
        self.totalStorage = float('%.2f' % total_storage)
        self.fqdn = host.name
        self.osFamily = None
        self.osName = host.summary.config.product.name
        self.osVersion = host.summary.config.product.version
        self.systemVendor = None
        self.productSerial = None
        self.productVersion = None
        self.cpu.append(host.summary.hardware.cpuModel)
        if host.runtime.powerState == "poweredOn":
            vhtime = host.runtime.bootTime
            if vhtime:
                vhtime = host.runtime.bootTime.strftime("%Y-%m-%d %H:%M:%S")
            self.startTime = vhtime
        else:
            self.status = "GREY"

    def parse_resource_pool(self, resourcePool, vmware):
        if resourcePool:
            resourcePool = vmware.name + "_" + resourcePool
            if vmware.parent.name != 'Resources':
                self.parse_resource_pool(resourcePool, vmware.parent)
        else:
            if vmware.resourcePool.name != 'Resources':
                resourcePool = vmware.resourcePool.name
                if vmware.resourcePool.parent.name != 'Resources':
                    self.parse_resource_pool(resourcePool, vmware.resourcePool.parent)

    def parse_vm(self, vmware):
        # self.defaultIp = summary.guest.ipAddress
        ipv4 = vmware.summary.guest.ipAddress
        # 因为【summary.config.name】是由用户配置的，所以有可能是中文
        hostname = vmware.summary.config.name
        self.productUuid = vmware.summary.config.uuid if vmware.summary.config.uuid is not None else ''
        if hostname is not None:
            zhmodel = re.compile(u'[\u4e00-\u9fa5]')
            match = zhmodel.search(hostname)
            if match:
                # 如果是中文需要base64转一下
                hostname = base64.b64encode(hostname.encode('utf-8'))
        self.server = vmware.summary.runtime.host.name
        if ipv4 is not None and hostname is not None:
            ipv4 = self.parse_tag(ipv4)
            self.key = '_'.join([self.vcenterHost, self.server, str(ipv4), str(hostname), self.productUuid])
            self.defaultIp = ipv4
        elif hostname is not None:
            self.key = '_'.join(
                [self.vcenterHost, self.server, self.parse_tag(hostname), str(hostname), self.productUuid])
            self.defaultIp = self.parse_tag(hostname)
        else:
            return
        total_storage = vmware.summary.storage.uncommitted + vmware.summary.storage.unshared
        self.status = str(vmware.overallStatus).upper()
        self.architecture = None
        self.biosDate = None
        self.biosVersion = None
        self.hostname = vmware.summary.config.name
        self.cpuMhz = None
        self.cpuNum = vmware.config.hardware.numCPU
        self.totalCPU = vmware.config.cpuAllocation.shares.shares
        self.totalMemory = float('%.2f' % (float(vmware.config.hardware.memoryMB) / 1024))
        self.totalStorage = float('%.2f' % (float(total_storage) / 1024 / 1024 / 1024))
        # self.fqdn = vmware.summary.config.name
        self.fqdn = vmware.summary.config.name
        self.osFamily = None
        self.osName = self.get_osname(vmware.summary.config.guestFullName)
        self.osVersion = None
        self.systemVendor = None
        self.productSerial = None
        self.productVersion = None
        self.cpu = []
        self.parse_resource_pool(self.resourcePool, vmware)
        if vmware.runtime.powerState == "poweredOn":
            vmtime = vmware.runtime.bootTime
            if vmtime:
                vmtime = vmware.runtime.bootTime.strftime("%Y-%m-%d %H:%M:%S")
            self.startTime = vmtime
        else:
            self.status = "GREY"