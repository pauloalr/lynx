import time


FLOW_IN = 1
FLOW_OUT = 2
FLOW_SAME = 3

class NetFlow():
    def __init__(self, guest_corr):
        self._guests = guest_corr
        self._flows = self._initialize_flows()
        self._flow_stats = dict()
        self._flow_ids = dict()
        self._flow_type = dict()
        self._ts = dict()

    def _initialize_flows(self):
        flows = {FLOW_IN: {},
                 FLOW_OUT: {},
                 FLOW_SAME: {}}
        return flows
    
    def _convert_ts(self, cpu_id, tsc):
        if cpu_id not in self._ts:
            start_ts = long((time.time()-86400) * 1000000000)
            self._ts[cpu_id] = {}
            self._ts[cpu_id][tsc] = start_ts
        start_tsc = self._ts[cpu_id].keys()[0]
        delta = tsc - start_tsc
        return long((delta + self._ts[cpu_id][start_tsc]) / 1000000000)
        
    def _is_xmit(self, event):
        return event == "net_dev_xmit"
    
    def _is_rx(self, event):
        return event == "netif_rx"
    
    def _is_phy_br(self, dev):
        return "phy-br" in dev
    
    def _is_int_br(self, dev):
        return "int-br" in dev
    
    def _is_tap(self, dev):
        return "tap" in dev
        
    def _get_flow_name(self, src_mac, dest_mac):
        sender = self._guests[src_mac]
        recver = self._guests[dest_mac]
        return sender + "_" + recver
    
    def _create_flow_id(self, flow_name, net_id):
        if flow_name not in self._flow_ids:
            self._flow_ids[flow_name] = {}
        if net_id not in self._flow_ids[flow_name]:
            self._flow_ids[flow_name][net_id] = 0
        else:
            self._flow_ids[flow_name][net_id] += 1
        return self._get_flow_id(flow_name, net_id)
    
    def _get_flow_id(self, flow_name, net_id):
        return net_id + "-" + str(self._flow_ids[flow_name][net_id])
        
    def _flow_entry(self, event, cpu_id, ts, net_id, dev, flow_name):
        if self._is_xmit(event) and self._is_phy_br(dev):
            flow_id = self._create_flow_id(flow_name, net_id)
            self._flows[FLOW_IN][(flow_name, flow_id)] = [(cpu_id, ts)]
            self._flow_type[flow_name] = FLOW_IN
            return True
        elif self._is_rx(event) and self._is_tap(dev):
            flow_id = self._create_flow_id(flow_name, net_id)
            if flow_name in self._flow_type:
                flow_type = self._flow_type[flow_name]
                self._flows[flow_type][(flow_name, flow_id)] = [(cpu_id, ts)]
            else:
                self._flows[FLOW_OUT][(flow_name, flow_id)] = [(cpu_id, ts)]
                self._flows[FLOW_SAME][(flow_name, flow_id)] = [(cpu_id, ts)]
            return True
        else:
            return False
        
    def _flow_exit(self, event, cpu_id, ts, net_id, dev, flow_name):
        try:
            flow_id = self._get_flow_id(flow_name, net_id)
        except:
            return
        if self._is_rx(event) and self._is_phy_br(dev):
            self._flows[FLOW_OUT][(flow_name, flow_id)].append((cpu_id, ts))
            self._flow_type[flow_name] = FLOW_OUT
            netflow = self._flows[FLOW_OUT][(flow_name, flow_id)]
            self._flow_stats.setdefault(flow_name, []).append(netflow)
            del self._flows[FLOW_OUT][(flow_name, flow_id)] 
            if (flow_name, flow_id) in self._flows[FLOW_SAME]:
                del self._flows[FLOW_SAME][(flow_name, flow_id)]
            return True
        elif self._is_xmit(event) and self._is_tap(dev):
            if (flow_name, flow_id) in self._flows[FLOW_IN]:
                self._flows[FLOW_IN][(flow_name, flow_id)].\
                                                append((cpu_id, ts))
                self._flow_type[flow_name] = FLOW_IN
                netflow = self._flows[FLOW_IN][(flow_name, flow_id)]
                self._flow_stats.setdefault(flow_name, []).append(netflow)
                del self._flows[FLOW_IN][(flow_name, flow_id)]
                return True
            elif (flow_name, flow_id) in self._flows[FLOW_SAME]:
                self._flows[FLOW_SAME][(flow_name, flow_id)].\
                                                append((cpu_id, ts))
                self._flow_type[flow_name] = FLOW_SAME
                netflow = self._flows[FLOW_SAME][(flow_name, flow_id)]
                self._flow_stats.setdefault(flow_name, []).append(netflow)
                del self._flows[FLOW_SAME][(flow_name, flow_id)]
                if (flow_name, flow_id) in self._flows[FLOW_OUT]:
                    del self._flows[FLOW_OUT][(flow_name, flow_id)] 
                return True
            else:
                return False
            
    def _flow_mid(self, event, cpu_id, ts, net_id, dev, flow_name):
        try:
            flow_id = self._get_flow_id(flow_name, net_id)
        except:
            return
        data = (cpu_id, ts)
        if self._is_rx(event) and self._is_int_br(dev):
            self._flows[FLOW_IN][(flow_name, flow_id)].append(data)
        elif self._is_xmit(event) and self._is_int_br(dev):
            self._flows[FLOW_OUT][(flow_name, flow_id)].append(data)
        else:
            if (flow_name, flow_id) in self._flows[FLOW_OUT]:
                self._flows[FLOW_OUT][(flow_name, flow_id)].append(data)
            elif (flow_name, flow_id) in self._flows[FLOW_IN]:
                self._flows[FLOW_IN][(flow_name, flow_id)].append(data)
            elif (flow_name, flow_id) in self._flows[FLOW_SAME]:
                self._flows[FLOW_SAME][(flow_name, flow_id)].append(data)
                
    def parse_flow(self, event, cpu_id, ts, net_id, dev, src_mac, dest_mac):
        try:
            flow_name = self._get_flow_name(src_mac, dest_mac)
        except:
            return
        ts = self._convert_ts(cpu_id, ts)
        if not self._flow_entry(event, cpu_id, ts, net_id, dev, flow_name):
            if not self._flow_exit(event, cpu_id, ts, net_id, dev, flow_name):
                self._flow_mid(event, cpu_id, ts, net_id, dev, flow_name)
                
    def get_flow(self):
        return self._flow_stats

        