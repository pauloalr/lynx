import sys

import flow
import utils
from zeus import ZeusClient

ZEUS_API = "http://api.ciscozeus.io"
TOKEN = "fd46bb82"
zeus = ZeusClient(TOKEN, ZEUS_API)


class Interpreter():
    def __init__(self, guest_corr):
        self._flow_parser = flow.NetFlow(guest_corr)
            
    def parse_systapline(self, line):
        strings = line.strip().split(" ")
        event_name = strings[0]
        cpu_id = int(strings[4])
        ts = long(strings[5])
        if event_name == "netif_receive_skb":
            dev_name = strings[7]
            net_id = strings[10]
            (src_mac, dest_mac) = strings[15:17]
            self._flow_parser.parse_flow(event_name, cpu_id, ts, net_id, 
                                         dev_name, src_mac, dest_mac)
        elif event_name == "netif_rx":
            dev_name = strings[7]
            net_id = strings[10]
            (src_mac, dest_mac) = strings[15:17]
            self._flow_parser.parse_flow(event_name, cpu_id, ts, net_id, 
                                         dev_name, src_mac, dest_mac)
        elif event_name == "net_dev_xmit":
            dev_name = strings[7]
            net_id = strings[10]
            (src_mac, dest_mac) = strings[15:17]
            self._flow_parser.parse_flow(event_name, cpu_id, ts, net_id, 
                                         dev_name, src_mac, dest_mac)
        else:
            pass
        
def prepare_zeus(flows):
    cpu_flows = {}
    for flow in flows:
        cpu_flows[flow] = dict((i, {}) for i in range(12))
        packets = flows[flow]
        for packet in packets:
            temp = []
            for item in packet:
                if item not in temp:
                    temp.append(item)
            for (cpu_id, ts) in temp:
                if ts not in cpu_flows[flow][cpu_id]:
                    cpu_flows[flow][cpu_id][ts] = 1
                else:
                    cpu_flows[flow][cpu_id][ts] += 1
    return cpu_flows

def insert_zeus(cpu_flows):
    for flow in cpu_flows:
        for cpu_id in cpu_flows[flow]:
            metric_name = str(3)+ "_" + flow + "_" + str(cpu_id)
            metrics = []
            if cpu_flows[flow][cpu_id]:
                for ts in cpu_flows[flow][cpu_id]:
                    metric = {"timestamp": int(ts), 
                              "value": cpu_flows[flow][cpu_id][ts]}
                    metrics.append(metric)
            print zeus.sendMetric(metric_name, metrics)     
        
def main():
    guest_corr = utils.load_guest_info()
    parser = Interpreter(guest_corr)
    infile = sys.argv[1]
    with open(infile, "r") as fin:
        for line in fin:
            try:
                parser.parse_systapline(line)
            except Exception,e:
                print e
        fin.close()
    flows = parser._flow_parser.get_flow()
    cpu_flows = prepare_zeus(flows)
    insert_zeus(cpu_flows)
    
if __name__ == "__main__":
    main()
