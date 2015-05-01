import json


def load_guest_info():
    with open("/Users/yuxihu/Downloads/discovery.txt", "r") as f:
        info = f.readlines()
        vm_info = json.loads(info[0])
    f.close()
    
    guest_corr = {}
    for vm in vm_info:
        guest = str(vm["hostname"])
        for i in range(len(vm["if_info"])):
            netif = vm["if_info"][i]
            key = guest + "-" + str(i)
            guest_corr[str(netif["tap"])] = key
            guest_corr[str(netif["qvb"])] = key
            guest_corr[str(netif["qvo"])] = key
            guest_corr[str(netif["mac_addr"])] = key
        
    return guest_corr
