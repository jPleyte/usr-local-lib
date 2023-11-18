'''
Created on Nov 17, 2023
Scans the network for a mac addresse and prints the associated ip address. 

Usage: sudo python3 get_ip_by_mac.py --mac ab:cd:ef:12:34:56 --subnet 192.168.0.0/24

Dependencies
- sudo - This script must be run using sudo because `arp-scan` requires privileged access
- arp-scan - The command line ARP scanner 

@author: jpleyte
'''
import subprocess
import re
import argparse

#ARP_SCAN = '/usr/sbin/arp-scan'
ARP_SCAN = 'arp-scan'
class ArpScanCmd:
    def scan(self, ip_net: str, mac: str):
        results = subprocess.run([ARP_SCAN, ip_net], capture_output=True, text=True).stdout.splitlines()

        # Regex for ip, mac, and ssid eg 192.168.1.1\t00:11:22:33:44:55\tMySSID        
        pattern = re.compile("^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+\t[0-9a-fA-F:]+\t.*$")
        
        for line in results:
            if pattern.match(line):
                ip, line_mac, ssid = line.split("\t")
                if line_mac.upper() == mac.upper():                    
                    return ip

        return None
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--subnet", help = "Subnet (e.g. 192.168.0.0/24)", required=True)
    parser.add_argument("-m", "--mac", help = "Mac address(e.g. 11:22:33:aa:BB:Cc)", required=True)
    args = parser.parse_args()
    
    subnet = args.subnet
    mac = args.mac
    
    arp = ArpScanCmd()
    ip = arp.scan(subnet, mac)

    if ip:
        print(ip)
    else:
        print(f"Did not find ip for {mac}")
