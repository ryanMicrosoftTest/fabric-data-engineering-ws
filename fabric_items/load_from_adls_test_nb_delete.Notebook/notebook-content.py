# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.12"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

df = spark.read.json('https://adbpocstorageeus2rh.blob.core.windows.net/landing/ServiceTags_Public_20260427.json')

display(df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

dfs_ep = 'https://adbpocstorageeus2rh.dfs.core.windows.net/landing/ServiceTags_Public_20260427.json'

container_name = 'landing'
storage_account = 'adbpocstorageeus2rh'
path = 'ServiceTags_Public_20260427.json'

abfss_path = f'abfss://{container_name}@{storage_account}.dfs.core.windows.net/{path}'


# df = spark.read.json(abfss_path)
abfss_path

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# MAGIC %%sh
# MAGIC tracert adbpocstorageeus2rh.dfs.core.windows.net

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# MAGIC %%sh
# MAGIC traceroute 8.8.8.8

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

%pip install scapy

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

from scapy.all import traceroute

# Target host
target = "google.com"

# Perform traceroute
res, unans = traceroute(target, maxttl=20, verbose=1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Source - https://stackoverflow.com/a/7018928
# Posted by Andre de Miranda, modified by community. See post 'Timeline' for change history
# Retrieved 2026-05-07, License - CC BY-SA 3.0

from scapy.all import *
target = ["192.168.1.254"]
result, unans = traceroute(target,l4=UDP(sport=RandShort())/DNS(qd=DNSQR(qname="www.google.com")))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

%pip install icmplib

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

from icmplib import traceroute
 
# Perform traceroute
hops = traceroute('google.com')
 
# Print results
for hop in hops:
    print(f"{hop.distance:>2}  {hop.address}  "
          f"{hop.avg_rtt:.2f} ms")
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

%pip install ipaddress

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import ipaddress

help(ipaddress.ip_network)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

res = ipaddress.ip_address('20.60.225.2')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

!ping adbpocstorageeus2rh.dfs.core.windows.net

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
