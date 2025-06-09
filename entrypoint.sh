#!/bin/bash
nohup pip3 install -r sp-enrichment/requirements.txt &
nohup python3 sp-enrichment/subscriber.py &
nohup python3 sp-enrichment/ipv4_publisher.py &
nohup python3 sp-enrichment/ipv6_publisher.py &
nohup python3 sp-enrichment/domain_publisher.py &
nohup python3 sp-enrichment/unknown_type_publisher.py &
tail -f publisher.log