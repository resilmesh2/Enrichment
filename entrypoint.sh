#!/bin/bash
nohup python3 /home/app/subscriber.py &
nohup python3 /home/app/ipv4_publisher.py &
nohup python3 /home/app/ipv6_publisher.py &
nohup python3 /home/app/domain_publisher.py &
nohup python3 /home/app/unknown_type_publisher.py &
tail -f /home/app/publisher.log