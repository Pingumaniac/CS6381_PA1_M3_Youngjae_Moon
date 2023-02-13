#!/bin/sh
h1 python3 DiscoveryAppln.py -r "10.0.0.1" -P 4 -S 4 > test4_discovery.out 2>&1 &
h2 python3 PublisherAppln.py -d "10.0.0.1:5555" -a "10.0.0.3" -T 7 -n pub1 > test4_pub1.out 2>&1 &
h3 python3 PublisherAppln.py -d "10.0.0.1:5555" -a "10.0.0.4" -T 7 -n pub2 > test4_pub2.out 2>&1 &
h4 python3 PublisherAppln.py -d "10.0.0.1:5555" -a "10.0.0.5" -T 7 -n pub3 > test4_pub3.out 2>&1 &
h5 python3 PublisherAppln.py -d "10.0.0.1:5555" -a "10.0.0.6" -T 7 -n pub4 > test4_pub4.out 2>&1 &
h6 python3 BrokerAppln.py -d "10.0.0.1:5555" -a "10.0.0.2" -n broker -p 5578 > test4_broker.out 2>&1 &
h7 python3 SubscriberAppln.py -d "10.0.0.1:5555" -a "10.0.0.7" -T 7 -n sub1 > test4_sub1.out 2>&1 &
h8 python3 SubscriberAppln.py -d "10.0.0.1:5555" -a "10.0.0.8" -T 7 -n sub2 > test4_sub2.out 2>&1 &
h9 python3 SubscriberAppln.py -d "10.0.0.1:5555" -a "10.0.0.9" -T 7 -n sub3 > test4_sub3.out 2>&1 &
h10 python3 SubscriberAppln.py -d "10.0.0.1:5555"-a "10.0.0.10" -T 7 -n sub4 > test4_sub4.out 2>&1 &