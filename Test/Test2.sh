#!/bin/sh
h1 python3 DiscoveryAppln.py -P 4 -S 4 -t 5000 > test2_discovery.out 2>&1 &
h2 python3 PublisherAppln.py -T 8 -n pub1 -d "localhost:5000" -p 5004 > test2_pub1.out 2>&1 &
h3 python3 PublisherAppln.py -T 8 -n pub2 -d "localhost:5000" -p 5005 > test2_pub2.out 2>&1 &
h4 python3 PublisherAppln.py -T 8 -n pub3 -d "localhost:5000" -p 5006 > test2_pub3.out 2>&1 &
h5 python3 PublisherAppln.py -T 8 -n pub4 -d "localhost:5000" -p 5007 > test2_pub4.out 2>&1 &
h6 python3 BrokerAppln.py -n broker1 -d "localhost:5000" -p 5001 > test2_broker.out 2>&1 &
h7 python3 SubscriberAppln.py -T 8 -n sub1 -d "localhost:5000" -p 5008 > test2_sub1.out 2>&1 &
h8 python3 SubscriberAppln.py -T 8 -n sub2 -d "localhost:5000" -p 5009 > test2_sub2.out 2>&1 &
h9 python3 SubscriberAppln.py -T 8 -n sub3 -d "localhost:5000" -p 5010 > test2_sub3.out 2>&1 &
h10 python3 SubscriberAppln.py -T 8 -n sub4 -d "localhost:5000" -p 5011 > test2_sub4.out 2>&1 &