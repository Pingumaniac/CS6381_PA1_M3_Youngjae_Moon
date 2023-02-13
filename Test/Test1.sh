#!/bin/sh
h1 python3 DiscoveryAppln.py -P 4 -S 4 -t 5555 > test1_discovery.out 2>&1 & 
h2 python3 PublisherAppln.py -T 5 -n pub1 -p 5570 > test1_pub1.out 2>&1 &
h3 python3 PublisherAppln.py -T 5 -n pub2 -p 5571 > test1_pub2.out 2>&1 &
h4 python3 PublisherAppln.py -T 5 -n pub3 -p 5572 > test1_pub3.out 2>&1 &
h5 python3 PublisherAppln.py -T 5 -n pub4 -p 5573 > test1_pub4.out 2>&1 &
h6 python3 BrokerAppln.py -n broker1 -p 5578 > test1_broker.out 2>&1 &
h7 python3 SubscriberAppln.py -T 5 -n sub1 -p 5574 > test1_sub1.out 2>&1 &
h8 python3 SubscriberAppln.py -T 5 -n sub2 -p 5576 > test1_sub2.out 2>&1 &
h9 python3 SubscriberAppln.py -T 5 -n sub3 -p 5577 > test1_sub3.out 2>&1 &
h10 python3 SubscriberAppln.py -T 5 -n sub4 -p 5581 > test1_sub4.out 2>&1 &