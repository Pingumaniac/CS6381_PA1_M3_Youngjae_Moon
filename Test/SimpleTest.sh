#!/bin/sh
h1 python3 DiscoveryAppln.py -P 2 -S 2 -t 5100 > simple_discovery.out 2>&1 & 
h3 python3 PublisherAppln.py -T 5 -n pub1 -p 5102 -d "localhost:5100" > simple_pub1.out 2>&1 &
h4 python3 PublisherAppln.py -T 5 -n pub2 -p 5103 -d "localhost:5100" > simple_pub2.out 2>&1 &
h2 python3 BrokerAppln.py -n broker1 -p 5101 -d "localhost:5100" > simple_broker.out 2>&1 &
h7 python3 SubscriberAppln.py -T 5 -n sub1 -p 5104 -d "localhost:5100" > simple_sub1.out 2>&1 &
h8 python3 SubscriberAppln.py -T 5 -n sub2 -p 5105 -d "localhost:5100" > simple_sub2.out 2>&1 &