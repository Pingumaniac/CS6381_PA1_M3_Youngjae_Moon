#!/bin/sh
h1 python3 DiscoveryAppln.py -P 4 -S 4 -t 5000 &
h2 python3 PublisherAppln.py -T 8 -n pub1 -d "localhost:5000" -p 5004 &
h3 python3 PublisherAppln.py -T 8 -n pub2 -d "localhost:5000" -p 5005 &
h4 python3 PublisherAppln.py -T 8 -n pub3 -d "localhost:5000" -p 5006 &
h5 python3 PublisherAppln.py -T 8 -n pub4 -d "localhost:5000" -p 5007 &
h6 python3 BrokerAppln.py -n broker1 -d "localhost:5000" -p 5001 &
h7 python3 SubscriberAppln.py -T 8 -n sub1 -d "localhost:5000" -p 5008 &
h8 python3 SubscriberAppln.py -T 8 -n sub2 -d "localhost:5000" -p 5009 &
h9 python3 SubscriberAppln.py -T 8 -n sub3 -d "localhost:5000" -p 5010 &
h10 python3 SubscriberAppln.py -T 8 -n sub4 -d "localhost:5000" -p 5011 &