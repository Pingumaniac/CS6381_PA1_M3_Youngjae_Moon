#!/bin/sh
h1 python3 DiscoveryAppln.py -r "10.0.0.1" -P 4 -S 4 &
h2 python3 PublisherAppln.py -d "10.0.0.1:5555" -a "10.0.0.3" -T 7 -n pub1 &
h3 python3 PublisherAppln.py -d "10.0.0.1:5555" -a "10.0.0.4" -T 7 -n pub2 &
h4 python3 PublisherAppln.py -d "10.0.0.1:5555" -a "10.0.0.5" -T 7 -n pub3 &
h5 python3 PublisherAppln.py -d "10.0.0.1:5555" -a "10.0.0.6" -T 7 -n pub4 &
h6 python3 BrokerAppln.py -d "10.0.0.1:5555" -a "10.0.0.2" -n broker -p 5578 &
h7 python3 SubscriberAppln.py -d "10.0.0.1:5555" -a "10.0.0.7" -T 7 -n sub1 &
h8 python3 SubscriberAppln.py -d "10.0.0.1:5555" -a "10.0.0.8" -T 7 -n sub2 &
h9 python3 SubscriberAppln.py -d "10.0.0.1:5555" -a "10.0.0.9" -T 7 -n sub3 &
h10 python3 SubscriberAppln.py -d "10.0.0.1:5555"-a "10.0.0.10" -T 7 -n sub4 &