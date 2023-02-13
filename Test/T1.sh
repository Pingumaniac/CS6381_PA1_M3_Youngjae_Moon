#!/bin/sh
h1 python3 DiscoveryAppln.py -P 4 -S 4 -t 5555 & 
h2 python3 PublisherAppln.py -T 5 -n pub1 -p 5570 &
h3 python3 PublisherAppln.py -T 5 -n pub2 -p 5571 &
h4 python3 PublisherAppln.py -T 5 -n pub3 -p 5572 &
h5 python3 PublisherAppln.py -T 5 -n pub4 -p 5573 &
h6 python3 BrokerAppln.py -n broker1 -p 5578 &
h7 python3 SubscriberAppln.py -T 5 -n sub1 -p 5574 &
h8 python3 SubscriberAppln.py -T 5 -n sub2 -p 5576 &
h9 python3 SubscriberAppln.py -T 5 -n sub3 -p 5577 &
h10 python3 SubscriberAppln.py -T 5 -n sub4 -p 5581 &