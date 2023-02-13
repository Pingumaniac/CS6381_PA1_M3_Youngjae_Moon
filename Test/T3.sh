#!/bin/sh
h1 python3 DiscoveryAppln.py -P 8 -S 8 -t 5012 &
h2 python3 PublisherAppln.py -T 4 -n pub1 -d "localhost:5012" -p 5016 &
h3 python3 PublisherAppln.py -T 4 -n pub2 -d "localhost:5012" -p 5017 &
h4 python3 PublisherAppln.py -T 4 -n pub3 -d "localhost:5012" -p 5018 &
h5 python3 PublisherAppln.py -T 4 -n pub4 -d "localhost:5012" -p 5019 &
h6 python3 PublisherAppln.py -T 4 -n pub5 -d "localhost:5012" -p 5020 &
h7 python3 PublisherAppln.py -T 4 -n pub6 -d "localhost:5012" -p 5021 &
h8 python3 PublisherAppln.py -T 4 -n pub7 -d "localhost:5012" -p 5022 &
h9 python3 PublisherAppln.py -T 4 -n pub8 -d "localhost:5012" -p 5023 &
h10 python3 BrokerAppln.py -n broker1 -d "localhost:5012" -p 5013 &
h11 python3 SubscriberAppln.py -T 4 -n sub1 -d "localhost:5012" -p 5024 &
h12 python3 SubscriberAppln.py -T 4 -n sub2 -d "localhost:5012" -p 5025 &
h13 python3 SubscriberAppln.py -T 4 -n sub3 -d "localhost:5012" -p 5026 &
h14 python3 SubscriberAppln.py -T 4 -n sub4 -d "localhost:5012" -p 5027 &
h15 python3 SubscriberAppln.py -T 4 -n sub5 -d "localhost:5012" -p 5028 &
h16 python3 SubscriberAppln.py -T 4 -n sub6 -d "localhost:5012" -p 5029 &
h17 python3 SubscriberAppln.py -T 4 -n sub7 -d "localhost:5012" -p 5030 &
h18 python3 SubscriberAppln.py -T 4 -n sub8 -d "localhost:5012" -p 5031 &