#!/bin/sh
h1 python3 DiscoveryAppln.py -P 8 -S 8 -t 5012 > test3_discovery.out 2>&1 &
h2 python3 PublisherAppln.py -T 4 -n pub1 -d "localhost:5012" -p 5016 > test3_pub1.out 2>&1 &
h3 python3 PublisherAppln.py -T 4 -n pub2 -d "localhost:5012" -p 5017 > test3_pub2.out 2>&1 &
h4 python3 PublisherAppln.py -T 4 -n pub3 -d "localhost:5012" -p 5018 > test3_pub3.out 2>&1 &
h5 python3 PublisherAppln.py -T 4 -n pub4 -d "localhost:5012" -p 5019 > test3_pub4.out 2>&1 &
h6 python3 PublisherAppln.py -T 4 -n pub5 -d "localhost:5012" -p 5020 > test3_pub5.out 2>&1 &
h7 python3 PublisherAppln.py -T 4 -n pub6 -d "localhost:5012" -p 5021 > test3_pub6.out 2>&1 &
h8 python3 PublisherAppln.py -T 4 -n pub7 -d "localhost:5012" -p 5022 > test3_pub7.out 2>&1 &
h9 python3 PublisherAppln.py -T 4 -n pub8 -d "localhost:5012" -p 5023 > test3_pub8.out 2>&1 &
h2 python3 BrokerAppln.py -n broker1 -d "localhost:5012" -p 5013 > test3_broker.out 2>&1 &
h11 python3 SubscriberAppln.py -T 4 -n sub1 -d "localhost:5012" -p 5024 > test3_sub1.out 2>&1 &
h12 python3 SubscriberAppln.py -T 4 -n sub2 -d "localhost:5012" -p 5025 > test3_sub2.out 2>&1 &
h13 python3 SubscriberAppln.py -T 4 -n sub3 -d "localhost:5012" -p 5026 > test3_sub3.out 2>&1 &
h14 python3 SubscriberAppln.py -T 4 -n sub4 -d "localhost:5012" -p 5027 > test3_sub4.out 2>&1 &
h15 python3 SubscriberAppln.py -T 4 -n sub5 -d "localhost:5012" -p 5028 > test3_sub5.out 2>&1 &
h16 python3 SubscriberAppln.py -T 4 -n sub6 -d "localhost:5012" -p 5029 > test3_sub6.out 2>&1 &
h17 python3 SubscriberAppln.py -T 4 -n sub7 -d "localhost:5012" -p 5030 > test3_sub7.out 2>&1 &
h18 python3 SubscriberAppln.py -T 4 -n sub8 -d "localhost:5012" -p 5031 > test3_sub8.out 2>&1 &