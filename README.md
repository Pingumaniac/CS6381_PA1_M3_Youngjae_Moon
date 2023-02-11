# CS6381_PA1_M3_Youngjae_Moon
About CS6381 PA1 Milestone3 Youngjae Moon

### What I have done for Milestone 3
1. Dissemination strategy using the direct approach
2. Publishers should be able to disseminate; subscribers receive based on topics
3. Dissemination strategy using Broker
4. Made Test.txt file for testing out sample cases on Mininet (5 sample test cases)
5. Made data_visualization.py to visualize data after testing out sample cases on Mininet
6. Modified the structure of Broker - used SUB and PUB sockets that subscribed and publishes on all topics. Commented out my legacy code that uses XPUB and XSUB 
7. Removed handle_reply_forPubSub() function in Common.py and implemented handle_reply() function for BrokerMW, SubscriberMW, and PublisherMW back to fix error that the discovery application is still holding on an event loop.

Note that InfluxDB has not been used for data visualization purposes as the professor told us in the class on Feb 10 (yesterday) that InfluxDB is not required.

### Basic Descripton of the code
1. Publisher, Subscriber, Broker registers on Discovery Service
2. Publisher generates all sample messages
3. Publisher passes these messages to Broker
4. Publisher and Subscriber identifies each other based on their topic interest.
5. Broker gets info about which topics each subscriber has subscribed.
6. For Broker approach, Broker then passes messages to the subscribers. Here a new label tag has been added to all messages at the end: "(from broker)"
7. For Direct approach, Publishers then sends messages directly to the subscribers.

For 3 and 7, note that the SUB socket for Broker has subscribed to all topics and hence when Publisher tries to disseminate messages based on topics, it will automatically send to the broker as well as to the subscribers.

### How to run the code
1. Open Ubuntu 22.04
2. Download all the codes on my repository
3. Open Terminal and move to the corresponding directory
4. Open Test.txt and copy and paste the codes written for each test case.
5. Run data_visualization.py and run the code.
6. Sample graphs/tables would have then been generated.
