# CS6381_PA1_M3_Youngjae_Moon
About CS6381 PA1 Milestone3 Youngjae Moon

### What I have done for Milestone 3
1. Dissemination strategy using the direct approach
2. Publishers should be able to disseminate; subscribers receive based on topics
3. Dissemination strategy using Broker
4. Made Test.txt file for testing out sample cases on Mininet
5. Made data_visualization.py to visualize data after testing out sample cases on Mininet
6. Modified the structure of Broker - used SUB and PUB sockets that subscribed and publishes on all topics. Commented out my legacy code that uses XPUB and XSUB 

### Basic Descripton of the code
1. Publisher, Subscriber, Broker registers on Discovery Service
2. Publisher generates all sample messages
3. Publisher passs these messages to Broker
4. Publisher and Subscriber identifies each other based on their topic interest.
5. Broker gets info about which topics each subscriber has subscribed.
6. For Broker approach, Broker then passes messages to the subscribers. Here a new label tag has been added to all messages: "From Broker: "
7. For Direct approach, Publishers then sends messages directly to the subscribers.

### How to run the code
1. Open Ubuntu 22.04
2. Download all the codes on my repository
3. Open Terminal and move to the corresponding directory
4. Open Test.txt and copy and paste the codes written for each test case.
5. Run data_visualization.py and run the code.
6. Sample graphs/tables would have then been generated.
