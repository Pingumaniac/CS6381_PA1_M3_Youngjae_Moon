# Here is what this middleware should do
# (1) it must maintain the ZMQ sockets, one in the REP role to talk to the Discovery service
# and one in the SUB role to receive topic data
# (2) It must, on behalf of the application logic, register the subscriber application with the
# discovery service. To that end, it must use the protobuf-generated serialization code to
# send the appropriate message with the contents to the discovery service.
# (3) On behalf of the subscriber appln, it must use the ZMQ setsockopt method to subscribe to all the
# user-supplied topics of interest. 
# (4) Since it is a receiver, the middleware object will maintain a poller and even loop waiting for some
# subscription to show up (or response from Discovery service).
# (5) On receipt of a subscription, determine which topic it is and let the application level
# handle the incoming data. To that end, you may need to make an upcall to the application-level
# object.

# import the needed packages
import os     # for OS functions
import sys    # for syspath and system exception
import time   # for sleep
import logging # for logging. Use it in place of print statements.
import zmq  # ZMQ sockets
from CS6381_MW import discovery_pb2
from CS6381_MW import topic_pb2
from CS6381_MW.Common import PinguMW

class SubscriberMW(PinguMW):

  def __init__(self, logger):
    super().__init__(logger)
    self.req = None # will be a ZMQ REQ socket to talk to Discovery service
    self.sub = None # will be a ZMQ SUB socket for dissemination

  def configure(self, args):
    try:
      self.logger.info("SubscriberMW::configure")
      self.port = args.port
      self.addr = args.addr
      context = zmq.Context()  # returns a singleton object
      self.poller = zmq.Poller()
      self.req = context.socket(zmq.REQ)
      self.sub = context.socket(zmq.SUB)
      self.poller.register(self.req, zmq.POLLIN)
      connect_str = "tcp://" + args.discovery
      self.req.connect(connect_str)
      self.connect2pubs(self.addr, self.port)
      self.logger.info("SubscriberMW::configure completed")
    except Exception as e:
      raise e

  # run the event loop where we expect to receive sth
  def event_loop(self, timeout=None):
    super().event_loop("SubscriberMW", self.req, timeout)
            
  # handle an incoming reply
  def handle_reply(self):
    try:
      self.logger.info("SubscriberMW::handle_reply")
      bytesRcvd = self.req.recv()
      discovery_response = discovery_pb2.DiscoveryResp()
      discovery_response.ParseFromString(bytesRcvd)
      if (discovery_response.msg_type == discovery_pb2.TYPE_REGISTER):
        timeout = self.upcall_obj.register_response(discovery_response.register_resp)
      elif (discovery_response.msg_type == discovery_pb2.TYPE_ISREADY):
        timeout = self.upcall_obj.isready_response(discovery_response.isready_resp)
      elif (discovery_response.msg_type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC):
        timeout = self.upcall_obj.receiveSubscribedPublishersResponse(discovery_response.lookup_resp)
      else: 
        raise ValueError ("Unrecognized response message")
      return timeout
    except Exception as e:
      raise e
            
  def register (self, name, topiclist):
    super().register("SubscriberMW", name, topiclist)

  def is_ready(self):
    super().is_ready("SubscriberMW")

  # Receive a list of publishers for the topics subscribed
  def receiveSubscribedPublishers(self, topiclist):
    try:
      self.logger.info("SubscriberMW::receiveSubscribedPublishers - start")
      lookup_request = discovery_pb2.LookupPubByTopicReq()
      lookup_request.topiclist[:] = topiclist
      discovery_request = discovery_pb2.DiscoveryReq()
      discovery_request.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC
      discovery_request.lookup_req.CopyFrom(lookup_request)
      buf2send = discovery_request.SerializeToString()
      self.req.send(buf2send) 
      self.logger.info("SubscriberMW::receiveSubscribedPublishers - end")
    except Exception as e:
      raise e
  
  def makeSubscription(self, pub, topiclist):
    try:
      self.logger.info("SubscriberMW::makeSubscription - Connect SUB with tcp://{}:{}".format(pub.addr, pub.port))
      self.sub.connect("tcp://{}:{}".format(pub.addr, pub.port))
      for topic in topiclist:
        self.sub.setsockopt_string(zmq.SUBSCRIBE, topic)
    except Exception as e:
      raise e
    
  def receive(self):
    try:
      self.logger.info("SubscriberMW:: receive messages")
      msg = self.sub.recv_string()
      self.logger.info("SubscriberMW:: received message = {}".format (msg))   
      return msg 
    except Exception as e:
      raise e
            
  # here we save a pointer (handle) to the application object
  def set_upcall_handle(self, upcall_obj):
    super().set_upcall_handle(upcall_obj)
        
  def disable_event_loop(self):
    super().disable_event_loop()
  
  # connect to pubs
  def connect2pubs(self, IP, port):
    connect_str = "tcp://" + IP + ":" + str(port)
    self.logger.info("SubscriberMW:: connect2pubs method. connect_str = {}".format(connect_str))
    self.sub.connect(connect_str)