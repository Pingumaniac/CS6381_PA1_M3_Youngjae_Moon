# As mentioned earlier, a broker serves as a proxy and hence has both
# publisher and subscriber roles. So in addition to the REQ socket to talk to the
# Discovery service, it will have both PUB and SUB sockets as it must work on
# behalf of the real publishers and subscribers. So this will have the logic of
# both publisher and subscriber middleware.

# import the needed packages
import os     # for OS functions
import sys    # for syspath and system exception
import time   # for sleep
import argparse # for argument parsing
import configparser # for configuration parsing
import logging # for logging. Use it in place of print statements.
import zmq  # ZMQ sockets
from CS6381_MW import discovery_pb2
from CS6381_MW import topic_pb2
from CS6381_MW.Common import PinguMW

class BrokerMW(PinguMW):
    def __init__ (self, logger):
        super().__init__(logger)
        self.req = None # will be a ZMQ REQ socket to talk to Discovery service
        self.pub = None # will be a ZMQ XPUB socket for representing publisher
        self.sub = None # will be a ZMQ XSUB socket for representing publisher
        
    # configure/initialize
    def configure (self, args):
        ''' Initialize the object '''
        try:
            self.logger.info("BrokerMW::configure")
            self.port = args.port
            self.addr = args.addr
            context = zmq.Context()  # returns a singleton object
            self.poller = zmq.Poller()
            self.req = context.socket(zmq.REQ)
            self.pub = context.socket(zmq.PUB)
            self.sub = context.socket(zmq.SUB)
            self.poller.register(self.req, zmq.POLLIN)
            connect_str = "tcp://" + args.discovery
            self.req.connect(connect_str)
            bind_string = "tcp://*:" + str(self.port)
            self.pub.bind(bind_string)
            self.logger.info("BrokerMW::configure completed")
        except Exception as e:
            raise e
        
    # run the event loop where we expect to receive a reply to a sent request
    def event_loop(self, timeout=None):
        super().event_loop("BrokerMW", self.req, timeout)
    
    def handle_reply(self):
        try:
            self.logger.info("BrokerMW::handle_reply")
            bytesRcvd = self.req.recv()
            discovery_response = discovery_pb2.DiscoveryResp()
            discovery_response.ParseFromString(bytesRcvd)
            if (discovery_response.msg_type == discovery_pb2.TYPE_REGISTER):
                timeout = self.upcall_obj.register_response(discovery_response.register_resp)
            elif (discovery_response.msg_type == discovery_pb2.TYPE_ISREADY):
                timeout = self.upcall_obj.isready_response(discovery_response.isready_resp)
            elif (discovery_response.msg_type == discovery_pb2.TYPE_LOOKUP_ALL_PUBS):
                timeout = self.upcall_obj.allPublishersResponse(discovery_response.allpubs_resp)
            else: 
                raise ValueError ("Unrecognized response message")
            return timeout
        except Exception as e:
            raise e
    
    def register(self, name, topiclist):
        super().register("BrokerMW", name, topiclist)
    
    def is_ready(self):
        super().is_ready("BrokerMW")
    
    # here we save a pointer (handle) to the application object
    def set_upcall_handle(self, upcall_obj):
        super().set_upcall_handle(upcall_obj)
        
    def disable_event_loop(self):
        super().disable_event_loop()
        
    def receive_msg_sub(self):
        try:
            self.logger.info("BrokerMW::recv_msg_sub - receive messages")
            msg = self.sub.recv_string()
            self.logger.info("BrokerMW::recv_msg_sub - received message = {}".format (msg))   
            return msg 
        except Exception as e:
            raise e
    
    def send_msg_pub(self, send_str):
        try:
            self.logger.info("BrokerMW::send_msg_pub - disseminate messages to subscribers from broker")
            self.logger.info("BrokerMW::send_msg_pub - {}".format (send_str))
            self.pub.send(bytes(send_str, "utf-8"))
        except Exception as e:
            raise e
    
    def receiveAllPublishers(self):
        try:
            self.logger.info("BrokerMW::receiveAllPublishers - start")
            allpubs_request = discovery_pb2.LookupAllPubsReq()
            discovery_request = discovery_pb2.DiscoveryReq()
            discovery_request.msg_type = discovery_pb2.TYPE_LOOKUP_ALL_PUBS
            discovery_request.allpubs_req.CopyFrom(allpubs_request)
            buf2send = discovery_request.SerializeToString()
            self.req.send(buf2send) 
            self.logger.info("BrokerMW::receiveAllPublishers - end")
        except Exception as e:
            raise e
    
    def connect2pubs(self, IP, port):
        connect_str = "tcp://" + IP + ":" + str(port)
        self.logger.info("BrokerMW:: connect2pubs method. connect_str = {}".format(connect_str))
        self.sub.connect(connect_str)