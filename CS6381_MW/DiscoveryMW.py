# The discovery service is a server. So at the middleware level, we will maintain
# a REP socket binding it to the port on which we expect to receive requests.
# There will be a forever event loop waiting for requests. Each request will be parsed
# and the application logic asked to handle the request. To that end, an upcall will need
# to be made to the application logic.

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

class DiscoveryMW(PinguMW):
    def __init__ (self, logger):
        super().__init__(logger)
        self.rep = None # will be a ZMQ REP socket for reply
        
    # configure/initialize
    def configure (self, args):
        ''' Initialize the object '''
        try:
            self.logger.info("DiscoveryMW::configure")
            self.port = args.port
            self.addr = args.addr
            context = zmq.Context()  # returns a singleton object
            self.poller = zmq.Poller()
            self.rep = context.socket(zmq.REP)
            self.poller.register(self.rep, zmq.POLLIN)
            bind_string = "tcp://*:" + str(self.port)
            self.rep.bind(bind_string)
            self.logger.info("DiscoveryMW::configure completed")
        except Exception as e:
            raise e
        
    # run the event loop where we expect to receive sth
    def event_loop(self, timeout=None):
        super().event_loop("DiscoveryMW", self.rep, timeout)
        
    def handle_request(self):
        try:
            self.logger.info("DiscoveryMW::handle_request")
            bytesRcvd = self.rep.recv()
            disc_req = discovery_pb2.DiscoveryReq()
            disc_req.ParseFromString(bytesRcvd)
            self.logger.info("DiscoveryMW::handle_request - bytes received")
            if (disc_req.msg_type == discovery_pb2.TYPE_REGISTER):
                self.logger.info("DiscoveryMW::handle_request - register")
                timeout = self.upcall_obj.register_request(disc_req.register_req)
            elif (disc_req.msg_type == discovery_pb2.TYPE_ISREADY):
                self.logger.info("DiscoveryMW::handle_request - is ready")
                timeout = self.upcall_obj.isready_request()
            elif (disc_req.msg_type == discovery_pb2.TYPE_LOOKUP_ALL_PUBS):
                self.logger.info("DiscoveryMW::handle_request - all pubs")
                timeout = self.upcall_obj.handle_all_publist()
            elif (disc_req.msg_type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC):
                self.logger.info("DiscoveryMW::handle_request - pub by topic")
                timeout = self.upcall_obj.handle_topic_request(disc_req.lookup_req)
            else: 
                raise ValueError("Unrecognized response message")
            return timeout
        except Exception as e:
            raise e
    
    def handle_register(self, status, reason):
        try:
            self.logger.info("DiscoveryMW::handle_register:: check whether the registration has been successful")
            register_response = discovery_pb2.RegisterResp() 
            if status: # if status is true, registration = successful
                register_response.status = discovery_pb2.Status.STATUS_SUCCESS
            else: # otherwise failure
                register_response.status = discovery_pb2.Status.STATUS_FAILURE
            register_response.reason = reason
            discovery_response = discovery_pb2.DiscoveryResp()
            discovery_response.msg_type = discovery_pb2.TYPE_REGISTER
            discovery_response.register_resp.CopyFrom(register_response)
            buf2send = discovery_response.SerializeToString()
            self.rep.send(buf2send)
            self.logger.info("DiscoveryMW::handle_register:: registration status has been checked. plz check the message")
            return 0
        except Exception as e:
            raise e
        
    def update_is_ready_status(self, is_ready):
        try:
            self.logger.info("DiscoveryMW::update_is_ready_status:: Start this method")
            ready_response = discovery_pb2.IsReadyResp() 
            ready_response.status = is_ready
            discovery_response = discovery_pb2.DiscoveryResp()
            discovery_response.msg_type = discovery_pb2.TYPE_ISREADY
            discovery_response.isready_resp.CopyFrom(ready_response)
            buf2send = discovery_response.SerializeToString()
            self.rep.send(buf2send)
            self.logger.info("DiscoveryMW::update_is_ready_status:: is_ready status sent.")
        except Exception as e:
            raise e

    def send_pubinfo_for_topic(self, pub_in_topic):
        try:
            self.logger.info("DiscoveryMW::send_pubinfo_for_topic:: Start this method")
            lookup_response = discovery_pb2.LookupPubByTopicResp() 
            for pub in pub_in_topic:
                reg_info = lookup_response.publisher_info.add()
                reg_info.id = pub[0] # name
                reg_info.addr = pub[1] # addr
                reg_info.port = pub[2] # port
                self.logger.info("DiscoveryMW::send_pubinfo_fo_topic:: Publisher address is tcp://{}:{}".format(reg_info.addr, reg_info.port))
            discovery_response = discovery_pb2.DiscoveryResp()
            discovery_response.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC
            discovery_response.lookup_resp.CopyFrom(lookup_response)
            buf2send = discovery_response.SerializeToString()
            self.rep.send(buf2send)  
            self.logger.info ("DiscoveryMW::send_pubinfo_for_topic:: List of publishers sent")
        except Exception as e:
            raise e
    
    def send_all_pub_list(self, pub_list):
        try:
            self.logger.info ("DiscoveryMW::send_all_pub_list:: Start this method")
            lookup_response = discovery_pb2.LookupAllPubsResp()
            for pub in pub_list:
                reg_info = lookup_response.publist.add()
                reg_info.id = pub[0] # name
                reg_info.addr = pub[1] # addr
                reg_info.port = pub[2] # port
                self.logger.info("DiscoveryMW::send_all_pub_list:: Publisher address is tcp://{}:{}".format(reg_info.addr, reg_info.port))
            discovery_response = discovery_pb2.DiscoveryResp()
            discovery_response.msg_type = discovery_pb2.TYPE_LOOKUP_ALL_PUBS
            discovery_response.allpubs_resp.CopyFrom(lookup_response)
            buf2send = discovery_response.SerializeToString()
            self.rep.send(buf2send)
        except Exception as e: 
            raise e
    
    # here we save a pointer (handle) to the application object
    def set_upcall_handle(self, upcall_obj):
        super().set_upcall_handle(upcall_obj)
        
    def disable_event_loop(self):
        super().disable_event_loop()