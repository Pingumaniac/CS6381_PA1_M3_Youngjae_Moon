###############################################
# Purpose: Skeleton/Starter code for the Discovery application
###############################################

# The key steps for the discovery application are
# (1) parse command line and configure application level parameters. 
# One of the parameters should be the total number of publishers and subscribers in the system.
# (2) obtain the discovery middleware object and configure it.
# (3) since we are a server, we always handle events in an infinite event loop.
# See publisher code to see how the event loop is written. Accordingly, when a
# message arrives, the middleware object parses the message and determines
# what method was invoked and then hands it to the application logic to handle it
# (4) Some data structure or in-memory database etc will need to be used to save the registrations.
# (5) When all the publishers and subscribers in the system have registered with us,
# then we are in a ready state and will respond with a true to is_ready method. Until then it will be false.

# import the needed packages
import os     # for OS functions
import sys    # for syspath and system exception
import time   # for sleep
import argparse # for argument parsing
import configparser # for configuration parsing
import logging # for logging. Use it in place of print statements.
import random # needed in the topic selection using random numbers

# Import our topic selector. Feel free to use alternate way to get your topics of interest
from topic_selector import TopicSelector

# Now import our CS6381 Middleware
from CS6381_MW.DiscoveryMW import DiscoveryMW
from CS6381_MW import discovery_pb2
from CS6381_MW import topic_pb2

from enum import Enum  # for an enumeration we are using to describe what state we are in

class DiscoveryAppln():
    # these are the states through which our Discovery appln object goes thru.
    # We maintain the state so we know where we are in the lifecycle and then take decisions accordingly
    class State(Enum):
        INITIALIZE = 0,
        CONFIGURE = 1,
        REGISTER = 2,
        COMPLETED = 3
    
    def __init__(self, logger):
        self.state = self.State.INITIALIZE # state that are we in
        self.name = None # our name (some unique name)
        self.topiclist = None # the different topics in the discovery service
        self.iters = None   # number of iterations of publication
        self.frequency = None # rate at which dissemination takes place
        self.num_topics = None # total num of topics in the discovery service
        self.mw_obj = None # handle to the underlying Middleware object
        self.logger = logger  # internal logger for print statements
        self.no_pubs = 0 # Initialise to 0
        self.no_subs = 0 # Initialise to 0
        self.no_broker = 0 # Initialise to 0
        self.pub_list = [] # Initialise to empty list
        self.sub_list = [] # Initialise to empty list
        self.broker_list = [] # Initalise to empty list
        self.lookup = None # one of the diff ways we do lookup
        self.dissemination = None # direct or via broker
        self.is_ready = False
    
    def configure(self, args):
        ''' Initialize the object '''
        try:
            self.logger.info("DiscoveryAppln::configure")
            self.state = self.State.CONFIGURE
            self.name = args.name # our name
            self.iters = args.iters  # num of iterations
            self.frequency = args.frequency # frequency with which topics are disseminated
            self.num_topics = args.num_topics  # total num of topics we publish
            self.no_pubs = args.no_pubs
            self.no_subs = args.no_subs
            self.no_broker = args.no_broker
            config = configparser.ConfigParser()
            config.read(args.config)
            self.lookup = config["Discovery"]["Strategy"]
            self.dissemination = config["Dissemination"]["Strategy"]
            self.mw_obj = DiscoveryMW(self.logger)
            self.mw_obj.configure(args) # pass remainder of the args to the m/w object
            self.logger.info("DiscoveryAppln::configure - configuration complete")
        except Exception as e:
            raise e
    
    def driver (self):
        ''' Driver program '''
        try:
            self.logger.info("DiscoveryAppln::driver")
            self.dump()
            self.logger.info("DiscoveryAppln::driver - upcall handle")
            self.mw_obj.set_upcall_handle(self)
            self.state = self.State.REGISTER
            self.mw_obj.event_loop(timeout=0)  # start the event loop
            self.logger.info("DiscoveryAppln::driver completed")
        except Exception as e:
            raise e
    
    def register_request(self, reg_request):
        try:
            self.logger.info("DiscoveryAppln::register_request")
            status = False # success = True, failure = False
            reason = ""
            if reg_request.role == discovery_pb2.ROLE_PUBLISHER:
                self.logger.info("DiscoveryAppln::register_request - ROLE_PUBLISHER")
                if len(self.pub_list) != 0:
                    for pub in self.pub_list:
                        if pub[0] == reg_request.info.id:
                            reason = "The publisher name is not unique."
                if reason == "":
                    self.pub_list.append([reg_request.info.id, reg_request.info.addr, reg_request.info.port, reg_request.topiclist])
                    status = True
                    reason = "The publisher name is unique."
            elif reg_request.role == discovery_pb2.ROLE_SUBSCRIBER:
                self.logger.info("DiscoveryAppln::register_request - ROLE_SUBSCRIBER")
                if len(self.sub_list) != 0:
                    for sub in self.sub_list:
                        if sub[0] == reg_request.info.id:
                            reason = "The subscriber name is not unique."
                if reason == "":
                    self.sub_list.append([reg_request.info.id, reg_request.info.addr, reg_request.info.port, reg_request.topiclist])
                    status = True
                    reason = "The subscriber name is unique."
            elif reg_request.role == discovery_pb2.ROLE_BOTH:
                self.logger.info("DiscoveryAppln::register_request - ROLE_BOTH")
                if len(self.broker_list) != 0:
                    reason = "There should be only one broker."
                if reason == "":
                    self.broker_list.append([reg_request.info.id, reg_request.info.addr, reg_request.info.port, reg_request.topiclist])
                    # Broker as as both publisher and subscriber
                    self.pub_list.append([reg_request.info.id, reg_request.info.addr, reg_request.info.port, reg_request.topiclist])
                    self.sub_list.append([reg_request.info.id, reg_request.info.addr, reg_request.info.port, reg_request.topiclist])
                    status = True
                    reason = "The broker name is unique and there is only one broker."
            else:
                raise Exception("Role unknown: Should be either publisher, subscriber, or broker.")
            if len(self.pub_list) >= self.no_pubs and len(self.sub_list) >= self.no_subs:
                self.is_ready = True
            self.mw_obj.handle_register(status, reason)
            return 0
        except Exception as e:
            raise e

    def isready_request(self):
        try:
            self.logger.info("DiscoveryAppln:: isready_request")
            self.mw_obj.update_is_ready_status(True)
            return 0
        except Exception as e:
            raise e
    
    def handle_topic_request(self, topic_req):
        try:
            self.logger.info("DiscoveryAppln::handle_topic_request - start")
            pubTopicList = []
            for pub in self.pub_list:
                if any(topic in pub[3] for topic in topic_req.topiclist):
                    self.logger.info("DiscoveryAppln::handle_topic_request - add pub")
                    pubTopicList.append([pub[0], pub[1], pub[2]])     
            self.mw_obj.send_pubinfo_for_topic(pubTopicList)
            return 0
        except Exception as e:
            raise e
    
    def handle_all_publist(self):
        try:
            self.logger.info ("DiscoveryAppln:: handle_all_publist")
            pubWithoutTopicList = []
            if len(self.pub_list) != 0:
                for pub in self.pub_list:
                    pubWithoutTopicList.append([pub[0], pub[1], pub[2]])
            else:
                pubWithoutTopicList = []
            self.mw_obj.send_all_pub_list(pubWithoutTopicList)
            return 0
        except Exception as e:
            raise e
    
    def dump(self):
        try:
            self.logger.info ("**********************************")
            self.logger.info ("DiscoveryAppln::dump")
            self.logger.info ("     Name: {}".format (self.name))
            self.logger.info ("     Lookup: {}".format (self.lookup))
            self.logger.info ("     Num Topics: {}".format (self.num_topics))
            self.logger.info ("     TopicList: {}".format (self.topiclist))
            self.logger.info ("     Iterations: {}".format (self.iters))
            self.logger.info ("     Frequency: {}".format (self.frequency))
            self.logger.info ("**********************************")
        except Exception as e:
            raise e
 
def parseCmdLineArgs ():
    # instantiate a ArgumentParser object
    parser = argparse.ArgumentParser (description="Discovery Application")
    # Now specify all the optional arguments we support
    parser.add_argument("-n", "--name", default="discovery", help="Discovery")
    parser.add_argument("-r", "--addr", default="localhost", help="IP addr of this publisher to advertise (default: localhost)")
    parser.add_argument("-t", "--port", type=int, default=5555, help="Port number on which our underlying publisher ZMQ service runs, default=5577")
    parser.add_argument("-P", "--no_pubs", type=int, default=1, help="Number of publishers")
    parser.add_argument("-S", "--no_subs", type=int, default=1, help="Number of subscribers")
    parser.add_argument("-B", "--no_broker", type=int, default=1, help="Number of brokers")
    parser.add_argument("-T", "--num_topics", type=int, choices=range(1,10), default=1, help="Number of topics to publish, currently restricted to max of 9")
    parser.add_argument("-c", "--config", default="config.ini", help="configuration file (default: config.ini)")
    parser.add_argument("-f", "--frequency", type=int,default=1, help="Rate at which topics disseminated: default once a second - use integers")
    parser.add_argument("-i", "--iters", type=int, default=1000, help="number of publication iterations (default: 1000)")
    parser.add_argument("-l", "--loglevel", type=int, default=logging.INFO, choices=[logging.DEBUG,logging.INFO,logging.WARNING,logging.ERROR,logging.CRITICAL], help="logging level, choices 10,20,30,40,50: default 20=logging.INFO")
    return parser.parse_args()
    
def main():
    try:
        logging.info("Main - acquire a child logger and then log messages in the child")
        logger = logging.getLogger("DiscoveryAppln")
        logger.debug("Main: parse command line arguments")
        args = parseCmdLineArgs()
        logger.debug("Main: resetting log level to {}".format (args.loglevel))
        logger.setLevel(args.loglevel)
        logger.debug("Main: effective log level is {}".format (logger.getEffectiveLevel ()))
        logger.debug("Main: obtain the Discovery appln object")
        discovery_app = DiscoveryAppln(logger)
        logger.debug("Main: configure the Discovery appln object")
        discovery_app.configure(args)
        logger.debug("Main: invoke the Discovery appln driver")
        discovery_app.driver()
    except Exception as e:
        logger.error("Exception caught in main - {}".format (e))
        return

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    main()