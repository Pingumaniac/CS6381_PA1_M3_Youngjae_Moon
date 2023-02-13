# This is left as an exercise for the student.  The Broker is involved only when
# the dissemination strategy is via the broker.
# A broker is an intermediary; thus it plays both the publisher and subscriber roles
# but in the form of a proxy. For instance, it serves as the single subscriber to
# all publishers. On the other hand, it serves as the single publisher to all the subscribers. 

# import the needed packages
import os     # for OS functions
import sys    # for syspath and system exception
import time   # for sleep
import argparse # for argument parsing
import configparser # for configuration parsing
import logging # for logging. Use it in place of print statements.
import random # needed in the topic selection using random numbers
from topic_selector import TopicSelector
from CS6381_MW.BrokerMW import BrokerMW
from CS6381_MW import discovery_pb2
from CS6381_MW import topic_pb2

from enum import Enum  # for an enumeration we are using to describe what state we are in

class BrokerAppln ():
    # these are the states through which our Broker appln object goes thru.
    # We maintain the state so we know where we are in the lifecycle and then
    # take decisions accordingly
    class State (Enum):
        INITIALIZE = 0,
        CONFIGURE = 1,
        REGISTER = 2,
        ISREADY = 3,
        CHECKMSG = 4,
        RECEIVEFROMPUB = 5
        DISSEMINATE = 6,
        COMPLETED = 7
    
    def __init__ (self, logger):
        self.state = self.State.INITIALIZE # state that are we in
        self.name = None # our name (some unique name)
        self.topiclist = None # the different topics that we publish on
        self.iters = None   # number of iterations of publication
        self.frequency = None # rate at which dissemination takes place
        self.mw_obj = None # handle to the underlying Middleware object
        self.logger = logger  # internal logger for print statements
        self.msg_list = [] # Initalise to empty list
        self.lookup = None # one of the diff ways we do lookup
        self.dissemination = None # direct or via broker
        self.is_ready = None
    
    def configure(self, args):
        try:
            self.logger.info("BrokerAppln::configure")
            self.state = self.State.CONFIGURE
            self.name = args.name # our name
            self.iters = args.iters  # num of iterations
            self.frequency = args.frequency # frequency with which topics are disseminated
            config = configparser.ConfigParser()
            config.read(args.config)
            self.lookup = config["Discovery"]["Strategy"]
            self.dissemination = config["Dissemination"]["Strategy"]
            self.mw_obj = BrokerMW(self.logger)
            self.mw_obj.configure(args) # pass remainder of the args to the m/w object
            self.topiclist = ["weather", "humidity", "airquality", "light", "pressure", "temperature", "sound", "altitude", "location"] # Subscribe to all topics
            self.logger.info("BrokerAppln::configure - configuration complete")
        except Exception as e:
            raise e
    
    def driver(self):
        try:
            self.logger.info("BrokerAppln::driver")
            self.dump()
            self.logger.info("BrokerAppln::driver - upcall handle")
            self.mw_obj.set_upcall_handle(self)
            self.state = self.State.REGISTER
            self.mw_obj.event_loop(timeout=0)  # start the event loop
            self.logger.info("BrokerAppln::driver completed")
        except Exception as e:
            raise e
    
    def invoke_operation(self):
        try:
            self.logger.info("BrokerAppln::invoke_operation")
            if (self.state == self.State.REGISTER):
                self.logger.info("BrokerAppln::invoke_operation - register with the discovery service")
                self.mw_obj.register(self.name, self.topiclist)
                return None
            elif (self.state == self.State.ISREADY):
                self.logger.info("BrokerAppln::invoke_operation - check if are ready to go")
                self.mw_obj.is_ready()  # send the is_ready? request
                return None
            elif (self.state == self.State.CHECKMSG):
                self.logger.info("BrokerAppln::invoke_operation - start checking messages from all publishers")
                self.mw_obj.receiveAllPublishers()
                return None
            elif (self.state == self.State.RECEIVEFROMPUB):
                self.logger.info("BrokerAppln::invoke_operation - RECEIVING Messages as shown below:")
                msg = self.mw_obj.receive_msg_sub()
                self.msg_list.append(msg)
                self.logger.info("BrokerAppln::invoke_operation - msg: " + str(msg))
                return self.frequency
            elif (self.state == self.State.DISSEMINATE):
                self.logger.info("BrokerAppln::invoke_operation - start Disseminating")
                for msg in self.msg_list:
                    self.mw_obj.send_msg_pub(msg + ":(from broker)")
                    time.sleep(1/float (self.frequency))  # ensure we get a floating point num
                self.logger.info("BrokerAppln::invoke_operation - Dissemination completed")
                self.state = self.State.COMPLETED
                return 0
            elif (self.state == self.State.COMPLETED):
                self.mw_obj.disable_event_loop()
                return None
            else:
                raise ValueError ("Undefined state of the appln object")
            self.logger.info ("BrokerAppln::invoke_operation completed")
        except Exception as e:
            raise e

    def register_response(self, reg_resp):
        try:
            self.logger.info ("BrokerAppln::register_response")
            if (reg_resp.status == discovery_pb2.STATUS_SUCCESS):
                self.logger.info("BrokerAppln::register_response - registration is a success")
                self.state = self.State.ISREADY  
                return 0  
            else:
                self.logger.info("BrokerAppln::register_response - registration is a failure with reason {}".format (reg_resp.reason))
                raise ValueError ("Broker needs to have unique id")
        except Exception as e:
            raise e

    # handle isready response method called as part of upcall
    # Also a part of upcall handled by application logic
    def isready_response(self, isready_resp):
        try:
            self.logger.info("BrokerAppln::isready_response")
            if not isready_resp.status:
                self.logger.info("BrokerAppln::driver - Not ready yet; check again")
                time.sleep(10)  # sleep between calls so that we don't make excessive calls
            else:
                # we got the go ahead + set the state to CHECKMSG
                self.state = self.State.CHECKMSG
            return 0
        except Exception as e:
            raise e

    def allPublishersResponse(self, check_response):
        try:
            self.logger.info("BrokerAppln::allPublishersResponse")
            for pub in check_response.publist:
                self.logger.info("tcp://{}:{}".format(pub.addr, pub.port))
                self.mw_obj.connect2pubs(pub.addr, pub.port)
            self.state = self.State.RECEIVEFROMPUB
            return 0
        except Exception as e:
            raise e
    
    def dump(self):
        try:
            self.logger.info("**********************************")
            self.logger.info("BrokerAppln::dump")
            self.logger.info("     Name: {}".format (self.name))
            self.logger.info("     Lookup: {}".format (self.lookup))
            self.logger.info("     Num Topics: {}".format (len(self.topiclist)))
            self.logger.info("     TopicList: {}".format (self.topiclist))
            self.logger.info("     Iterations: {}".format (self.iters))
            self.logger.info("     Frequency: {}".format (self.frequency))
            self.logger.info("**********************************")
        except Exception as e:
            raise e

# Parse command line arguments
def parseCmdLineArgs ():
    # instantiate a ArgumentParser object
    parser = argparse.ArgumentParser(description="Broker Application")
    # Now specify all the optional arguments we support
    parser.add_argument("-n", "--name", default="broker", help="broker")
    parser.add_argument("-r", "--addr", default="localhost", help="IP addr of this broker to advertise (default: localhost)")
    parser.add_argument("-p", "--port", type=int, default=5578, help="Port number on which our underlying Broker ZMQ service runs, default=5576")
    parser.add_argument("-d", "--discovery", default="localhost:5555", help="IP Addr:Port combo for the discovery service, default localhost:5555")
    parser.add_argument("-c", "--config", default="config.ini", help="configuration file (default: config.ini)")
    parser.add_argument("-f", "--frequency", type=int,default=1, help="Rate at which topics disseminated: default once a second - use integers")
    parser.add_argument("-i", "--iters", type=int, default=1000, help="number of publication iterations (default: 1000)")
    parser.add_argument("-l", "--loglevel", type=int, default=logging.INFO, choices=[logging.DEBUG,logging.INFO,logging.WARNING,logging.ERROR,logging.CRITICAL], help="logging level, choices 10,20,30,40,50: default 20=logging.INFO")
    return parser.parse_args()

def main():
    try:
        # obtain a system wide logger and initialize it to debug level to begin with
        logging.info("Main - acquire a child logger and then log messages in the child")
        logger = logging.getLogger("BrokerAppln")
        # first parse the arguments
        logger.debug ("Main: parse command line arguments")
        args = parseCmdLineArgs()
        # reset the log level to as specified
        logger.debug("Main: resetting log level to {}".format (args.loglevel))
        logger.setLevel(args.loglevel)
        logger.debug("Main: effective log level is {}".format (logger.getEffectiveLevel ()))
        # Obtain a Broker application
        logger.debug("Main: obtain the Broker appln object")
        broker_app = BrokerAppln (logger)
        # configure the object
        logger.debug("Main: configure the Broker appln object")
        broker_app.configure(args)
        # now invoke the driver program
        logger.debug("Main: invoke the Broker appln driver")
        broker_app.driver()

    except Exception as e:
        logger.error ("Exception caught in main - {}".format (e))
        return

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    main()