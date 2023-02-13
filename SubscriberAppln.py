# The key steps for the subscriber application are
# (1) parse command line and configure application level parameters
# (2) obtain the subscriber middleware object and configure it.
# (3) As in the Subscriber, register ourselves with the discovery service
# (4) since we are a subscriber, we need to ask the discovery service to
# let us know of each Subscriber that publishes the topic of interest to us. Then
# our middleware object will connect its SUB socket to all these Subscribers
# for the Direct strategy else connect just to the broker.
# (5) Subscriber will always be in an event loop waiting for some matching
# publication to show up. We also compute the latency for dissemination and
# store all these time series data in some database for later analytics.

# import the needed packages
import os     # for OS functions
import sys    # for syspath and system exception
import time   # for sleep
import argparse # for argument parsing
import configparser # for configuration parsing
import logging # for logging. Use it in place of print statements.
from topic_selector import TopicSelector
from CS6381_MW.SubscriberMW import SubscriberMW
from CS6381_MW import discovery_pb2
from CS6381_MW import topic_pb2

# import any other packages you need.
from enum import Enum  # for an enumeration we are using to describe what state we are in
import json
from datetime import datetime

class SubscriberAppln():
  # These are the states through which our Subscriber appln object goes thru. We maintain the state 
  # so we know where we are in the lifecycle and then take decisions accordingly
  class State(Enum):
    INITIALIZE = 0,
    CONFIGURE = 1,
    REGISTER = 2,
    ISREADY = 3,
    CHECKMSG = 4,
    RECEIVE = 5,
    COMPLETED = 6

  def __init__ (self, logger):
    self.name = None # our name (some unique name)
    self.topiclist = None # the different topics that we subscribe on
    self.iters = None   # number of iterations of publication
    self.frequency = None # rate at which dissemination takes place
    self.num_topics = None # total num of topics we subcribe
    self.mw_obj = None # handle to the underlying Middleware object
    self.logger = logger  # internal logger for print statements
    self.state = self.State.INITIALIZE # state that are we in
    self.lookup = None # one of the diff ways we do lookup
    self.dissemination = None # direct or via broker
    self.msg_list = []

  def configure (self, args):
    ''' Initialize the object '''
    try:
      self.logger.info ("SubscriberAppln::configure")
      self.state = self.State.CONFIGURE
      self.name = args.name # our name
      self.iters = args.iters  # num of iterations
      self.frequency = args.frequency # frequency with which topics are received
      self.num_topics = args.num_topics  # total num of topics we publish
      config = configparser.ConfigParser()
      config.read(args.config)
      self.lookup = config["Discovery"]["Strategy"]
      self.dissemination = config["Dissemination"]["Strategy"]
      self.logger.info("SubscriberAppln::configure - selecting our topic list")
      self.subscribeTopics()
      self.logger.info("SubscriberAppln::configure - initialize the middleware object")
      self.mw_obj = SubscriberMW(self.logger)
      self.mw_obj.configure(args) # pass remainder of the args to the m/w object
      self.logger.info("SubscriberAppln::configure - configuration complete")
    except Exception as e:
      raise e

  def driver (self):
    try:
      self.logger.info("SubscriberAppln::driver")
      self.dump()
      self.logger.info("SubscriberAppln::driver - upcall handle")
      self.mw_obj.set_upcall_handle(self)
      self.state = self.State.REGISTER
      self.mw_obj.event_loop(timeout=0)  # start the event loop
      self.logger.info("SubscriberAppln::driver completed")
    except Exception as e:
      raise e

  def invoke_operation (self):
    ''' Invoke operating depending on state  '''
    try:
      self.logger.info ("SubscriberAppln::invoke_operation")
      if (self.state == self.State.REGISTER):
        self.logger.info("SubscriberAppln::invoke_operation - register with the discovery service")
        self.mw_obj.register(self.name, self.topiclist)
        return None
      elif (self.state == self.State.ISREADY):
        self.logger.info ("SubscriberAppln::invoke_operation - check if are ready to go")
        self.mw_obj.is_ready()  # send the is_ready? request
        return None 
      elif (self.state == self.State.CHECKMSG):
        self.logger.info ("SubscriberAppln::invoke_operation - start checking messages")
        self.mw_obj.receiveSubscribedPublishers(self.topiclist)
        return None
      elif (self.state == self.State.RECEIVE):
        msg = self.mw_obj.receive()
        self.logger.info(msg)
        current_time = datetime.now().strftime('%H-%M-%S-%f')[:-3]
        self.msg_list.append([msg, current_time])
        self.logger.info("SubscriberAppln::invoke_operation - RECEIVING Messages as shown below: {}".format (msg))
        self.logger.info("SubscriberAppln::invoke_operation - Current time: {}".format (current_time))
        return self.frequency
      elif (self.state == self.State.COMPLETED):
        self.mw_obj.disable_event_loop()
        return None
      else:
        raise ValueError ("Undefined state of the appln object")
      self.logger.info ("SubscriberAppln::invoke_operation completed")
    except Exception as e:
      raise e

  def register_response(self, reg_resp):
    try:
      self.logger.info("SubscriberAppln::register_response")
      if (reg_resp.status == discovery_pb2.STATUS_SUCCESS):
        self.logger.info("SubscriberAppln::register_respons - registration is a success")
        # set our next state to isready so that we can then send the isready message right away
        self.state = self.State.ISREADY  
        # return a timeout of zero so that the event loop in its next iteration will immediately make
        # an upcall to us
        return 0  
      else:
        self.logger.info("SubscriberAppln::register_response - registration is a failure with reason {}".format (reg_resp.reason))
        raise ValueError("Subscriber needs to have unique id")
    except Exception as e:
      raise e

  def isready_response(self, isready_resp):
    try:
      self.logger.info("SubscriberAppln::isready_response")
      if not isready_resp.status:
        self.logger.info("SubscriberAppln::driver - Not ready yet; check again")
        time.sleep (10)  # sleep between calls so that we don't make excessive calls
      else:
        self.state = self.State.CHECKMSG
      return 0
    except Exception as e:
      raise e

  def dump(self):
    try:
      self.logger.info("**********************************")
      self.logger.info("SubscriberAppln::dump")
      self.logger.info("     Name: {}".format (self.name))
      self.logger.info("     Lookup: {}".format (self.lookup))
      self.logger.info("     Dissemination: {}".format (self.dissemination))
      self.logger.info("     Num Topics: {}".format (self.num_topics))
      self.logger.info("     TopicList: {}".format (self.topiclist))
      self.logger.info("     Iterations: {}".format (self.iters))
      self.logger.info("     Frequency: {}".format (self.frequency))
      self.logger.info("**********************************")
    except Exception as e:
      raise e
  
  def saveJSON(self, msg, current_time):
    try:
      msglist = msg.split(":")
      id = msglist[1]
      topic = msglist[0]
      disseminationdata = msglist[2]
      sent_time = msglist[3]
      receivedFromBroker = False
      if "(from broker)" in msg:
        receivedFromBroker = True
      t1 = datetime.strptime(sent_time, "%H-%M-%S-%f")
      t2 = datetime.strptime(current_time, "%H-%M-%S-%f")
      delta = t2 - t1
      sec = delta.total_seconds()
      latency = sec * 1000
      jsonDict = {
        "pub_id" : id,
        "topic" : topic,
        "disseminationdata" : disseminationdata,
        "sent_time" : sent_time,
        "sub_id" : self.name,
        "received_time" : current_time,
        "Num_topics_subscribed": self.num_topics,
        "latency" : latency, # in milliseconds
        "receivedFromBroker" : receivedFromBroker
      }
      jsonObj = json.dumps(jsonDict, indent=4)
      with open("sample.json", "a+") as outfile:
        outfile.write(jsonObj)
    except Exception as e:
      raise e
      
    
  def receiveSubscribedPublishersResponse(self, lookup_resp):
    try:
      self.logger.info("SubscriberAppln::receiveSubscribedPublishersResponse - start")
      for pub in lookup_resp.publisher_info:
        self.logger.info("tcp://{}:{}".format(pub.addr, pub.port))
        self.mw_obj.makeSubscription(pub, self.topiclist)
      self.state = self.State.RECEIVE
      return 0
    except Exception as e:
      raise e

  def subscribeTopics(self):
    topicSelector = TopicSelector()
    self.topiclist = topicSelector.interest(self.num_topics)  # let topic selector give us the desired num of topics

def parseCmdLineArgs():
  parser = argparse.ArgumentParser(description="Subscriber Application")
  parser.add_argument("-n", "--name", default="sub", help="Some name assigned to us. Keep it unique per Subscriber")
  parser.add_argument("-a", "--addr", default="localhost", help="IP addr of this Subscriber to advertise (default: localhost)")
  parser.add_argument("-p", "--port", type=int, default=5574, help="Port number on which our underlying Subscriber ZMQ service runs, default=5576")
  parser.add_argument("-d", "--discovery", default="localhost:5555", help="IP Addr:Port combo for the discovery service, default localhost:5555")
  parser.add_argument("-T", "--num_topics", type=int, choices=range(1,10), default=7, help="Number of topics to subscribe, currently restricted to max of 9")
  parser.add_argument("-c", "--config", default="config.ini", help="configuration file (default: config.ini)")
  parser.add_argument("-f", "--frequency", type=int,default=1, help="Rate at which topics disseminated: default once a second - use integers")
  parser.add_argument("-i", "--iters", type=int, default=1000, help="number of publication iterations (default: 1000)")
  parser.add_argument("-l", "--loglevel", type=int, default=logging.INFO, choices=[logging.DEBUG,logging.INFO,logging.WARNING,logging.ERROR,logging.CRITICAL], help="logging level, choices 10,20,30,40,50: default 20=logging.INFO")
  return parser.parse_args()

def main ():
  try:
    logging.info("Main - acquire a child logger and then log messages in the child")
    logger = logging.getLogger("SubscriberAppln")
    logger.debug("Main: parse command line arguments")
    args = parseCmdLineArgs()
    logger.debug("Main: resetting log level to {}".format (args.loglevel))
    logger.setLevel (args.loglevel)
    logger.debug("Main: effective log level is {}".format (logger.getEffectiveLevel ()))
    logger.debug("Main: obtain the Subscriber appln object")
    sub_app = SubscriberAppln(logger)
    logger.debug("Main: configure the Subscriber appln object")
    sub_app.configure(args)
    logger.debug("Main: invoke the Subscriber appln driver")
    sub_app.driver()
  except Exception as e:
    logger.error("Exception caught in main - {}".format (e))
    return

if __name__ == "__main__":
  logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
  main()