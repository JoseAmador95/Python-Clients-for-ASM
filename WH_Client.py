#!/usr/bin/python
# Program Name: Sensor Mesh for Agriculture Master Node
# Author: Jose Augusto Amador Demeneghi
# Python 2.7
# RTES/IoT
# December 2019

#============================ imports =========================================

from SmartMeshSDK.HartMgrConnector import HartMgrConnector
from SmartMeshSDK.ApiException      import ConnectionError,  \
                                           CommandTimeoutError
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import sys, json, datetime, struct, time, logging, traceback

#============================ defines =========================================

host = 'a7sgauwy0d54b-ats.iot.us-east-1.amazonaws.com'
rootCAPath = 'AmazonRootCA1.pem'
certificatePath = '9a1ae81b8f-certificate.pem.crt'
privateKeyPath = '9a1ae81b8f-private.pem.key'
port = 8883
clientId = 'PyClient'
topic = 'ASM/'


#============================ callbacks =======================================
# Function callback to User Connect event
def callback_userconnect( Tuple):
    '''("Tuple_UserConnect", ['timeStamp', 'eventId', 'channel', 'ipAddr', 'userName'])'''
    payloadJSON = json.dumps({'event':'userconnect',
                              'managertimestamp': Tuple.timeStamp,
                              'clienttimestamp': datetime.datetime.utcnow().replace(microsecond=0).isoformat(),
                              'eventid':Tuple.eventId,
                              'channel':Tuple.channel,
                              'ipaddr':Tuple.ipAddr,
                              'username':Tuple.userName})
    return payloadJSON, 'Connections'

# Function callback to Mote Live event
def callback_motelive( Tuple):
    '''("Tuple_MoteLive", ['timeStamp', 'eventId', 'moteId', 'macAddr', 'reason'])'''
    payloadJSON = json.dumps({'event':'motelive',
                              'managertimestamp': Tuple.timeStamp,
                              'clienttimestamp': datetime.datetime.utcnow().replace(microsecond=0).isoformat(),
                              'eventid':Tuple.eventId,
                              'moteid':Tuple.moteId,
                              'macAddr':Tuple.macAddr,
                              'reason':Tuple.reason})
    return payloadJSON, 'Joins'

# Function callback to incoming data event
def callback_data( Tuple):
    '''("Tuple_data", ['moteId', 'macAddr', 'time', 'payload', 'payloadType', 'isReliable', 'isRequest', 'isBroadcast', 'callbackId', 'counter'])'''
    if len(Tuple.payload) == 4:
        payload = '%.2f'%(struct.unpack('<f',bytearray(Tuple.payload))[0])
    else:
        payload = '%.2f'%(struct.unpack('>H',bytearray(Tuple.payload[-2::]))[0]/(100*1.0))
    payloadJSON = json.dumps({'event':'data',
                              'managertimestamp': Tuple.time,
                              'clienttimestamp': datetime.datetime.utcnow().replace(microsecond=0).isoformat(),
                              'moteid':Tuple.moteId,
                              'macAddr':Tuple.macAddr,
                              'payload': payload,
                              'counter':Tuple.counter})
    return payloadJSON, 'Data'

# Function callback to non implemented events
def callback_notimplemented(Tuple):
    return None, None

# Dictionary that relates notification type with callback function
NotificationParser = {
                      'UserConnect':callback_userconnect,
                      'MoteLive': callback_motelive,
                      'data': callback_data
                     }
#============================ main ============================================

print '\n\n================== Step 1. Connecting to the manager =============='

print '\n=====\nCreating connector'
connector = HartMgrConnector.HartMgrConnector()
print 'done.'

print '\n=====\nConnecting to HART manager'
try:
    # Try to Connect
    connector.connect({})
except ConnectionError as err:
    # If a ConnectionError error is asserted, exit program
    print err
    sys.exit(1)
print 'done.'

print '\n\n================== Step 2. Getting information from the network ===='
# Logger function for AWS library
logger = logging.getLogger("AWSIoTPythonSDK.core")
logger.setLevel(logging.NOTSET)
streamHandler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
streamHandler.setFormatter(formatter)
logger.addHandler(streamHandler)

try:
    # Setup AWS client
    myAWSIoTMQTTClient = AWSIoTMQTTClient(clientId) # With a Client ID
    myAWSIoTMQTTClient.configureEndpoint(host, port) # To the given host and port
    # Using the given certificate files generated from AWS
    myAWSIoTMQTTClient.configureCredentials(rootCAPath, privateKeyPath, certificatePath)
    myAWSIoTMQTTClient.configureConnectDisconnectTimeout(10)  # 10 sec 
    myAWSIoTMQTTClient.configureMQTTOperationTimeout(5)  # 5 sec
    # Try to connect to AWS MQTT broker
    myAWSIoTMQTTClient.connect()
    isConnectedAWS = True
except Exception as err:
    # If an error is asserted, keep going without AWS
    isConnectedAWS = False
    print err

print '\n=====\nRetrieve the network info'
try:
    # Try to get network statistics from manager
    print connector.dn_getNetworkStatistics('lifetime',0)
except (ConnectionError,CommandTimeoutError) as err:
    print "Could not send data, err={0}".format(err)

print '\n\n================== Step 3. Subcribe to Data Event ================='
# Subscribe to events and incoming data
response = connector.dn_subscribe('data events')

print '\n\n================== Step 4. keep Getting Notifications ============='
try:
    lastNot = None
    while True:
        # Get the newest notification
        currentNot = connector.getNotification(1)
        # If the newest notification is different than the last...
        if currentNot and currentNot != lastNot:
            # Parse notification data and get Json string to upload
            messageJson, subTopic = NotificationParser.get(currentNot[0], callback_notimplemented)(currentNot[1])
            print messageJson
            # If a valid Json exists and client is connected to AWS...
            if messageJson and isConnectedAWS:
                # Publish to selected topic (QoS 1, send until ack)
                print 'Publishing JSON to ' + topic + subTopic
                myAWSIoTMQTTClient.publish(topic+subTopic, messageJson, 1)
            lastNot = currentNot
        currentNot = None
except Exception as e:
    # If an error happens print traceback and disconnect
    print e
    traceback.print_exc()

print '\n\n================== Step 5. Disconnecting from the device =========='

print '\n=====\nDisconnecting from HART manager'
try:
    connector.disconnect()
except (ConnectionError,CommandTimeoutError) as err:
    print err
print 'done.'

