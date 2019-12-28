from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import logging
import time
import argparse
import json

AllowedActions = ['both', 'publish', 'subscribe']

# Custom MQTT message callback
def customCallback(client, userdata, message):
    print("Received a new message: ")
    print(message.payload)
    print("from topic: ")
    print(message.topic)
    print("--------------\n\n")


# Read in command-line parameters

host = 'a7sgauwy0d54b-ats.iot.us-east-1.amazonaws.com'
rootCAPath = 'AmazonRootCA1.pem'
certificatePath = '522c284bf7-certificate.pem.crt'
privateKeyPath = '522c284bf7-private.pem.key'
port = 8883
useWebsocket = False
clientId = 'Pythonista'
topic = 'ASM/Data'
mode = 'subscribe'

if mode not in AllowedActions:
    parser.error("Unknown --mode option %s. Must be one of %s" % (mode, str(AllowedActions)))
    exit(2)

if useWebsocket and certificatePath and privateKeyPath:
    parser.error("X.509 cert authentication and WebSocket are mutual exclusive. Please pick one.")
    exit(2)

if not useWebsocket and (not certificatePath or not privateKeyPath):
    parser.error("Missing credentials for authentication.")
    exit(2)

# Port defaults
if useWebsocket and not port:  # When no port override for WebSocket, default to 443
    port = 443
if not useWebsocket and not port:  # When no port override for non-WebSocket, default to 8883
    port = 8883

# Configure logging
logger = logging.getLogger("AWSIoTPythonSDK.core")
logger.disabled = True
logger.setLevel(logging.NOTSET)
streamHandler = logging.StreamHandler()
#formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
formatter = logging.Formatter('')
streamHandler.setFormatter(formatter)
logger.addHandler(streamHandler)


# Init AWSIoTMQTTClient
myAWSIoTMQTTClient = None
if useWebsocket:
    myAWSIoTMQTTClient = AWSIoTMQTTClient(clientId, useWebsocket=True)
    myAWSIoTMQTTClient.configureEndpoint(host, port)
    myAWSIoTMQTTClient.configureCredentials(rootCAPath)
else:
    myAWSIoTMQTTClient = AWSIoTMQTTClient(clientId)
    myAWSIoTMQTTClient.configureEndpoint(host, port)
    myAWSIoTMQTTClient.configureCredentials(rootCAPath, privateKeyPath, certificatePath)

# AWSIoTMQTTClient connection configuration
myAWSIoTMQTTClient.configureAutoReconnectBackoffTime(1, 32, 20)
myAWSIoTMQTTClient.configureOfflinePublishQueueing(-1)  # Infinite offline Publish queueing
myAWSIoTMQTTClient.configureDrainingFrequency(2)  # Draining: 2 Hz
myAWSIoTMQTTClient.configureConnectDisconnectTimeout(10)  # 10 sec
myAWSIoTMQTTClient.configureMQTTOperationTimeout(5)  # 5 sec
# Connect and subscribe to AWS IoT
myAWSIoTMQTTClient.connect()
if mode == 'both' or mode == 'subscribe':
    myAWSIoTMQTTClient.subscribe(topic, 1, customCallback)
time.sleep(2)

# Publish to the same topic in a loop forever
loopCount = 0
while True:
    if mode == 'both' or mode == 'publish':
        message = {}
        message['message'] = args.message
        message['sequence'] = loopCount
        messageJson = json.dumps(message)
        #myAWSIoTMQTTClient.publish(btopic, messageJson, 1)
        if args.mode == 'publish':
            print('Published topic %s: %s\n' % (topic, messageJson))
        loopCount += 1
    time.sleep(1)
