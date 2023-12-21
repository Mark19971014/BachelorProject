import paho.mqtt.client as mqtt
import json
from queue import Queue
from threading import Thread
from time import sleep

MQTT_HOST = 'demo.thingsboard.io'
MQTT_PORT = 1883

GATEWAY_TOPIC = 'v1/gateway/telemetry'
GATEWAY_TOKEN = 'CCEpfBSCCHsPHfkxax5Q'
gateway_client = mqtt.Client('NodeInfoPublisher')
gateway_client.username_pw_set(GATEWAY_TOKEN)
gateway_client.connect(MQTT_HOST, MQTT_PORT)
gateway_client.loop_start()

DEVICE_TOPIC = 'v1/devices/me/telemetry'
DEVICE_TOKEN = 'xGSM58ai8ECYU2tVz1ya'
device_client = mqtt.Client('ObservationPublisher')
device_client.username_pw_set(DEVICE_TOKEN)
device_client.connect(MQTT_HOST, MQTT_PORT)
device_client.loop_start()

msg_queue = Queue()


def push_data_into_queue(data):
    msg_queue.put(data)


def publish_gateway_data(data):
    result = json.dumps(data)
    gateway_client.publish(GATEWAY_TOPIC, result)


def publish_task():
    while True:
        if not msg_queue.empty():
            msg = msg_queue.get_nowait()
            publish_gateway_data(msg)
           
        else:
            sleep(.2)

#daemon thread to publish the message
publish_thread = Thread(target=publish_task)
publish_thread.daemon = True
publish_thread.start()

#connect the device to gateway with the corresponding topic 
def device_connect(device_name, device_type):
    connect_topic = 'v1/gateway/connect'
    gateway_client.publish(connect_topic, payload=json.dumps({"device": device_name, "type": device_type}),
                           qos=1).wait_for_publish()



def publish_PowerAvg(data):
    print("publishing PowerAvg")

    result = json.dumps(data)
    device_client.publish(DEVICE_TOPIC, result)


def publish_DelayAvg(data):
    print("publishing DelayAvg")
    result = json.dumps(data)
    device_client.publish(DEVICE_TOPIC, result)


def publish_Pdr_mean(data):
    print("publishing pdr_mean")

    result = json.dumps(data)
    device_client.publish(DEVICE_TOPIC, result)

