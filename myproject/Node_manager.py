import myproject.mqtt as mqtt
import random
#The Name of the device on ThingsBoard Device Profile
DEVICE_TYPE = 'Node'


class Nodes:
    # node is the node object in db
    def __init__(self, node):
        self.id = node["node_id"]
        self.neighbor = node["neighbors"]


def run(node_obj):
    #source node
    device_name = node_obj.id
    #connect the source node device to ThingsBoard GateWay
    mqtt.device_connect(device_name, DEVICE_TYPE)
    for elem in node_obj.neighbor:
       #the distance node in the neighbor array of the source node
        dst_device = elem['dst']
       # connect the dst node device to ThingsBoard GateWay
        mqtt.device_connect(dst_device, DEVICE_TYPE)
       #the content of message 
        publish_data = {
            dst_device: [
                {
                        "RSSI": elem['rssi'],
                        "ETX": elem['etx'],
                        "src": device_name,
                        "type": DEVICE_TYPE
                }
            ]
        }
        #push the message into Queue
        mqtt.push_data_into_queue(publish_data)
       
