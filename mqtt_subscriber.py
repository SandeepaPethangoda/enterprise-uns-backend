import paho.mqtt.client as mqtt
import json
from datetime import datetime
from queue import Queue

class MQTTSubscriber:
    def __init__(self, mqtt_broker: str, queue: Queue):
        self.client = mqtt.Client()
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.connect(mqtt_broker, 1883)
        self.queue = queue

    def on_connect(self, client, userdata, flags, rc):
        client.subscribe("althinect_enterprise")
        print("Connected to MQTT broker")

    def on_message(self, client, userdata, msg):
        self.payload={}
        try:
            
            if(msg.payload!=self.payload):
                self.payload=msg.payload
                payload = json.loads(msg.payload)
                self.queue.put((msg.topic, payload))
                print(f"Received message on topic {msg.topic}")
            else:
                print("Duplicate message received, ignoring.")

                
        except Exception as e:
            print(f"Error parsing message: {e}")

    def run(self):
        self.client.loop_forever()
