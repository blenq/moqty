#!/usr/bin/env python
import sys

import paho.mqtt.client as mqtt

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, reason, properties):

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.

    if len(sys.argv) > 1:
        topic = sys.argv[1]
    else:
        topic = "$SYS"
    if len(sys.argv) > 2:
        msg = sys.argv[2]
    else:
        msg = "hello"
    client.publish(topic, msg)
    client.disconnect()
    print("Message sent")


client = mqtt.Client(protocol=5)
client.on_connect = on_connect

client.connect("localhost", 1883, 6000)

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
client.loop_forever()
