Moqty
=====

Demo MQTT server, using Python asyncio. 

The client scripts use a 3rd party package to connect to the server. This is
done both for ease of development as a proof that the server speaks MQTT
properly.
The server itself does not have any dependencies, besides python.

Note: this is tested on Ubuntu Linux 20.04 with python 3.8.

Another Note: This is by no means a full fledged MQTT server. It is just a bit
of fun.

Install demo
============

Create a virtual environment, activate, clone repo and install deps

```shell

$ python -m venv venv
$ . venv/bin/activate
$ python -m pip install -U pip setuptools wheel
$ git clone https://github.com/blenq/moqty.git
$ python -m pip install -r moqty/requirements.txt

```

Run demo
========

Within an activated virtual environment run server

```console
python moqty/run_moqty.py
```

In another shell, with activated virtual environment, run the listener,
which is subscribed to everything. To set up multiple listeners, just
repeat this step.

```console
python moqty/listener.py
```

In yet another shell run the sender with optional arguments "topic" and
"payload"

```console
python moqty/sender.py my_topic Hello
```

The listener(s) will now print the topic and the payload to the screen

```console
$ python moqty/listener.py 
Connected
my_topic Hello
```

Please send more messages using the sender to see the results.
