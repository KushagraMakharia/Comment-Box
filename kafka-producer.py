from confluent_kafka import Producer
import time
import socket

conf = {'bootstrap.servers': 'localhost:9092',
        'client.id': socket.gethostname()}

producer = Producer(conf)


def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))

while True:
    msg = input("Enter the message:")
    producer.produce('comm-line', key="text", value=msg, callback=acked)