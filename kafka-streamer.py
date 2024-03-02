from confluent_kafka import Consumer, Producer
from transformers import pipeline
import socket


# Consumer
consumer_conf = {'bootstrap.servers': 'localhost:9092',
        'group.id': 'streamer1',
        'auto.offset.reset': 'smallest'}

consumer = Consumer(consumer_conf)
consumer.subscribe(['comm-line'])


# Producer
producer_conf = {'bootstrap.servers': 'localhost:9092',
        'client.id': socket.gethostname()}

producer = Producer(producer_conf)


def predict_sentiment(text):
        print(text)
        pipe = pipeline(model="KoalaAI/Text-Moderation")
        category_dict = {
                "S": "sexual",
                "H": "hate",
                "V": "violence",
                "HR": 'harassment',
                "SH": "self harm",
                "S3": "sexual/minors",
                "H2": "hate/threatening",
                "V2": "violence/graphic",
                "OK": "Ok"
        }
        res = pipe(str(text))
        return category_dict[res[0]['label']]


def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))

while True:
    # Consume messages from the topics
    messages = consumer.consume(num_messages=10, timeout=1.0)

    # Loop over the messages
    for message in messages:
        # Check for errors
        if message.error():
            print(f"Error: {message.error()}")
        else:
            # Print the message value and metadata        
            text = message.value()
            print(f"Message value: {str(text)}")
            sentiment = predict_sentiment(text)            
            if sentiment != 'Ok':
                text = f"Censored due to {sentiment} content"
            producer.produce('res-line', key="text", value=text, callback=acked)
            
            
            
        
                 
                 
