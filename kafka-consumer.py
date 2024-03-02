from confluent_kafka import Consumer

conf = {'bootstrap.servers': 'localhost:9092',
        'group.id': 'consumer1',
        'auto.offset.reset': 'smallest'}

consumer = Consumer(conf)

consumer.subscribe(['res-line'])


while True:
    # Consume messages from the topics
    messages = consumer.consume(num_messages=10, timeout=1.0)

    # Loop over the messages
    for message in messages:
        # Check for errors
        if message.error():
            print(f"Error: {message.error()}")
        else:
            print(f"Message value: {str(message.value())}")
