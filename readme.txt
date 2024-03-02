Context:
Producer Broadcasts the message.
Consumer subscribe to that message.
Streamer consumes the message from consumer, transforms it and produce new topic to be consumed.

How to use:
1. First need to start Kafka server and Zookeper server
2. Install the requirements in a virtual environment.
3. Start kafka-producer.py, kafka-streamer.py, kafka-consumer.py.
4. Enter a message in the kafka-streamer terminal, and see the result in kafka-consumer terminal.

