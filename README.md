# Kafka Video Stream

Visit my blog for additional info and instructions: [Kafka-Video-Streaming](https://blog.primef.org/posts/kafka/2021-04-10/kafka-video-stream/)

## Setup and start producer

Edit producer.py and change `bootstrap_servers` under __main__, then start producing by issuing:

	python3 producer.py myvideo


## Setup and start consumer

Update bootstrap servers also in consumer.py and issue:

	python3 consumer.py


Video should start playing in your screen. To stop, simply presh button 'q'.

