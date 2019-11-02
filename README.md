# Kafka Video Stream


## Setup and start producer

Edit producer.py and change `bootstrap_servers` under __main__, then start producing by issuing:

	python3 producer.py myvideo


## Setup and start consumer

Update bootstrap servers also in consumer.py and issue:

	python3 consumer.py


Video should start playing in your screen. To stop, simply presh button 'q'.

