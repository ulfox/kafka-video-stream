from kafka import KafkaConsumer
from time import sleep
import cv2
import numpy as np
from queue import Queue
from threading import Thread
from threading import Event

class kafkaVideoView():
    def __init__(self, bootstrap_servers, topic, client_id, group_id, poll=500, frq=0.01):
        self.topic = topic
        self.client_id = client_id
        self.group_id = group_id
        self.bootstrap_servers = bootstrap_servers
        self.poll = poll
        self.frq = frq

    def setConsumer(self):
        self.consumer = KafkaConsumer(
                self.topic, 
                bootstrap_servers=self.bootstrap_servers.split(','),
                fetch_max_bytes=52428800,
                fetch_max_wait_ms=1000,
                fetch_min_bytes=1,
                max_partition_fetch_bytes=1048576,
                value_deserializer=None,
                key_deserializer=None,
                max_in_flight_requests_per_connection=10,
                client_id=self.client_id,
                group_id=self.group_id,
                auto_offset_reset='earliest',
                max_poll_records=self.poll,
                max_poll_interval_ms=300000,
                heartbeat_interval_ms=3000,
                session_timeout_ms=10000,
                enable_auto_commit=True,
                auto_commit_interval_ms=5000,
                reconnect_backoff_ms=50,
                reconnect_backoff_max_ms=500,
                request_timeout_ms=305000,
                receive_buffer_bytes=32768,
            )

    def playStream(self, queue):
        while True:
            msg = queue.get()
            nparr = np.frombuffer(msg, np.uint8)
            frame = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
            cv2.imshow('frame', frame)
            
            if cv2.waitKey(1) & 0xFF == ord('q'):
                self.keep_consuming = False
                break

            sleep(self.frq)

    def run(self):
        self.setConsumer()
        self.videoQueue = Queue()
        self.keep_consuming = True

        self.playerThread = Thread(target=self.playStream, args=(self.videoQueue, ), daemon=False)
        self.playerThread.start()

        while self.keep_consuming:
            payload = self.consumer.poll(self.poll)
            for bucket in payload:
                for msg in payload[bucket]:
                    self.videoQueue.put(msg.value)

        self.playerThread.join()


if __name__ == "__main__":
    streamVideoPlayer = kafkaVideoView(
        bootstrap_servers='localhost:9092',
        topic='KafkaVideoStream',
        client_id='KafkaVSClient',
        group_id='KafkaVideoStreamConsumer',
        poll=500,
        frq=0.025
    )
    
    streamVideoPlayer.run()