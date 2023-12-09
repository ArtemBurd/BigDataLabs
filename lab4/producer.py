from kafka import KafkaProducer
import random
import time
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

try:
    for i in range(100):
        time_processing = abs(random.gauss(mu=10., sigma=2.0))
        user_id = random.randint(1000, 9999)
        message = {'user': f'user{user_id}',
                   'time_of_user_processing': f'{time_processing}'}
        producer.send('IT', value=message)
        print(f"Produced: {message}")
        time.sleep(1)
except KeyboardInterrupt:
    print("Producer has stopped.")

producer.close()