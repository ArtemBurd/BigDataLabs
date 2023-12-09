from kafka import KafkaConsumer
import json
import numpy as np

consumer = KafkaConsumer(
    'IT',
    bootstrap_servers='localhost:9092',
    group_id='my_group',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

user_time = np.array([])

try:
    for message in consumer:
        print(f"Consumed: {message.value}")
        user_time = np.append(user_time, float(message.value["time_of_user_processing"]))
        print(f"Mean time to proccess users is {user_time.mean():.4f} seconds\n")
except:
    print("Consumer has stopped.")

consumer.close()