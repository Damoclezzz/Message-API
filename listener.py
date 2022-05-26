import re
import requests
import json
from kafka import KafkaConsumer

pattern = r"абракадабра"
consumer = KafkaConsumer('messages')

if __name__ == "__main__":
    while True:
        for data in consumer:
            data = json.loads(data)
            text = data['text'].lower()
            match = re.search(pattern, text)
            if match is None:
                json = {'message_id': data['message_id'], 'success': True}
            else:
                json = {'message_id': data['message_id'], 'success': False}
            headers = {'Authorization': data['token']}
            requests.post("http://127.0.0.1:5000/api/v1/message_confirmation", headers=headers, json=json)
