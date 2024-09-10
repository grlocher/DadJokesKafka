from confluent_kafka import Producer
import socket
import requests
import time

KAFKA_TOPIC = 'jokes'
KAFKA_BOOTSTRAP_SERVERS = ':60347'
URL = 'https://icanhazdadjoke.com/'


def delivery_callback(err, msg):
    if err:
        print(f'Message failed delivery: {err}')
    else:
        print(f'Message delivered to the {msg.topic()} topic')


def main():
    conf = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS, 'client.id': socket.gethostname()}
    producer = Producer(conf)
    try:
        while True:
            response = requests.get(URL, headers={'Accept': 'application/json'})
            body = response.json()
            joke = body['joke']
            producer.produce(KAFKA_TOPIC, value=joke, callback=delivery_callback)
            producer.flush()
            time.sleep(5)
    except Exception as e:
        print(f'Producer experience uncaught error: {e}')
    finally:
        producer.flush()


if __name__ == '__main__':
    main()