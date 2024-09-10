from confluent_kafka import Consumer

KAFKA_TOPIC = 'jokes'
KAFKA_BOOTSTRAP_SERVERS = ':60347'
joke_count = 0
total_words = 0


def main():
    global joke_count
    global total_words
    conf = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS, 'group.id': 'joke_count'}
    consumer = Consumer(conf)
    try:
        consumer.subscribe([KAFKA_TOPIC])
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error():
                    print(f'Error receiving message: {msg.topic()}')
            else:
                word_count = len(msg.split(' '))
                total_words += word_count
                joke_count += 1
                if joke_count % 5 == 0:
                    print(f'Average words per joke: {total_words / joke_count}')

    except Exception as e:
        print(f'Producer experience uncaught error: {e}')
    finally:
        consumer.close()


if __name__ == '__main__':
    main()