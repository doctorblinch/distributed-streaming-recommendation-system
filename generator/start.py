import time

from generator.streaming.streamer import start_streaming
from dataclasses import dataclass
from multiprocessing import Process

DEFAULT_BROKER_SERVERS = ['localhost:9092']
RECOMMENDATION_REQUESTS_TOPIC = 'recommendation-requests'
MESSAGES_PER_INTERVAL = 2000

@dataclass
class KafkaTarget():
    servers: list
    topic: str


@dataclass
class StreamingContext():
    messages: int
    interval: float
    messages_per_interval: int


def start_generator_instances(
        generators,
        total_messages,
        available_time_in_secs,
        broker_servers,
        topic
):
    """
    Given a number of generator instances, this function calculates the number of messages
    to be sent by each instance, given the available time in seconds, and initializes one process
    per generator instance in order to stream the data to Kafka.
    The interval is multiplied with 2000, since we will send 2000 messages every time the scheduler
    wakes up the instance.
    """
    messages_per_generator = int(total_messages / generators)
    streaming_context = StreamingContext(
        messages=messages_per_generator,
        interval=MESSAGES_PER_INTERVAL*available_time_in_secs / messages_per_generator,
        messages_per_interval=MESSAGES_PER_INTERVAL
    )
    target = KafkaTarget(servers=broker_servers, topic=topic)

    try:
        if generators == 1:
            start_streaming(streaming_context, target)
        else:
            for _ in range(generators):
                Process(
                    target=start_streaming,
                    args=(streaming_context, target),
                    daemon=False
                ).start()
    except Exception as e:
        print('Error in starting generator instances', e)


def start(
        brokerServers = DEFAULT_BROKER_SERVERS,
        topic=RECOMMENDATION_REQUESTS_TOPIC,
        total_messages=10,
        available_time_in_secs=10,
        generators=1,
        wait_time_in_secs=30
):
    try:
        start_generator_instances(
            generators,
            total_messages,
            available_time_in_secs,
            brokerServers,
            topic
        )
        time.sleep(wait_time_in_secs)
    except Exception as e:
        print('Unknown Exception', e)
