from datetime import datetime
from os import replace
import os
import numpy as np
import pandas as pd
import sched
import time
from kafka import KafkaProducer
from generator.streaming.request import RecommendationRequest

current_dir = os.path.dirname(os.path.realpath(__file__))

SCHED_PRIORITY = 1
USERS_PATH = os.path.join(current_dir, 'users.npy')
MOVIES_PATH = os.path.join(current_dir, 'movies.npy')


def start_streaming(context, target):
    """
    Given specific streaming interval seconds and messages, this function
    schedules the streaming of messages to the target system with constant speed
    """
    # print(target.servers)
    producer = KafkaProducer(bootstrap_servers=target.servers)
    schedule_constant_streaming(context, producer, target.topic)


def request_generator(size):
    users = np.load(USERS_PATH)
    movies = np.load(MOVIES_PATH)
    np.random.shuffle(users)
    np.random.shuffle(movies)

    user_ids = np.random.choice(users, size=size, replace=True)
    movie_ids = np.random.choice(movies, size=size, replace=True)
    
    for usr, mov in zip(user_ids, movie_ids):
        yield RecommendationRequest(user_id=int(usr), movie_id=int(mov), timestamp=str(datetime.now()))


def schedule_constant_streaming(context, producer, topic):
    """
    Schedules constant streaming every interval seconds
    """
    scheduler = sched.scheduler(time.time, time.sleep)
    
    gen = request_generator(context.messages)

    scheduler.enter(
        context.interval,
        SCHED_PRIORITY,
        stream_in_constant_intervals,
        (scheduler, context, producer, gen, topic)
    )
    scheduler.run()


def stream_in_constant_intervals(scheduler, context, producer, gen, topic):
    """
    Streams data to the connection and reschedules next streaming call after the given interval seconds
    """

    if context.messages == 0:
        return
    else:    
        context.messages -= context.messages_per_interval

    try:
        for i in range(context.messages_per_interval):
            request = next(gen)
            if request.user_id < 100:
                print(f'Sending recommendation request for user {request.user_id} and topic {topic}')
            producer.send(topic, request.to_message().encode())
            
    except Exception as e:
        raise DataStreamingException('Error while publishing message', e)

    scheduler.enter(
        context.interval,
        SCHED_PRIORITY,
        stream_in_constant_intervals,
        (scheduler, context, producer, gen, topic)
    )


class DataStreamingException(Exception):
    pass
