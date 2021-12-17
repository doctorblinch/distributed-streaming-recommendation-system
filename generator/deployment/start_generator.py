import argparse
import env
import os
import sys

current_dir = os.path.dirname(os.path.realpath(__file__))
project_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(project_dir)
from generator.start import start


def parsed_arguments():
    """
    Parses the given user arguments
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--brokerServers', nargs='?' , help='Comma-separated ')
    parser.add_argument('--topic', nargs='?' , help='Kafka Topic', default='user2movie-prediction')
    parser.add_argument('--generators', nargs='?' , help='Number of generators')
    parser.add_argument('--messages', nargs='?' , help='Number of messages to stream')
    parser.add_argument(
        '--time', nargs='?' , help='Time threshold in seconds to stream the messages')
    parser.add_argument(
        '--waitTime', nargs='?' , help='Time in seconds to wait before exiting')
    return parser.parse_args()


def start_generator(args):
    """
    Triggers the execution of the generator given the parameters from arguments
    """
    start(
        brokerServers=args.brokerServers.split(',') or env.DEFAULT_BROKER_SERVERS.split(','),
        topic=args.topic or env.DEFAULT_TOPIC,
        total_messages=int(
            args.messages) if args.messages else env.DEFAULT_NUMBER_OF_MESSAGES,
        available_time_in_secs=int(
            args.time) if args.time else env.DEFAULT_TIME_IN_SECS,
        generators=int(
            args.generators) if args.generators else env.DEFAULT_NUMBER_OF_GENERATORS,
        wait_time_in_secs=int(
            args.waitTime) if args.waitTime else env.DEFAULT_WAIT_TIME_IN_SECS
    )


if __name__ == "__main__":
    start_generator(parsed_arguments())
