import os
import env
import argparse

current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)
project_dir = os.path.dirname(parent_dir)

def parsed_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--topic', nargs='?', help='Kafka topic')
    parser.add_argument('--brokerServer', nargs='?', help='Kafka broker server')
    parser.add_argument('--partitions', nargs='?', help='')
    return parser.parse_args()

def create_topic(topic, broker_server, partitions):
    os.system(f'{env.SUDO} {env.KAFKA_HOME}/bin/kafka-topics.sh --create --topic {topic} --bootstrap-server {broker_server} --partitions {partitions} --replication-factor 1')

if __name__ == "__main__":
    args = parsed_arguments()
    create_topic(args.topic, args.brokerServer, args.partitions)
