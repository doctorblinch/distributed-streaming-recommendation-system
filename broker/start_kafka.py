import os
import env
import argparse

current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)
project_dir = os.path.dirname(parent_dir)


def parsed_arguments():
    """
    Parses the given user arguments
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--node', nargs='?', help='Node to deploy kafka node')
    parser.add_argument('--port', nargs='?', help='Port to deploy kafka node')
    parser.add_argument('--zookeeperNodes', nargs='?', help='Comma-separated client ports for zookeeper instances')
    parser.add_argument('--id', nargs='?', help='Id of the kafka broker node')
    return parser.parse_args()


def start_kafka(node, port, zookeeper_nodes, broker_id, kafka_nodes=1):
    for i in range(kafka_nodes):
        kafka_properties = f'{env.KAFKA_CONFIG}/server{broker_id}.properties'
        print(kafka_properties)
        kafka_logs = f'/tmp/logs/kafka-logs-{broker_id}'
    
        os.system(f'{env.SUDO} rm -rf {kafka_properties}')
        os.system(f'{env.SUDO} rm -rf {kafka_logs}')
        os.system(f'{env.SUDO} touch {kafka_properties}')
        os.system(f'echo "broker.id={broker_id}" | {env.SUDO} tee -a {kafka_properties}')
        os.system(f'echo "listeners=PLAINTEXT://{node}:{port}" | {env.SUDO} tee -a {kafka_properties}')
        os.system(f'echo "log.dirs={kafka_logs}" | {env.SUDO} tee -a {kafka_properties}')
        
        os.system(f'echo "zookeeper.connect={zookeeper_nodes}" | {env.SUDO} tee -a {kafka_properties}')
        os.system(f'echo "num.network.threads=8" | {env.SUDO} tee -a {kafka_properties}')
        os.system(f'echo "num.io.threads=8" | {env.SUDO} tee -a {kafka_properties}')
        os.system(f'echo "socket.send.buffer.bytes=102400" | {env.SUDO} tee -a {kafka_properties}')
        os.system(f'echo "socket.receive.buffer.bytes=102400" | {env.SUDO} tee -a {kafka_properties}')
        os.system(f'echo "socket.request.max.bytes=104857600" | {env.SUDO} tee -a {kafka_properties}')
        os.system(f'echo "num.partitions=1" | {env.SUDO} tee -a {kafka_properties}')
        os.system(f'echo "num.recovery.threads.per.data.dir=1" | {env.SUDO} tee -a {kafka_properties}')
        os.system(f'echo "offsets.topic.replication.factor=1" | {env.SUDO} tee -a {kafka_properties}')
        os.system(f'echo "transaction.state.log.replication.factor=1" | {env.SUDO} tee -a {kafka_properties}')
        os.system(f'echo "transaction.state.log.min.isr=1" | {env.SUDO} tee -a {kafka_properties}')
        os.system(f'echo "log.retention.hours=48" | {env.SUDO} tee -a {kafka_properties}')
        os.system(f'echo "log.segment.bytes=1073741824" | {env.SUDO} tee -a {kafka_properties}')
        os.system(f'echo "log.retention.check.interval.ms=300000" | {env.SUDO} tee -a {kafka_properties}')
        os.system(f'echo "zookeeper.connection.timeout.ms=18000" | {env.SUDO} tee -a {kafka_properties}')
        os.system(f'echo "group.initial.rebalance.delay.ms=0" | {env.SUDO} tee -a {kafka_properties}')

    for i in range(kafka_nodes):
        os.system(
            f'{env.SUDO} {env.KAFKA_HOME}/bin/kafka-server-start.sh {env.KAFKA_CONFIG}/server{broker_id}.properties &')


if __name__ == "__main__":
    args = parsed_arguments()
    start_kafka(args.node, args.port, args.zookeeperNodes, args.id, 1)
