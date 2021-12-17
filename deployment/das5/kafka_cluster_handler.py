import os
import time

current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)
project_dir = os.path.dirname(parent_dir)

KAFKA_PORT = "9082"
PYTHON_PATH = '/var/scratch/ddps2107/Soft/miniconda3/bin/python3'

def deploy_kafka_cluster(zookeeperInstances, kafka_nodes):
    """
    Triggers the execution of the generator on the given node
    """
    node_1 = kafka_nodes[0]
    zookeeper_ports = [f'219{i}' for i in range(zookeeperInstances)]

    os.system(
        f'ssh {node_1}'
        f' {PYTHON_PATH} '
        f' {project_dir}/broker/start_zookeeper.py'
        f' --node {node_1}'
        f' --clientPorts {",".join(zookeeper_ports)} &'
    )

    zookeeper_nodes = [f'{node_1}:{port}' for port in zookeeper_ports]

    time.sleep(20)
    for broker_id, node in enumerate(kafka_nodes):
        os.system(
            f'ssh {node}'
            f' {PYTHON_PATH} '
            f' {project_dir}/broker/start_kafka.py'
            f' --node {node}'
            f' --port {KAFKA_PORT}'
            f' --id {broker_id}'
            f' --zookeeperNodes {",".join(zookeeper_nodes)} &'
        )

    kafka_servers = [f'{node}:{KAFKA_PORT}' for node in kafka_nodes]
    time.sleep(5)
    return kafka_servers

def create_topic(kafka_server, topic, partitions):
    os.system(
        f'ssh {kafka_server.split(":")[0]}'
        f' {PYTHON_PATH} '
        f' {project_dir}/broker/create_topic.py --brokerServer {kafka_server} --topic {topic} --partitions {partitions}'
    )

def stop_kafka_cluster(kafka_nodes):
    for node in kafka_nodes:
        os.system(
            f'ssh {node}'
            f' {PYTHON_PATH} '
            f' {project_dir}/broker/stop_kafka.py'
        )

    os.system(
        f'ssh {kafka_nodes[0]}'
        f' {PYTHON_PATH} '
        f' {project_dir}/broker/stop_zookeeper.py'
    )

