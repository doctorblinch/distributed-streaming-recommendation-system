import argparse
import time
import sys
import os


current_dir = os.path.dirname(os.path.realpath(__file__))
project_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(project_dir)

from deployment.das5.submit_spark_experiment import submit_experiment
from deployment.das5.spark_cluster_handler import is_spark_ready, deploy_cluster, wait_for_ready_cluster, stop_cluster
from deployment.das5.kafka_cluster_handler import deploy_kafka_cluster, stop_kafka_cluster, create_topic
from deployment.das5.nodes_reservation import get_reserved_nodes, cancel_reservation
from deployment.das5.deploy_generator import deploy_generator

GENERATOR_NODE = 1

RESERVATION_ID_INDEX = 0
RESERVATION_STATUS_INDEX = 6
FIRST_RESERVED_NODE_INDEX = 8
RESERVATION_POLLING_TIME = 3
RESERVATION_MAX_WAIT_TIME = 30

CLUSTER_READY_POLLING_TIME = 3
CLUSTER_READY_MAX_WAIT_TIME = 60

EXPERIMENT_MAX_WAIT_TIME = 13 * 60


def parsed_arguments():
    """
    Parses arguments to be used for allocating nods, deploying generator and spark cluster and run the experiment
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--kafkaNodes', help='Kafka nodes to be used')
    parser.add_argument('--sparkNodes', help='Spark nodes to be used')
    parser.add_argument('--zookeeperInstances',
                        help='Zookeeper instances to run on the first of the Kafka node')
    parser.add_argument(
        '--generators', help='Instances of generators to send messages')
    parser.add_argument('--topic', help='Kafka topic')
    parser.add_argument(
        '--messages', help='Number of messages to be streamed by generator')
    parser.add_argument(
        '--time', help='Time slot to stream the total number of messages')
    parser.add_argument(
        '--waitTime', help='Time to wait till experiment is executed')
    parser.add_argument('--outputFileName', help='Output file name')
    return parser.parse_args()


def run_experiment(args):
    """
    Reserves nodes, deploys generator, deploys spark cluster,
    submits spark task for experiment, releases reserved nodes
    """
    if os.path.exists('/var/scratch/ddps2107/checkpoint'):
        os.system('rm -rf /var/scratch/ddps2107/checkpoint')

    kafka_nodes_num = int(args.kafkaNodes)
    spark_nodes_num = int(args.sparkNodes)
    nodes = get_reserved_nodes(
        kafka_nodes_num + spark_nodes_num + GENERATOR_NODE)
    print(f'Nodes {", ".join(nodes)} were reserved')

    generator_node = nodes[0]
    kafka_nodes = nodes[GENERATOR_NODE:GENERATOR_NODE+kafka_nodes_num]
    spark_nodes = nodes[GENERATOR_NODE+kafka_nodes_num:]
    master_spark_node = spark_nodes[0]

    print(f'Generator node: {generator_node}')
    print(f'Kafka nodes: {", ".join(kafka_nodes)}')
    print(
        f'Spark nodes: {", ".join(spark_nodes)} with master node: {master_spark_node}')

    try:
        print("Deploying kafka cluster...")
        kafka_servers = deploy_kafka_cluster(
            int(args.zookeeperInstances), kafka_nodes
            )

        print("Kafka cluster was deployed")

        create_topic(kafka_servers[0], args.topic, len(kafka_nodes))

        print("Deploying spark cluster...")
        if not is_spark_ready(master_spark_node):
            deploy_cluster(args, spark_nodes)

        wait_for_ready_cluster(master_spark_node, args)
        print('Spark cluster is live')
        
        print('Deploying generator...')
        generator_args = {
            "brokerServers": ','.join(kafka_servers),
            "topic": args.topic,
            "generators": args.generators,
            "messages": args.messages,
            "time": args.time,
            "waitTime": args.waitTime
        }

        print(f'Submitting experiment')
        submit_experiment(master_spark_node, spark_nodes_num,','.join(kafka_servers), args.topic, args.outputFileName)

        time.sleep(30)
        deploy_generator(generator_args, generator_node)
        print('Generator was deployed!')

        wait_time = int(args.waitTime) if args.waitTime else EXPERIMENT_MAX_WAIT_TIME
        print(f'Waiting {wait_time} seconds for the experiment to run')
        time.sleep(wait_time)
        print('Waiting time is over')
    except Exception as e:
        print('Experiment was interrupted with exception ', e)
    finally:
        print('Stopping spark cluster')
        stop_cluster(master_spark_node)
        print('Stopping kafka cluster')
        stop_kafka_cluster(kafka_nodes)
        print("Cancelling reservation")
        cancel_reservation()


if __name__ == "__main__":
    args = parsed_arguments()
    run_experiment(args)

