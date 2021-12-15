import os
import argparse
from datetime import datetime

if os.environ.get('DEPLOY_ENV') == 'LOCAL':
    import local_env as env
else:
    import das5_env as env

current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)


def parsed_arguments():
    parser = argparse.ArgumentParser()
    # parser.add_argument('--experiment', help='Experiment to be executed')
    parser.add_argument('--masterNode', help='Master node host')
    parser.add_argument('--topic', help='Kafka topic')
    parser.add_argument('--outputFileName', help='Output file name')
    parser.add_argument('--brokerServers',
                        help='Broker servers to use for streaming')
    parser.add_argument('--nodesNumber',
                        help='Number of spark nodes')
    
    return parser.parse_args()


def submit_task(args):
    os.chdir(env.SPARK_HOME)
    os.system(f'./bin/spark-submit '
              f'--master spark://{args.masterNode}:{env.SPARK_MASTER_PORT}  '
              f'--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 '
              f'--class {env.DEFAULT_CLASS} '
              f'{parent_dir}/target/scala-2.12/streaming-prediction_2.12-0.1.jar  '
              f'{args.brokerServers} '
              f'{args.topic} '
              f'{env.MODEL_PATH} {args.outputFileName} '	
              f'--num-executors {args.nodesNumber * 3} --executor-cores 5 --executor-memory 12G'
            #   f'{parent_dir}/results/event-latency_{datetime.now().strftime("%Y-%m-%d_%H:%M")}/event-latency'
            #   f' >> {parent_dir}/results/processing-latency-{datetime.now().strftime("%Y-%m-%d_%H:%M:%S")}'
            )


if __name__ == "__main__":
    args=parsed_arguments()
    # if args.experiment not in executable_per_experiment:
        # print(f'No executable found for')
        # exit(1)

    submit_task(args)
