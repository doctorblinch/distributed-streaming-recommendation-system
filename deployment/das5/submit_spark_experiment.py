import os

current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)
project_dir = os.path.dirname(parent_dir)

PYTHON_PATH = '/var/scratch/ddps2107/Soft/miniconda3/bin/python3'

def submit_experiment(master_spark_node, nodesNumber, kafka_servers, topic, outputFileName):
    """
    Submits the given experiment as a task on the cluster with the given master node
    """
    os.system(
        f'ssh {master_spark_node}'
        f' {PYTHON_PATH} '
        f' {project_dir}/spark/deployment/submit_task.py'
        f' --brokerServers {kafka_servers}'
        f' --topic {topic}'
        f' --outputFileName {outputFileName}'
        f' --nodesNumber {nodesNumber}'
        f' --masterNode {master_spark_node} &'
    )
