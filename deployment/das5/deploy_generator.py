import os

current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)
project_dir = os.path.dirname(parent_dir)

PYTHON_PATH = '/var/scratch/ddps2107/Soft/miniconda3/bin/python3'

def deploy_generator(args, node):
    """
    Triggers the execution of the generator on the given node
    """
    print(
        f'ssh {node}'
        f' {PYTHON_PATH} '
        f' {project_dir}/generator/deployment/start_generator.py'
        f' --brokerServers {args["brokerServers"]}'
        f' --topic {args["topic"]}'
        f' --generators {args["generators"]}'
        f' --messages {args["messages"]}'
        f' --time {args["time"]} '
        f' --waitTime {args["waitTime"]} &'
        )
        
    os.system(
        f'ssh {node}'
        f' {PYTHON_PATH} '
        f' {project_dir}/generator/deployment/start_generator.py'
        f' --brokerServers {args["brokerServers"]}'
        f' --topic {args["topic"]}'
        f' --generators {args["generators"]}'
        f' --messages {args["messages"]}'
        f' --time {args["time"]} '
        f' --waitTime {args["waitTime"]} &'
    )
    return
