import os
import env
import argparse

current_dir = os.path.dirname(os.path.realpath(__file__))


def parsed_arguments():
    """
    Parses the given user arguments
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--node', nargs='?', help='Node to deploy zookeeper cluster')
    parser.add_argument('--clientPorts', nargs='?', help='Comma-separated client ports for zookeeper instances')
    return parser.parse_args()


def start_zookeeper(node, client_ports):
    os.system(f'rm -rf {current_dir}/zookeeper')
    os.system(f'mkdir {current_dir}/zookeeper')
    zookeeper_nodes = []
    for i in range(len(client_ports)):
        zookeeper_nodes.append(f'{node}:{client_ports[i]}')
        zk_path = f'{current_dir}/zookeeper/zookeeper{i}'
        zk_properties_path = f'{env.KAFKA_CONFIG}/zookeeper{i}.properties'

        os.system(f'{env.SUDO} mkdir {zk_path}')
        os.system(f'{env.SUDO} touch {zk_path}/myid')
        os.system(f'echo "{i}" | {env.SUDO} tee -a {zk_path}/myid')
        # os.system(f'echo {i} > {zk_path}/myid')
        
        os.system(f'{env.SUDO} rm {zk_properties_path}')
        os.system(
            f'echo "dataDir=/tmp/zookeeper" | {env.SUDO} tee -a {zk_properties_path}')
        os.system(
            f'echo "clientPort={client_ports[i]}" | {env.SUDO} tee -a {zk_properties_path}')
        os.system(
            f'echo "maxClientCnxns=10000" | {env.SUDO} tee -a {zk_properties_path}')
        os.system(
            f'echo "admin.enableServer=false" | {env.SUDO} tee -a {zk_properties_path}')
        for j in range(1, len(client_ports)+1):
            os.system(
                f'echo "server.{j}={node}:{env.ZOOKEEPER_PORT_1_PREFIX}{j}:{env.ZOOKEEPER_PORT_2_PREFIX}{j}" | {env.SUDO} tee -a {zk_properties_path}'
            )

    for i in range(len(client_ports)):
        os.system(
            f'{env.SUDO} {env.KAFKA_HOME}/bin/zookeeper-server-start.sh {env.KAFKA_CONFIG}/zookeeper{i}.properties &'
        )

    return zookeeper_nodes


if __name__ == "__main__":
    args = parsed_arguments()
    start_zookeeper(args.node, args.clientPorts.split(','))
