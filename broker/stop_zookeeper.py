import os
import env

current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)
project_dir = os.path.dirname(parent_dir)


def stop_kafka():
    os.system(f'{env.SUDO} {env.KAFKA_HOME}/bin/zookeeper-server-stop.sh')


if __name__ == "__main__":
    stop_kafka()
