import subprocess


def is_cluster_live():
    jps = subprocess.check_output('jps', shell=True)
    print('Spark cluster is live') if 'Master' in str(jps) else print('There is no active spark cluster')
    return True if 'Master' in str(jps) else False


if __name__ == "__main__":
    is_cluster_live()
