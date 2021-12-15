import os

if os.environ.get('DEPLOY_ENV') == 'LOCAL':
    import local_env as env
else:
    import das5_env as env


def stop_cluster():
    os.chdir(f'{env.SPARK_HOME}')
    os.system(f'./sbin/stop-all.sh')


if __name__ == "__main__":
    stop_cluster()
