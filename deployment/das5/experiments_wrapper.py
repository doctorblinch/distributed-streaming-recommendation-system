import os
import time
import subprocess

RESULT_DIR = '/var/scratch/ddps2107/final_experiments'
param_grid = [ 
    (1, 1, 5_000_000),
    (1, 1, 10_000_000),
    (1, 2, 5_000_000),
    (1, 2, 10_000_000),
    (1, 2, 20_000_000),
    (2, 1, 2_000_000),
    (2, 1, 5_000_000),
    (2, 1, 10_000_000),
    (2, 2, 5_000_000),
    (2, 2, 10_000_000),
    (2, 2, 20_000_000),
    (2, 3, 5_000_000),
    (2, 3, 10_000_000),
    (2, 3, 20_000_000),
    (2, 3, 50_000_000),
    (3, 2, 5_000_000),
    (3, 2, 10_000_000),
    (3, 2, 20_000_000),
    (3, 2, 50_000_000),
    (3, 3, 5_000_000),
    (3, 3, 10_000_000),
    (3, 3, 20_000_000),
    (3, 3, 50_000_000),
]

def name_from(param):
    return f'exp-k{param[0]}-s{param[1]}' + \
           f'-g32-m{param[2]}-t600'


if not os.path.exists(RESULT_DIR):
    os.mkdir(RESULT_DIR)


for param in param_grid:
    for iteration in range(3):
        i = 0
        file_name = name_from(param)
        file_name_tmp = file_name + f'-v{str(i)}'
        while os.path.exists(os.path.join(RESULT_DIR, file_name_tmp)):
            i += 1
            file_name_tmp = file_name + f'-v{str(i)}'

        try:
            subprocess.run([
                'python3',
                'deployment/das5/run_experiment.py',
                '--kafkaNodes', f'{param[0]}',
                '--sparkNodes', f'{param[1]}',
                '--generators', '32',
                '--messages', f'{param[2]}',
                '--time', '600',
                '--waitTime', '720',
                '--topic', file_name_tmp,
                '--zookeeperInstances', '1',
                '--outputFileName', f'final_experiments/{file_name_tmp}',
            ])

        except Exception as e:
            with open('failed-final-experiments.txt', 'a') as file:
                file.write(f'Experiment {file_name_tmp} failed with Exception: {e}')

            continue

        time.sleep(10)

