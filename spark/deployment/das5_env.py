import pathlib

PATH_TO_SPARK_MODULE = pathlib.Path(__file__).parent.parent.resolve()
SPARK_HOME = '/var/scratch/ddps2107/Soft/spark-3.2.0-bin-hadoop3.2'
DEFAULT_CLASS = 'prediction.StreamingPrediction'
MODEL_PATH = '/var/scratch/ddps2107/als.model'

SPARK_MASTER_PORT = 7077
SPARK_MASTER_WEBUI_PORT = 8088
JAVA_HOME = '/usr'
SPARK_WORKER_CORES = 16
SPARK_WORKER_MEMORY = '16g'
