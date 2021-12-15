import pathlib

PATH_TO_SPARK_MODULE = pathlib.Path(__file__).parent.parent.resolve()
SPARK_HOME = '/usr/local/spark'
DEFAULT_CLASS = 'prediction.StreamingPrediction'
MODEL_PATH = '/tmp/als.model'

SPARK_MASTER_HOST = 'localhost'
SPARK_MASTER_PORT = 7077
SPARK_MASTER_WEBUI_PORT = 8088
JAVA_HOME = '/Users/pzakkas/.sdkman/candidates/java/11.0.11-zulu'
SPARK_WORKER_CORES = 2
SPARK_WORKER_MEMORY = '3g'
