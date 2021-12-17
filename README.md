# Distributed streaming recommendation system

Report 'DDPS_2.pdf' contains details about the system and the findings of our experimentation

## Executing an experiment
Execute you own experiment on DAS-5 by running the following script:
~~~
$ python3 deployment/das5/run_experiment.py --kafkaNodes <kafka_nodes> --sparkNodes <spark_nodes> --generators <generator_instances> --messages <number_of_messages --time <time_to_generate_messages> --waitTime <time_to_wait_for_experiment_end> --zookeeperInstances 1 --topic <kafka_topic_name> --outputFileName <results_file_name>
~~~


## Executing multiple experiments sequentially
By updating the configuration of the `deployment/das5/experiments_wrapper.py` file, you may create a sequence of experiments that can be executed sequentially with the following command:
~~~
$ python3 deployment/das5/experiments_wrapper.py
~~~