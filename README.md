This Pig branch adds a Spark execution mode (Spork!).


Getting Started
===============

1. Check out the code base.
```
$ git clone https://github.com/sigmoidanalytics/spork.git -b spork-0.9
```

2. Configure the following environment variables in your shell
```
export SPARK_HOME=/path/to/spark
export HADOOP_HOME=/path/to/hadoop
export HADOOP_CONF_DIR=/path/to/hadoop/conf
export BROADCAST_MASTER_IP="SET IT AS THE SPARK_MASTER_IP"      # localhost
export BROADCAST_PORT=6000
export SPARK_MASTER="set spark master here"     # local or spark://localhost:7077
```

3. Build the project using ant
```
$ ant jar-all
```

4. Test basic pig scriopt
```
$ hadoop fs -mkdir /pig-test/input/
$ hadoop fs -put ./tutorial/data/excite-small.log /pig-test/input/

$ ./pig-spark

raw = LOAD '/pig-test/input/excite-small.log' USING PigStorage('\t') AS (user: chararray, time:chararray, query:chararray);
queries = FOREACH raw GENERATE query;
distinct_queries = DISTINCT queries;
STORE distinct_queries INTO '/pig-test/output/';
```

Please feel free to file issues on our github repo (https://github.com/sigmoidanalytics/spork) or mail us at: spark@sigmoidanalytics.com.
