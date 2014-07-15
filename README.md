This Pig branch adds a Spark execution mode (Spork!).

# Getting Started

## Dependencies

1. Spark version 0.9.0
2. Hadoop 1.0.4
3. Java 7
4. Git client
5. ant

## Building spork

Download the code and build spork using ant:

    $ git clone https://github.com/sigmoidanalytics/spork.git -b spork-0.9
    $ ant jar-all

## Configuring spork

Export below variables into shell or in your bash profile:

    export SPARK_HOME=/path/to/spark
    export HADOOP_HOME=/path/to/hadoop
    export HADOOP_CONF_DIR=/path/to/hadoop/conf
    export BROADCAST_MASTER_IP="SET IT AS THE SPARK_MASTER_IP"      # localhost
    export BROADCAST_PORT=6000
    export SPARK_MASTER="set spark master here"     # local or spark://localhost:7077

## Run sample script

Put data into hdfs:

    $ hadoop fs -mkdir /pig-test/input/
    $ hadoop fs -put ./tutorial/data/excite-small.log /pig-test/input/
    
Start pig and paste the script:    

    $ ./pig-spark
    raw = LOAD '/pig-test/input/excite-small.log' USING PigStorage('\t') AS (user: chararray, time:chararray, query:chararray);
    queries = FOREACH raw GENERATE query;
    distinct_queries = DISTINCT queries;
    STORE distinct_queries INTO '/pig-test/output/';

## TODO

1. Migrate to Spark-1.0
2. Create spark planner instead of using mapreduce planner
3. Get e2e tests to work with Spork and create a benchmark report

Please feel free to file issues on our github repo (https://github.com/sigmoidanalytics/spork) or mail us at: spark@sigmoidanalytics.com.
