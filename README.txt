Apache Pig
===========
Pig is a dataflow programming environment for processing very large files. Pig's
language is called Pig Latin. A Pig Latin program consists of a directed
acyclic graph where each node represents an operation that transforms data.
Operations are of two flavors: (1) relational-algebra style operations such as
join, filter, project; (2) functional-programming style operators such as map,
reduce. 

Pig compiles these dataflow programs into (sequences of) map-reduce jobs and
executes them using Hadoop. It is also possible to execute Pig Latin programs
in a "local" mode (without Hadoop cluster), in which case all processing takes
place in a single local JVM. 

General Info
===============

For the latest information about Pig, please visit our website at:

   http://pig.apache.org/

and our wiki, at:

   http://wiki.apache.org/pig/

Getting Started
===============
1. To learn about Pig, try http://wiki.apache.org/pig/PigTutorial
2. To build and run Pig, try http://wiki.apache.org/pig/BuildPig and
http://wiki.apache.org/pig/RunPig
3. To check out the function library, try http://wiki.apache.org/pig/PiggyBank

Development Guide
=================
1. Building Pig using ant
   $ ant jar-all
2. Environments variables required
   export SPARK_HOME=/path/to/spark
   export HADOOP_HOME=/path/to/hadoop
   export MASTER=local[2] # for local mode or 
   export MASTER=spark://ip:7077 # for cluster mode
3. Using pig-spark
   $ ./pig-spark
   
Contributing to the Project
===========================

We welcome all contributions. For the details, please, visit
http://wiki.apache.org/pig/HowToContribute.

