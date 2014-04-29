This Pig branch adds a Spark execution mode (Spork!).

Warning: this is highly experimental.

Spork currently requires you to build Spark separately and publish it to your
local ivy cache. You can do this as follows:

Currently works only With Spark 0.8.0 

Build & run Spark from 

spark.incubator.apache.org/docs/
OR 
http://docs.sigmoidanalytics.com/index.php/Installing_Spark_andSetting_Up_Your_Cluster


Make sure you call 
sbt/sbt assembly 
sbt/sbt publish-local

To run pig in Spark mode, you need to set a few env variables:
SPARK_MASTER (local by default)
SPARK_MAX_CPUS (32 by default)
HADOOP_CONF_DIR


and start pig in Spark mode: 'pig -x spark myscript.pig'

Caching: we added a new Pig operator, CACHE (as in " foo = foreach bar do_stuff; cache foo; ....) .

In Pig mode, this is a no-op. In Spark mode, this caches data in memory for all the in-memory Spark benefits.
This may or may not work, we rolled it in about 45 minutes and didn't look back..
Testing of this would be appreciated :)

Please let us know if you are playing with this, have patches, etc. Email dev@pig.apache.org 
or find @J_, @billgraham, @squarecog, @jco, @mayur_rustagi or @matei_zaharia on Twitter.
