This Pig branch adds a Spark execution mode (Spork!).

Warning: this is highly experimental.

Spork currently requires you to build Spark separately and publish it to your
local ivy cache. You can do this as follows:

git clone git://github.com/mesos/spark.git
cd spark
sbt/sbt publish-local

Then build Spork using ant jar as usual.

To run pig in Spark mode, you need to set a few env variables:
SPARK_MASTER (local by default)
SPARK_HOME (don't need this for local spark mode)
SPARK_JARS (comma delimited)
MESOS_NATIVE_LIBRARY (don't need this for local spark mode)
SPARK_MAX_CPUS (32 by default)

and start pig in Spark mode: 'pig -x spark myscript.pig'

Caching: we added a new Pig operator, CACHE (as in " foo = foreach bar do_stuff; cache foo; ....) .
In Pig mode, this is a no-op. In Spark mode, this caches data in memory for all the in-memory Spark benefits.
This may or may not work, we rolled it in about 45 minutes and didn't look back..
Testing of this would be appreciated :)

TODO: extensive testing for correctness and perf, esp. in Mesos
TODO: support for more operators, such as RANK, CUBE, etc.

Please let us know if you are playing with this, have patches, etc. Email dev@pig.apache.org 
or find @J_, @billgraham, @squarecog, @jco, or @matei_zaharia on Twitter.
