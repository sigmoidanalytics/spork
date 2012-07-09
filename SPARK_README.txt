Spork currently requires you to build Spark separately and publish it to your
local ivy cache. You can do this as follows:

git clone git://github.com/mesos/spark.git
cd spark
sbt/sbt publish-local

Then build Spork using ant jar as usual.
