package org.apache.pig.spark;

import java.io.IOException;

import org.apache.hadoop.mapred.JobConf;
import org.apache.pig.data.Tuple;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;

public interface LoadSparkFunc {

	public RDD<Tuple> getRDDfromContext(SparkContext sc,String path,JobConf conf) throws IOException;
	
}
