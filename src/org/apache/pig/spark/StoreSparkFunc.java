package org.apache.pig.spark;

import java.io.IOException;

import org.apache.hadoop.mapred.JobConf;
import org.apache.pig.data.Tuple;
import org.apache.spark.rdd.RDD;

public interface StoreSparkFunc {

	public void putRDD(RDD<Tuple> rdd,String path,JobConf conf) throws IOException;
	
}
