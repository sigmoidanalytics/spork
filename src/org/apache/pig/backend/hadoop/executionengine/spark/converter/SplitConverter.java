package org.apache.pig.backend.hadoop.executionengine.spark.converter;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSplit;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import org.apache.pig.data.Tuple;
import org.apache.spark.rdd.RDD;
import org.apache.spark.storage.StorageLevel;

public class SplitConverter implements POConverter<Tuple, Tuple, POSplit> {

	private HashMap<POSplit, RDD<Tuple>> cachedRDDs;
	private StorageLevel storageLevel;
	private static Log log = LogFactory.getLog(SplitConverter.class);

	public SplitConverter() {
		cachedRDDs = new HashMap<POSplit, RDD<Tuple>>();

		String storageLevelStr = System.getenv("SPARK_STORAGE_LEVEL");
		log.info("StorageLevel for Spark: " + storageLevel);
		if (storageLevelStr != null) {
			storageLevel = StorageLevel.fromString(storageLevelStr);
		} else {
			storageLevel = StorageLevel.MEMORY_ONLY();
		}
	}

	@Override
	public RDD<Tuple> convert(List<RDD<Tuple>> predecessors, POSplit poSplit)
			throws IOException {
		SparkUtil.assertPredecessorSize(predecessors, poSplit, 1);

		if (!cachedRDDs.containsKey(poSplit)) {
			RDD<Tuple> rdd = predecessors.get(0);
			RDD<Tuple> persist = rdd.persist(storageLevel);
			cachedRDDs.put(poSplit, persist);
		}

		return cachedRDDs.get(poSplit);
	}

	public boolean contains(POSplit op) {
		return cachedRDDs.containsKey(op);
	}

	public RDD<Tuple> get(POSplit op) {
		return cachedRDDs.get(op);
	}

	public void remove(POSplit op) {
		cachedRDDs.remove(op);
	}

}
