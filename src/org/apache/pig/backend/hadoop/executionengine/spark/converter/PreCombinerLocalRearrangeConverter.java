package org.apache.pig.backend.hadoop.executionengine.spark.converter;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPreCombinerLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import org.apache.pig.data.Tuple;
import org.apache.spark.rdd.RDD;

import scala.collection.Iterator;
import scala.collection.JavaConversions;
import scala.runtime.AbstractFunction1;

@SuppressWarnings({ "serial" })
public class PreCombinerLocalRearrangeConverter implements
		POConverter<Tuple, Tuple, POPreCombinerLocalRearrange> {
	@Override
	public RDD<Tuple> convert(List<RDD<Tuple>> predecessors,
			POPreCombinerLocalRearrange physicalOperator) throws IOException {
		SparkUtil.assertPredecessorSize(predecessors, physicalOperator, 1);
		RDD<Tuple> rdd = predecessors.get(0);
		// call local rearrange to get key and value
		RDD<Tuple> mapPartitions = rdd.mapPartitions(new PreCombinerLocalRearrange(physicalOperator),
				true, SparkUtil.getManifest(Tuple.class));
		//mapPartitions.saveAsTextFile("DebugPreCombiner-" + System.currentTimeMillis());
		return mapPartitions;

	}

	private static class PreCombinerLocalRearrange extends
			AbstractFunction1<Iterator<Tuple>, Iterator<Tuple>> implements
			Serializable {

		private POPreCombinerLocalRearrange physicalOperator;

		private PreCombinerLocalRearrange(POPreCombinerLocalRearrange physicalOperator) {
			this.physicalOperator = physicalOperator;
		}

		public Iterator<Tuple> apply(Iterator<Tuple> i) {
			final java.util.Iterator<Tuple> input = JavaConversions
					.asJavaIterator(i);
			Iterator<Tuple> output = JavaConversions
					.asScalaIterator(new POOutputConsumerIterator(input) {
						protected void attach(Tuple tuple) {
							physicalOperator.setInputs(null);
							physicalOperator.attachInput(tuple);
						}

						protected Result getNextResult() throws ExecException {
							return physicalOperator.getNextTuple();
						}
					});
			return output;
		}
	}

}
