package org.apache.pig.backend.hadoop.executionengine.spark.converter;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.PODistinct;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import org.apache.pig.data.Tuple;
import org.apache.spark.rdd.PairRDDFunctions;
import org.apache.spark.rdd.RDD;

import scala.Function1;
import scala.Function2;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.reflect.ClassTag;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;

@SuppressWarnings({ "serial" })
public class DistinctConverter implements POConverter<Tuple, Tuple, PODistinct> {
	private static final Function1<Iterator<Tuple>, Iterator<Tuple2<Tuple, Object>>> TO_KEY_VALUE_FUNCTION = new ToKeyValueFunction();
	private static final Function2<Object, Object, Object> MERGE_VALUES_FUNCTION = new MergeValuesFunction();
	private static final Function1<Iterator<Tuple2<Tuple, Object>>, Iterator<Tuple>> TO_VALUE_FUNCTION = new ToValueFunction();

	@Override
	public RDD<Tuple> convert(List<RDD<Tuple>> predecessors,
			PODistinct poDistinct) throws IOException {
		SparkUtil.assertPredecessorSize(predecessors, poDistinct, 1);
		RDD<Tuple> rdd = predecessors.get(0);

		ClassTag<Tuple2<Tuple, Object>> tuple2ClassManifest = SparkUtil
				.<Tuple, Object> getTuple2Manifest();

		RDD<Tuple2<Tuple, Object>> rddPairs = rdd.mapPartitions(
				TO_KEY_VALUE_FUNCTION, true, tuple2ClassManifest);
		PairRDDFunctions<Tuple, Object> pairRDDFunctions = new PairRDDFunctions<Tuple, Object>(
				rddPairs, SparkUtil.getManifest(Tuple.class),
				SparkUtil.getManifest(Object.class), null);
		int parallelism = SparkUtil.getParallelism(predecessors, poDistinct);
		return pairRDDFunctions.reduceByKey(MERGE_VALUES_FUNCTION, parallelism)
				.mapPartitions(TO_VALUE_FUNCTION, true,
						SparkUtil.getManifest(Tuple.class));
	}

	private static final class ToKeyValueFunction extends
			AbstractFunction1<Iterator<Tuple>, Iterator<Tuple2<Tuple, Object>>>
			implements Serializable {
		@Override
		public Iterator<Tuple2<Tuple, Object>> apply(Iterator<Tuple> input) {

			AbstractFunction1<Tuple, Tuple2<Tuple, Object>> abstractFunction1 = new AbstractFunction1<Tuple, Tuple2<Tuple, Object>>() {

				@Override
				public Tuple2<Tuple, Object> apply(Tuple t) {
					Tuple key = t;
					Object value = null; // value
					// (key, value)
					Tuple2<Tuple, Object> out = new Tuple2<Tuple, Object>(key,
							value);
					return out;
				}

			};

			return input.map(abstractFunction1);
		}
	}

	private static final class MergeValuesFunction extends
			AbstractFunction2<Object, Object, Object> implements Serializable {
		@Override
		public Object apply(Object arg0, Object arg1) {
			return null;
		}
	}

	private static final class ToValueFunction extends
			AbstractFunction1<Iterator<Tuple2<Tuple, Object>>, Iterator<Tuple>>
			implements Serializable {

		@Override
		public Iterator<Tuple> apply(Iterator<Tuple2<Tuple, Object>> input) {

			AbstractFunction1<Tuple2<Tuple, Object>, Tuple> abstractFunction1 = new AbstractFunction1<Tuple2<Tuple, Object>, Tuple>() {

				@Override
				public Tuple apply(Tuple2<Tuple, Object> input) {
					return input._1;
				}
			};

			return input.map(abstractFunction1);
		}

	}
}
