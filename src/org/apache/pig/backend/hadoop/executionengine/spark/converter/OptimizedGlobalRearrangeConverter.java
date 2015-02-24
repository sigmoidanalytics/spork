package org.apache.pig.backend.hadoop.executionengine.spark.converter;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduce;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POUserFunc;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.pig.backend.hadoop.executionengine.spark.KryoSerializer;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import org.apache.pig.backend.hadoop.executionengine.spark.plans.POOptimizedGlobalRearrange;
import org.apache.pig.backend.hadoop.executionengine.util.MapRedUtil;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.SchemaTupleBackend;
import org.apache.pig.data.SingleTupleBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.spark.rdd.PairRDDFunctions;
import org.apache.spark.rdd.RDD;

import scala.Tuple2;
import scala.collection.Iterator;
//import scala.reflect.ClassManifest;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;

@SuppressWarnings({ "serial" })
public class OptimizedGlobalRearrangeConverter implements
		POConverter<Tuple, Tuple, POOptimizedGlobalRearrange> {
	private static final Log LOG = LogFactory
			.getLog(OptimizedGlobalRearrangeConverter.class);
	private static final TupleFactory tf = TupleFactory.getInstance();
	private static final BagFactory bf = BagFactory.getInstance();

	private byte[] confBytes;

	public OptimizedGlobalRearrangeConverter(byte[] confBytes) {
		this.confBytes = confBytes;
	}

	@Override
	public RDD<Tuple> convert(List<RDD<Tuple>> predecessors,
			POOptimizedGlobalRearrange physicalOperator) throws IOException {
		SparkUtil.assertPredecessorSizeGreaterThan(predecessors,
				physicalOperator, 0);//
		int parallelism = SparkUtil.getParallelism(predecessors,
				physicalOperator);

		String reducers = System.getenv("SPARK_PARALLELISM");
		if (reducers != null) {
			parallelism = Integer.parseInt(reducers);
		}
		LOG.info("Parallelism for Spark groupBy: " + parallelism);

		if (predecessors.size() == 1) {
			// GROUP
			// JavaRDD<Tuple> jrdd = predecessors.get(0).toJavaRDD();
			RDD<Tuple2<Object, Tuple>> map = predecessors.get(0).mapPartitions(
					new TupleToTuple2Partitions(), false,
					SparkUtil.<Object, Tuple> getTuple2Manifest());
			PairRDDFunctions<Object, Tuple> pairRDDFunctions = new PairRDDFunctions<Object, Tuple>(
					map, SparkUtil.getManifest(Object.class),
					SparkUtil.getManifest(Tuple.class), null);
			RDD<Tuple2<Object, Tuple>> mapValues = pairRDDFunctions
					.reduceByKey(new ReduceFunction(physicalOperator
							.getIntermediateForEach()));
//			mapValues.saveAsTextFile("DebugOptimized1-"
//					+ System.currentTimeMillis());
			RDD<Tuple> map2 = mapValues.mapPartitions(new GroupTupleFunction2(
					physicalOperator.getFinalForEach()), false, SparkUtil
					.getManifest(Tuple.class));
//			map2.saveAsTextFile("DebugOptimized2-"
//					+ System.currentTimeMillis());
			return map2;

		} else {
			throw new RuntimeException(
					"Rearrange Optimization is only for 1 predecessor");
		}
	}

	private static class ReduceFunction extends
			AbstractFunction2<Tuple, Tuple, Tuple> implements Serializable {

		POForEach intermediateFunc;

		public ReduceFunction(POForEach intermediateFunc) {
			this.intermediateFunc = intermediateFunc;
		}

		@Override
		public Tuple apply(Tuple t1, Tuple t2) {
			try {
				DataBag bag1 = (DataBag) t1.get(1);
				DataBag bag2 = (DataBag) t2.get(1);
				bag1.addAll(bag2);
				t1.set(1, bag1);
				intermediateFunc.setInputs(null);
				intermediateFunc.attachInput(t1);

				Tuple result = (Tuple) intermediateFunc.getNextTuple().result;
				bag1.clear();
				bag1.add((Tuple) result.get(1));
				t1.set(1, bag1);

				return t1;
			} catch (ExecException e) {
				throw new RuntimeException(e);
			}
		}

	}

	private static class TupleToTuple2Partitions extends
			AbstractFunction1<Iterator<Tuple>, Iterator<Tuple2<Object, Tuple>>>
			implements Serializable {

		@Override
		public Iterator<Tuple2<Object, Tuple>> apply(Iterator<Tuple> input) {

			return input
					.map(new AbstractFunction1<Tuple, Tuple2<Object, Tuple>>() {
						@Override
						public Tuple2<Object, Tuple> apply(Tuple v1) {
							try {
								Object key = v1.get(1);
								Tuple newTuple = tf.newTuple(2);
								DataBag newDefaultBag = bf.newDefaultBag();
								Tuple item = (Tuple) v1.get(2);
								newDefaultBag.add((Tuple) item.get(0));
								newTuple.set(0, key);
								newTuple.set(1, newDefaultBag);
								return new Tuple2<Object, Tuple>(key, newTuple);
							} catch (Exception e) {
								throw new RuntimeException(e);
							}
						}
					});

		}

	}

	private static class GroupTupleFunction2 extends
			AbstractFunction1<Iterator<Tuple2<Object, Tuple>>, Iterator<Tuple>>
			implements Serializable {

		POForEach finalFunc;

		public GroupTupleFunction2(POForEach finalFunc) {
			this.finalFunc = finalFunc;
		}

		public Iterator<Tuple> apply(Iterator<Tuple2<Object, Tuple>> input) {

			return input
					.map(new AbstractFunction1<Tuple2<Object, Tuple>, Tuple>() {

						@Override
						public Tuple apply(Tuple2<Object, Tuple> v1) {
							Tuple t = v1._2();
							
							finalFunc.setInputs(null);
							finalFunc.attachInput(t);

							try {
								return (Tuple) finalFunc.getNextTuple().result;
							} catch (ExecException e) {
								throw new RuntimeException(e);
							}
						}
					});
		}
	}
}
