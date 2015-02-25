package org.apache.pig.backend.hadoop.executionengine.spark.converter;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduce;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POUserFunc;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.pig.backend.hadoop.executionengine.spark.KryoSerializer;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import org.apache.pig.backend.hadoop.executionengine.spark.plans.POOptimizedGlobalRearrange;
import org.apache.pig.backend.hadoop.executionengine.util.MapRedUtil;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.SchemaTupleBackend;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.spark.rdd.PairRDDFunctions;
import org.apache.spark.rdd.RDD;

import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.mutable.MutableList;
//import scala.reflect.ClassManifest;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;

@SuppressWarnings({ "serial" })
public class OptimizedGlobalRearrangeConverter2 implements
		POConverter<Tuple, Tuple, POOptimizedGlobalRearrange> {

	private static final Log LOG = LogFactory
			.getLog(OptimizedGlobalRearrangeConverter2.class);
	private static final TupleFactory tf = TupleFactory.getInstance();
	private static final BagFactory bf = BagFactory.getInstance();

	private byte[] confBytes;

	public OptimizedGlobalRearrangeConverter2(byte[] confBytes) {
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
			RDD<Tuple2<Object, Tuple>> mapPartitions = predecessors.get(0)
					.mapPartitions(
							new CombinerFunction(
									physicalOperator.getIntermediateForEach(),
									confBytes), false,
							SparkUtil.<Object, Tuple> getTuple2Manifest());
			PairRDDFunctions<Object, Tuple> pairRDDFunctions = new PairRDDFunctions<Object, Tuple>(
					mapPartitions, SparkUtil.getManifest(Object.class),
					SparkUtil.getManifest(Tuple.class), null);
			RDD<Tuple2<Object, Iterable<Tuple>>> combineByKey = pairRDDFunctions
					.combineByKey(new Tuple2ToIterable(),
							new AddTuple2ToIterable(),
							new AddIterableToIterable(), parallelism);

			RDD<Tuple> mapPartitions2 = combineByKey.mapPartitions(
					new ReduceFunction(physicalOperator.getFinalForEach(),
							confBytes), false, SparkUtil
							.getManifest(Tuple.class));
			// JavaRDD<Tuple> jrdd = predecessors.get(0).toJavaRDD();
			// RDD<Tuple2<Object, Tuple>> map =
			// predecessors.get(0).mapPartitions(
			// new TupleToTuple2Partitions(), false,
			// SparkUtil.<Object, Tuple> getTuple2Manifest());
			// PairRDDFunctions<Object, Tuple> pairRDDFunctions = new
			// PairRDDFunctions<Object, Tuple>(
			// map, SparkUtil.getManifest(Object.class),
			// SparkUtil.getManifest(Tuple.class), null);
			// RDD<Tuple2<Object, Tuple>> mapValues = pairRDDFunctions
			// .reduceByKey(new ReduceFunction(physicalOperator
			// .getIntermediateForEach(), confBytes));
			// RDD<Tuple> map2 = mapValues.mapPartitions(new
			// GroupTupleFunction2(
			// physicalOperator.getFinalForEach(), confBytes), false,
			// SparkUtil.getManifest(Tuple.class));

			return mapPartitions2;

		} else {
			throw new RuntimeException(
					"Rearrange Optimization is only for 1 predecessor");
		}
	}

	private static class CombinerFunction extends
			AbstractFunction1<Iterator<Tuple>, Iterator<Tuple2<Object, Tuple>>>
			implements Serializable {

		POForEach intermediateFunc;
		private byte[] confBytes;
		private transient JobConf jobConf;

		public CombinerFunction(POForEach intermediateFunc, byte[] confBytes) {
			this.intermediateFunc = intermediateFunc;
			this.confBytes = confBytes;
		}

		void initializeJobConf() {
			if (this.jobConf == null) {
				this.jobConf = KryoSerializer
						.deserializeJobConf(this.confBytes);
				PigMapReduce.sJobConfInternal.set(jobConf);
				try {
					MapRedUtil.setupUDFContext(jobConf);
					PigContext pc = (PigContext) ObjectSerializer
							.deserialize(jobConf.get("pig.pigContext"));
					SchemaTupleBackend.initialize(jobConf, pc);

					PhysicalOperator[] planLeafOps = intermediateFunc
							.getPlanLeafOps();
					if (planLeafOps != null) {
						for (PhysicalOperator op : planLeafOps) {
							if (op.getClass() == POUserFunc.class) {
								POUserFunc udf = (POUserFunc) op;
								udf.setFuncInputSchema();
							}
						}
					}

				} catch (IOException ioe) {
					String msg = "Problem while configuring UDFContext from ForEachConverter.";
					throw new RuntimeException(msg, ioe);
				}
			}
		}

		@Override
		public Iterator<Tuple2<Object, Tuple>> apply(Iterator<Tuple> iterator) {
			initializeJobConf();
			MutableList<Tuple2<Object, Tuple>> results = new MutableList<Tuple2<Object, Tuple>>();
			try {
				HashMap<Object, ArrayList<Tuple>> hashMap = new HashMap<Object, ArrayList<Tuple>>();
				while (iterator.hasNext()) {
					Tuple v1 = iterator.next();
					Object key = v1.get(1);
					ArrayList<Tuple> arrayList = hashMap.get(key);
					if (arrayList == null) {
						arrayList = new ArrayList<Tuple>();
						hashMap.put(key, arrayList);
					}
					arrayList.add((Tuple) v1.get(2));
				}

				Set<Object> keySet = hashMap.keySet();
				for (Object key : keySet) {
					ArrayList<Tuple> arrayList = hashMap.get(key);
					Tuple tuple = arrayList.get(0);
					int size = tuple.size() + 1;
					Tuple newTuple = tf.newTuple(size);
					newTuple.set(0, key);
					for (int i = 1; i < size; i++) {
						DataBag newDefaultBag = bf.newDefaultBag();
						newTuple.set(i, newDefaultBag);
						for (Tuple t : arrayList) {
							newDefaultBag.add((Tuple) t.get(i - 1));
						}
					}
					intermediateFunc.setInputs(null);
					intermediateFunc.attachInput(newTuple);

					Tuple result = (Tuple) intermediateFunc.getNextTuple().result;
					results.$plus$eq(new Tuple2<Object, Tuple>(key, result));
				}
				return results.iterator();
			} catch (ExecException e) {
				throw new RuntimeException(e);
			}
		}

	}

	private static class Tuple2ToIterable extends
			AbstractFunction1<Tuple, Iterable<Tuple>> implements Serializable {

		@Override
		public Iterable<Tuple> apply(Tuple t) {
			ArrayList<Tuple> arrayList = new ArrayList<Tuple>();
			arrayList.add(t);
			return arrayList;

		}

	}

	private static class AddTuple2ToIterable extends
			AbstractFunction2<Iterable<Tuple>, Tuple, Iterable<Tuple>>
			implements Serializable {

		@Override
		public Iterable<Tuple> apply(Iterable<Tuple> it, Tuple t) {
			((ArrayList<Tuple>) it).add(t);
			return it;
		}

	}

	private static class AddIterableToIterable
			extends
			AbstractFunction2<Iterable<Tuple>, Iterable<Tuple>, Iterable<Tuple>>
			implements Serializable {

		@Override
		public Iterable<Tuple> apply(Iterable<Tuple> it, Iterable<Tuple> it2) {
			((ArrayList<Tuple>) it).addAll((ArrayList<Tuple>) it2);
			return it;
		}

	}

	private static class ReduceFunction extends
			AbstractFunction1<Iterator<Tuple2<Object, Iterable<Tuple>>>, Iterator<Tuple>>
			implements Serializable {

		POForEach finalFunc;
		private byte[] confBytes;
		private transient JobConf jobConf;

		public ReduceFunction(POForEach finalFunc, byte[] confBytes) {
			this.finalFunc = finalFunc;
			this.confBytes = confBytes;
		}

		void initializeJobConf() {
			if (this.jobConf == null) {
				this.jobConf = KryoSerializer
						.deserializeJobConf(this.confBytes);
				PigMapReduce.sJobConfInternal.set(jobConf);
				try {
					MapRedUtil.setupUDFContext(jobConf);
					PigContext pc = (PigContext) ObjectSerializer
							.deserialize(jobConf.get("pig.pigContext"));
					SchemaTupleBackend.initialize(jobConf, pc);

					PhysicalOperator[] planLeafOps = finalFunc.getPlanLeafOps();
					if (planLeafOps != null) {
						for (PhysicalOperator op : planLeafOps) {
							if (op.getClass() == POUserFunc.class) {
								POUserFunc udf = (POUserFunc) op;
								udf.setFuncInputSchema();
							}
						}
					}

				} catch (IOException ioe) {
					String msg = "Problem while configuring UDFContext from ForEachConverter.";
					throw new RuntimeException(msg, ioe);
				}
			}
		}

		@Override
		public Iterator<Tuple> apply(Iterator<Tuple2<Object, Iterable<Tuple>>> iterator) {
			initializeJobConf();
			return iterator
					.map(new AbstractFunction1<Tuple2<Object, Iterable<Tuple>>, Tuple>() {

						@Override
						public Tuple apply(Tuple2<Object, Iterable<Tuple>> pair) {
							try {
								ArrayList<Tuple> tuplesForKey = (ArrayList<Tuple>)pair._2;
								Tuple tuple = tuplesForKey.get(0);
								int size = tuple.size();
								Tuple newTuple = tf.newTuple(size);
								newTuple.set(0, pair._1);
								for (int i = 1; i < size; i++) {
									DataBag newDefaultBag = bf.newDefaultBag();
									newTuple.set(i, newDefaultBag);
									for (Tuple t : tuplesForKey) {
										newDefaultBag.add((Tuple) t.get(i));
									}
								}
								finalFunc.setInputs(null);
								finalFunc.attachInput(newTuple);

								Tuple result = (Tuple) finalFunc.getNextTuple().result;
								return result;
							} catch (ExecException e) {
								throw new RuntimeException(e);
							}
						}
					});

		}

	}
}
