package org.apache.pig.backend.hadoop.executionengine.spark.converter;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.parse.HiveParser_IdentifiersParser.booleanValue_return;
import org.apache.hadoop.mapred.JobConf;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduce;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POUserFunc;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.pig.backend.hadoop.executionengine.spark.KryoSerializer;
import org.apache.pig.backend.hadoop.executionengine.spark.MemoryWarningSystem;
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
import org.apache.spark.HashPartitioner;
import org.apache.spark.rdd.OrderedRDDFunctions;
import org.apache.spark.rdd.PairRDDFunctions;
import org.apache.spark.rdd.RDD;
import org.apache.spark.rdd.ShuffledRDD;

import scala.Function1;
import scala.Some;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConversions;
import scala.math.Ordering;
import scala.runtime.AbstractFunction0;
//import scala.reflect.ClassManifest;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;

@SuppressWarnings({ "serial" })
public class OptimizedGlobalRearrangeConverter3 implements
		POConverter<Tuple, Tuple, POOptimizedGlobalRearrange> {

	private static final Log LOG = LogFactory
			.getLog(OptimizedGlobalRearrangeConverter3.class);
	private static final TupleFactory tf = TupleFactory.getInstance();
	private static final BagFactory bf = BagFactory.getInstance();

	private byte[] confBytes;

	public OptimizedGlobalRearrangeConverter3(byte[] confBytes) {
		this.confBytes = confBytes;
	}

	@Override
	public RDD<Tuple> convert(List<RDD<Tuple>> predecessors,
			POOptimizedGlobalRearrange physicalOperator) throws IOException {
		SparkUtil.assertPredecessorSizeGreaterThan(predecessors,
				physicalOperator, 0);//
		int parallelism = SparkUtil.getParallelism(predecessors,
				physicalOperator);

		String parallelStr = System.getenv("SPARK_PARALLELISM");
		if (parallelStr != null) {
			parallelism = Integer.parseInt(parallelStr);
		}
		LOG.info("Parallelism for Spark groupBy: " + parallelism);

		if (predecessors.size() == 1) {
			// GROUP
			// predecessors.get(0).saveAsTextFile(
			// "DebugOptimizedGlobalRearrangeConverter0-"
			// + System.currentTimeMillis());
			RDD<Tuple> rdd = predecessors.get(0);
			RDD<Tuple2<Object, Tuple>> pairRdd = rdd.mapPartitions(
					new TupleToTuple2(), false,
					SparkUtil.<Object, Tuple> getTuple2Manifest());
			// pairRdd.saveAsTextFile("DebugOptimizedGlobalRearrangeConverter0-"
			// + System.currentTimeMillis());
			RDD<Tuple2<Object, Tuple>> repartitionAndSortWithinPartitions = new OrderedRDDFunctions<Object, Tuple, Tuple2<Object, Tuple>>(
					pairRdd, new NaturalOrdering(),
					SparkUtil.getManifest(Object.class),
					SparkUtil.getManifest(Tuple.class),
					SparkUtil.<Object, Tuple> getTuple2Manifest())
					.repartitionAndSortWithinPartitions(new HashPartitioner(
							parallelism));
			// repartitionAndSortWithinPartitions
			// .saveAsTextFile("DebugOptimizedGlobalRearrangeConverter1-"
			// + System.currentTimeMillis());
			RDD<Tuple2<Object, Tuple>> mapPartitions = repartitionAndSortWithinPartitions
					.mapPartitions(
							new CombinerFunction(physicalOperator
									.getIntermediateForEach(), confBytes),
							false, SparkUtil
									.<Object, Tuple> getTuple2Manifest());
			// mapPartitions
			// .saveAsTextFile("DebugOptimizedGlobalRearrangeConverter2-"
			// + System.currentTimeMillis());

			PairRDDFunctions<Object, Tuple> pairRDDFunctions = new PairRDDFunctions<Object, Tuple>(
					mapPartitions, SparkUtil.getManifest(Object.class),
					SparkUtil.getManifest(Tuple.class), null);

			RDD<Tuple2<Object, Tuple>> combineByKey = pairRDDFunctions
					.combineByKey(new Tuple2ToIterable(),
							new AddTuple2ToIterable(),
							new AddIterableToIterable(), parallelism);
			// combineByKey
			// .saveAsTextFile("DebugOptimizedGlobalRearrangeConverter3-"
			// + System.currentTimeMillis());
			RDD<Tuple> mapPartitions2 = combineByKey.mapPartitions(
					new ReduceFunction(physicalOperator.getFinalForEach(),
							confBytes), false, SparkUtil
							.getManifest(Tuple.class));

			return mapPartitions2;

		} else {
			throw new RuntimeException(
					"Rearrange Optimization is only for 1 predecessor");
		}
	}

	private static class CombinerFunction
			extends
			AbstractFunction1<Iterator<Tuple2<Object, Tuple>>, Iterator<Tuple2<Object, Tuple>>>
			implements Serializable
	// , Spillable //FOR FUTURE THIS MAY NEED TO BE IMPLEMENTED
	{

		private static final int SPILL_REGISTER_THRESHOLD = 100 * 1024;
		private static final int ELEMENT_THRESHOLD = 100 * 1000;

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
		public Iterator<Tuple2<Object, Tuple>> apply(
				Iterator<Tuple2<Object, Tuple>> iterator) {
			initializeJobConf();

			DataBag results = bf.newDefaultBag();
			try {
				Tuple collectTuple = null;
				Object lastKey = null;
				while (iterator.hasNext()) {
					Tuple v1 = iterator.next()._2();
					Object key = v1.get(1);

					if (!key.equals(lastKey)) {
						combineCollectedTuples(results, collectTuple);
						collectTuple = null;
						lastKey = key;
					}

					if (collectTuple == null) {
						Tuple t = (Tuple) v1.get(2);
						int size = t.size() + 1;
						Tuple newTuple = tf.newTuple(size);
						newTuple.set(0, key);
						for (int i = 1; i < size; i++) {
							DataBag newDefaultBag = bf.newDefaultBag();
							newTuple.set(i, newDefaultBag);
							newDefaultBag.add((Tuple) t.get(i - 1));
						}
						collectTuple = newTuple;
					} else {
						Tuple t = (Tuple) v1.get(2);
						for (int i = 1; i < collectTuple.size(); i++) {
							DataBag dataBag = (DataBag) collectTuple.get(i);
							dataBag.add((Tuple) t.get(i - 1));
						}
					}
				}

				combineCollectedTuples(results, collectTuple);

				return JavaConversions.asScalaIterator(results.iterator()).map(
						new AbstractFunction1<Tuple, Tuple2<Object, Tuple>>() {
							@Override
							public Tuple2<Object, Tuple> apply(Tuple t) {
								try {
									return new Tuple2<Object, Tuple>(t.get(0),
											t);
								} catch (ExecException e) {
									LOG.info(e);
									e.printStackTrace();
									throw new RuntimeException(e);
								}
							}
						});
			} catch (ExecException e) {
				LOG.info(e);
				e.printStackTrace();
				throw new RuntimeException(e);
			}
		}

		/**
		 * @param results
		 * @param hashMap
		 * @throws ExecException
		 */
		private void combineCollectedTuples(DataBag results,
				Tuple collectedtuple) throws ExecException {
			if (collectedtuple != null) {
				intermediateFunc.setInputs(null);
				intermediateFunc.attachInput(collectedtuple);

				Tuple result = (Tuple) intermediateFunc.getNextTuple().result;
				results.add(result);
			}
		}
	}

	private static class TupleToTuple2 extends
			AbstractFunction1<Iterator<Tuple>, Iterator<Tuple2<Object, Tuple>>>
			implements Serializable {

		@Override
		public Iterator<Tuple2<Object, Tuple>> apply(Iterator<Tuple> it) {
			return it
					.map(new AbstractFunction1<Tuple, Tuple2<Object, Tuple>>() {

						@Override
						public Tuple2<Object, Tuple> apply(Tuple t) {
							try {
								Object key = t.get(1);
								return new Tuple2<Object, Tuple>(key, t);
							} catch (ExecException e) {
								throw new RuntimeException(e);
							}
						}
					});
		}
	}

	private static class Tuple2ToIterable extends
			AbstractFunction1<Tuple, Tuple> implements Serializable {
		@Override
		public Tuple apply(Tuple t) {
			try {
				int size = t.size();
				for (int i = 1; i < size; i++) {
					Tuple index = (Tuple) t.get(i);
					DataBag b = bf.newDefaultBag();
					b.add(index);
					t.set(i, b);
				}
				return t;
			} catch (ExecException e) {
				LOG.info(e);
				e.printStackTrace();
				throw new RuntimeException(e);
			}
		}
	}

	private static class AddTuple2ToIterable extends
			AbstractFunction2<Tuple, Tuple, Tuple> implements Serializable {
		@Override
		public Tuple apply(Tuple collectTuple, Tuple t) {
			try {
				int size = t.size();
				for (int i = 1; i < size; i++) {
					Tuple index = (Tuple) t.get(i);
					DataBag b = (DataBag) collectTuple.get(i);
					b.add(index);
				}
				return collectTuple;
			} catch (ExecException e) {
				LOG.info(e);
				e.printStackTrace();
				throw new RuntimeException(e);
			}
		}

	}

	private static class AddIterableToIterable extends
			AbstractFunction2<Tuple, Tuple, Tuple> implements Serializable {
		@Override
		public Tuple apply(Tuple it, Tuple it2) {
			try {
				int size = it.size();
				for (int i = 1; i < size; i++) {
					DataBag b2 = (DataBag) it2.get(i);
					DataBag b = (DataBag) it.get(i);
					b.addAll(b2);
				}
				return it;
			} catch (ExecException e) {
				LOG.info(e);
				e.printStackTrace();
				throw new RuntimeException(e);
			}
		}
	}

	private static class ReduceFunction extends
			AbstractFunction1<Iterator<Tuple2<Object, Tuple>>, Iterator<Tuple>>
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
					LOG.info(ioe);
					ioe.printStackTrace();
					throw new RuntimeException(msg, ioe);
				}
			}
		}

		@Override
		public Iterator<Tuple> apply(Iterator<Tuple2<Object, Tuple>> iterator) {
			initializeJobConf();
			return iterator
					.map(new AbstractFunction1<Tuple2<Object, Tuple>, Tuple>() {

						@Override
						public Tuple apply(Tuple2<Object, Tuple> pair) {
							try {
								Tuple tuple = pair._2;
								finalFunc.setInputs(null);
								finalFunc.attachInput(tuple);
								Tuple result = (Tuple) finalFunc.getNextTuple().result;
								return result;
							} catch (ExecException e) {
								LOG.info(e);
								e.printStackTrace();
								throw new RuntimeException(e);
							}
						}
					});

		}

	}

	private static class NaturalOrdering implements Ordering<Object> {

		@Override
		public int compare(Object arg0, Object arg1) {
			if (arg0 instanceof Comparable && arg1 instanceof Comparable) {
				return ((Comparable) arg0).compareTo((Comparable) arg1);
			} else {
				return arg0.hashCode() - arg1.hashCode();
			}
		}

		@Override
		public boolean equiv(Object arg0, Object arg1) {
			return compare(arg0, arg1) == 0;
		}

		@Override
		public boolean gt(Object arg0, Object arg1) {
			return compare(arg0, arg1) > 0;
		}

		@Override
		public boolean gteq(Object arg0, Object arg1) {
			return compare(arg0, arg1) >= 0;
		}

		@Override
		public boolean lt(Object arg0, Object arg1) {
			return compare(arg0, arg1) < 0;
		}

		@Override
		public boolean lteq(Object arg0, Object arg1) {
			return compare(arg0, arg1) <= 0;
		}

		@Override
		public Object max(Object arg0, Object arg1) {
			return (compare(arg0, arg1) > 0) ? arg0 : arg1;
		}

		@Override
		public Object min(Object arg0, Object arg1) {
			return (compare(arg0, arg1) < 0) ? arg0 : arg1;
		}

		@Override
		public scala.math.Ordering.Ops mkOrderingOps(Object arg0) {
			return new Ordering.Ops(new NaturalOrdering(), arg0);
		}

		@Override
		public <U> Ordering<U> on(Function1<U, Object> arg0) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Ordering<Object> reverse() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Some<Object> tryCompare(Object arg0, Object arg1) {
			// TODO Auto-generated method stub
			return null;
		}

	}
}
