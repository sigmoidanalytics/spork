package org.apache.pig.backend.hadoop.executionengine.spark.converter;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

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
import org.apache.spark.Partitioner;
import org.apache.spark.rdd.OrderedRDDFunctions;
import org.apache.spark.rdd.PairRDDFunctions;
import org.apache.spark.rdd.RDD;

import scala.Function1;
import scala.Some;
import scala.Tuple2;
import scala.collection.GenTraversableOnce;
import scala.collection.Iterator;
import scala.collection.mutable.MutableList;
import scala.math.Ordering;
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

		String parallelStr = System.getenv("SPARK_PARALLELISM");
		if (parallelStr != null) {
			parallelism = Integer.parseInt(parallelStr);
		}
		LOG.info("Parallelism for Spark groupBy: " + parallelism);


		if (predecessors.size() == 1) {
			// GROUP

			RDD<Tuple> rdd = predecessors.get(0);
			//FROM TUPLES CREATE TUPLE2 TUPLE WITH PARTITION AND KEY INFO
			final RDD<Tuple2<Tuple2<Integer, Object>, Tuple>> pairRdd = rdd
					.mapPartitionsWithIndex(
							new TupleToTuple2WithIndex(),
							false,
							SparkUtil
									.<Tuple2<Integer, Object>, Tuple> getTuple2Manifest());

			//
			RDD<Tuple2<Tuple2<Integer, Object>, Tuple>> repartitionAndSortWithinPartitions = new OrderedRDDFunctions<Tuple2<Integer, Object>, Tuple, Tuple2<Tuple2<Integer, Object>, Tuple>>(
					pairRdd,
					new NaturalOrdering(),
					SparkUtil.<Integer, Object> getTuple2Manifest(),
					SparkUtil.getManifest(Tuple.class),
					SparkUtil
							.<Tuple2<Integer, Object>, Tuple> getTuple2Manifest())
					.repartitionAndSortWithinPartitions(new SamePartitionsPartitioner(
							pairRdd.partitions().length));

			RDD<Tuple2<Object, Tuple>> mapPartitions = repartitionAndSortWithinPartitions
					.mapPartitions(
							new CombinerFunction(physicalOperator
									.getIntermediateForEach(), confBytes),
							false, SparkUtil
									.<Object, Tuple> getTuple2Manifest());

			PairRDDFunctions<Object, Tuple> pairRDDFunctions = new PairRDDFunctions<Object, Tuple>(
					mapPartitions, SparkUtil.getManifest(Object.class),
					SparkUtil.getManifest(Tuple.class), null);

			RDD<Tuple2<Object, Tuple>> combineByKey = pairRDDFunctions
					.combineByKey(new Tuple2ToIterable(),
							new AddTuple2ToIterable(),
							new AddIterableToIterable(), parallelism);

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
			AbstractFunction1<Iterator<Tuple2<Tuple2<Integer, Object>, Tuple>>, Iterator<Tuple2<Object, Tuple>>>
			implements Serializable
	{

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
				final Iterator<Tuple2<Tuple2<Integer, Object>, Tuple>> iterator) {
			initializeJobConf();

			LOG.info("COMBINER START");

			return iterator
					.flatMap(new AbstractFunction1<Tuple2<Tuple2<Integer, Object>, Tuple>, GenTraversableOnce<Tuple2<Object, Tuple>>>() {
						Tuple collectTuple = null;
						Object lastKey = null;

						// int elementCounter = 0;

						@Override
						public GenTraversableOnce<Tuple2<Object, Tuple>> apply(
								Tuple2<Tuple2<Integer, Object>, Tuple> t2) {
							try {
								// elementCounter++;
								Tuple v1 = t2._2();
								Object key = v1.get(1);

								MutableList<Tuple2<Object, Tuple>> mutableList = new MutableList<Tuple2<Object, Tuple>>();
								if (!key.equals(lastKey)) {
									// || elementCounter > ELEMENT_THRESHOLD) {
									Tuple combineCollectedTuples = combineCollectedTuples(collectTuple);
									if (combineCollectedTuples != null) {
										mutableList
												.$plus$eq(new Tuple2<Object, Tuple>(
														combineCollectedTuples
																.get(0),
														combineCollectedTuples));
									}
									collectTuple = null;
									lastKey = key;
									// elementCounter = 0;
								}

								if (collectTuple == null) {
									Tuple t = (Tuple) v1.get(2);
									int size = t.size() + 1;
									Tuple newTuple = tf.newTuple(size);
									newTuple.set(0, key);
									for (int i = 1; i < size; i++) {
										DataBag newDefaultBag = bf
												.newDefaultBag();
										newTuple.set(i, newDefaultBag);
										newDefaultBag.add((Tuple) t.get(i - 1));
									}
									collectTuple = newTuple;
								} else {
									Tuple t = (Tuple) v1.get(2);
									for (int i = 1; i < collectTuple.size(); i++) {
										DataBag dataBag = (DataBag) collectTuple
												.get(i);
										dataBag.add((Tuple) t.get(i - 1));
									}
								}
								if (!iterator.hasNext()) {
									Tuple combineCollectedTuples = combineCollectedTuples(collectTuple);
									if (combineCollectedTuples != null) {
										mutableList
												.$plus$eq(new Tuple2<Object, Tuple>(
														combineCollectedTuples
																.get(0),
														combineCollectedTuples));
									}
								}

								return mutableList;
							} catch (ExecException e) {
								LOG.info("COMBINER ERROR" + e);
								throw new RuntimeException(e);
							}
						}

						private Tuple combineCollectedTuples(
								Tuple collectedtuple) throws ExecException {

							if (collectedtuple != null) {
								intermediateFunc.setInputs(null);
								intermediateFunc.attachInput(collectedtuple);

								return (Tuple) intermediateFunc.getNextTuple().result;
							} else {
								return null;
							}

						}
					});
		}
	}

	private static class TupleToTuple2WithIndex
			extends
			AbstractFunction2<Object, Iterator<Tuple>, Iterator<Tuple2<Tuple2<Integer, Object>, Tuple>>>
			implements Serializable {

		@Override
		public Iterator<Tuple2<Tuple2<Integer, Object>, Tuple>> apply(
				final Object index, Iterator<Tuple> it) {
			return it
					.map(new AbstractFunction1<Tuple, Tuple2<Tuple2<Integer, Object>, Tuple>>() {

						@Override
						public Tuple2<Tuple2<Integer, Object>, Tuple> apply(
								Tuple t) {
							try {
								return new Tuple2<Tuple2<Integer, Object>, Tuple>(
										new Tuple2<Integer, Object>(
												(Integer) index, t.get(1)), t);
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

	private static class NaturalOrdering implements
			Ordering<Tuple2<Integer, Object>>, Serializable {

		@Override
		public int compare(Tuple2<Integer, Object> arg0,
				Tuple2<Integer, Object> arg1) {
			if (arg0._2() instanceof Comparable
					&& arg1._2() instanceof Comparable) {
				return ((Comparable) arg0._2()).compareTo((Comparable) arg1
						._2());
			} else {
				return arg0._2().hashCode() - arg1._2().hashCode();
			}
		}

		@Override
		public boolean equiv(Tuple2<Integer, Object> arg0,
				Tuple2<Integer, Object> arg1) {
			return compare(arg0, arg1) == 0;
		}

		@Override
		public boolean gt(Tuple2<Integer, Object> arg0,
				Tuple2<Integer, Object> arg1) {
			return compare(arg0, arg1) > 0;
		}

		@Override
		public boolean gteq(Tuple2<Integer, Object> arg0,
				Tuple2<Integer, Object> arg1) {
			return compare(arg0, arg1) >= 0;
		}

		@Override
		public boolean lt(Tuple2<Integer, Object> arg0,
				Tuple2<Integer, Object> arg1) {
			return compare(arg0, arg1) < 0;
		}

		@Override
		public boolean lteq(Tuple2<Integer, Object> arg0,
				Tuple2<Integer, Object> arg1) {
			return compare(arg0, arg1) <= 0;
		}

		@Override
		public Tuple2<Integer, Object> max(Tuple2<Integer, Object> arg0,
				Tuple2<Integer, Object> arg1) {
			return (compare(arg0, arg1) > 0) ? arg0 : arg1;
		}

		@Override
		public Tuple2<Integer, Object> min(Tuple2<Integer, Object> arg0,
				Tuple2<Integer, Object> arg1) {
			return (compare(arg0, arg1) < 0) ? arg0 : arg1;
		}

		@Override
		public scala.math.Ordering.Ops mkOrderingOps(
				Tuple2<Integer, Object> arg0) {
			return new Ordering.Ops(new NaturalOrdering(), arg0);
		}

		@Override
		public <U> Ordering<U> on(Function1<U, Tuple2<Integer, Object>> arg0) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Ordering<Tuple2<Integer, Object>> reverse() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Some<Object> tryCompare(Tuple2<Integer, Object> arg0,
				Tuple2<Integer, Object> arg1) {
			// TODO Auto-generated method stub
			return null;
		}

	}

	private static class SamePartitionsPartitioner extends Partitioner
			implements Serializable {

		int numPartitions;

		public SamePartitionsPartitioner(int numPartitions) {
			this.numPartitions = numPartitions;
		}

		@Override
		public int numPartitions() {
			return numPartitions;
		}

		@Override
		public int getPartition(Object key) {
			return ((Tuple2<Integer, Object>) key)._1;
		}
	}
}
