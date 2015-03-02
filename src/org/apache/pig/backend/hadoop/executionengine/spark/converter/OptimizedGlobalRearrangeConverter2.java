package org.apache.pig.backend.hadoop.executionengine.spark.converter;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
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
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.SchemaTupleBackend;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.Spillable;
import org.apache.pig.impl.util.SpillableMemoryManager;
import org.apache.spark.rdd.PairRDDFunctions;
import org.apache.spark.rdd.RDD;
import org.python.modules.synchronize;

import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConversions;
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
			RDD<Tuple2<Object, Tuple>> mapPartitions = predecessors.get(0)
					.mapPartitions(
							new CombinerFunction(
									physicalOperator.getIntermediateForEach(),
									confBytes), false,
							SparkUtil.<Object, Tuple> getTuple2Manifest());
			PairRDDFunctions<Object, Tuple> pairRDDFunctions = new PairRDDFunctions<Object, Tuple>(
					mapPartitions, SparkUtil.getManifest(Object.class),
					SparkUtil.getManifest(Tuple.class), null);
			// mapPartitions.saveAsTextFile("DebugOptimizedGlobalRearrangeConverter1-"
			// + System.currentTimeMillis());
			RDD<Tuple2<Object, Tuple>> combineByKey = pairRDDFunctions
					.combineByKey(new Tuple2ToIterable(),
							new AddTuple2ToIterable(),
							new AddIterableToIterable(), parallelism);
			// combineByKey.saveAsTextFile("DebugOptimizedGlobalRearrangeConverter2-"
			// + System.currentTimeMillis());
			RDD<Tuple> mapPartitions2 = combineByKey.mapPartitions(
					new ReduceFunction(physicalOperator.getFinalForEach(),
							confBytes), false, SparkUtil
							.getManifest(Tuple.class));
			// mapPartitions2.saveAsTextFile("DebugOptimizedGlobalRearrangeConverter3-"
			// + System.currentTimeMillis());

			return mapPartitions2;

		} else {
			throw new RuntimeException(
					"Rearrange Optimization is only for 1 predecessor");
		}
	}

	/**
	 * @param t
	 * @throws ExecException
	 */
	private static void tupleToCollectTuple(Tuple t) throws ExecException {
		int size = t.size();
		for (int i = 1; i < size; i++) {
			Tuple index = (Tuple) t.get(i);
			DataBag b = bf.newDefaultBag();
			b.add(index);
			t.set(i, b);
		}
	}

	/**
	 * @param collectTuple
	 * @param t
	 * @return
	 * @throws ExecException
	 */
	private static void addTupleToCollectTuple(Tuple collectTuple, Tuple t)
			throws ExecException {
		int size = t.size();
		for (int i = 1; i < size; i++) {
			Tuple index = (Tuple) t.get(i);
			DataBag b = (DataBag) collectTuple.get(i);
			b.add(index);
		}
	}

	/**
	 * @param returnTuple
	 * @param t
	 * @throws ExecException
	 */
	private static void addColectTupleToCollectTuple(Tuple returnTuple, Tuple t)
			throws ExecException {
		int size = returnTuple.size();
		for (int i = 1; i < size; i++) {
			DataBag b2 = (DataBag) t.get(i);
			DataBag b = (DataBag) returnTuple.get(i);
			b.addAll(b2);
		}
	}

	private static class CombinerFunction extends
			AbstractFunction1<Iterator<Tuple>, Iterator<Tuple2<Object, Tuple>>>
			implements Serializable, Spillable {

		private static final int SPILL_REGISTER_THRESHOLD = 100 * 1024;

		private boolean spillRegistered;

		HashMap<Object, Tuple> hashMap;
		long aggSampleTupleSize;
		int sampled;
		DataBag results;

		POForEach intermediateFunc;
		private byte[] confBytes;
		private transient JobConf jobConf;

		public CombinerFunction(POForEach intermediateFunc, byte[] confBytes) {
			this.intermediateFunc = intermediateFunc;
			this.confBytes = confBytes;
			this.hashMap = new HashMap<Object, Tuple>();
			spillRegistered = false;
			aggSampleTupleSize = 0;
			sampled = 0;
			results = bf.newDefaultBag();
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

			try {

				while (iterator.hasNext()) {
					Tuple v1 = iterator.next();
					Object key = v1.get(1);
					Tuple t = (Tuple) v1.get(2);

					synchronized (hashMap) {
						Tuple collectTuple = hashMap.get(key);
						if (collectTuple == null) {
							int size = t.size() + 1;
							Tuple newTuple = tf.newTuple(size);
							newTuple.set(0, key);
							for (int i = 1; i < size; i++) {
								DataBag newDefaultBag = bf.newDefaultBag();
								newTuple.set(i, newDefaultBag);
								newDefaultBag.add((Tuple) t.get(i - 1));
							}

							hashMap.put(key, newTuple);

							// Last Tuple Size
							aggSampleTupleSize += newTuple.getMemorySize();
							sampled++;
						} else {
							aggSampleTupleSize -= t.getMemorySize();
							for (int i = 1; i < collectTuple.size(); i++) {
								DataBag dataBag = (DataBag) collectTuple.get(i);
								dataBag.add((Tuple) t.get(i - 1));
							}
							// Last Tuple Size
							aggSampleTupleSize += collectTuple.getMemorySize();
						}
					}

					markSpillableIfNecessary();
				}

				combineCollectedTuples();

				return JavaConversions.asScalaIterator(results.iterator()).map(
						new AbstractFunction1<Tuple, Tuple2<Object, Tuple>>() {
							@Override
							public Tuple2<Object, Tuple> apply(Tuple t) {
								try {
									return new Tuple2<Object, Tuple>(t.get(0),
											t);
								} catch (ExecException e) {
									throw new RuntimeException(e);
								}
							}
						});
			} catch (ExecException e) {
				throw new RuntimeException(e);
			}
		}

		/**
		 * 
		 */
		private void markSpillableIfNecessary() {
			if (!spillRegistered && getMemorySize() > SPILL_REGISTER_THRESHOLD) {
				SpillableMemoryManager.getInstance().registerSpillable(this);
				spillRegistered = true;
			}
		}

		/**
		 * @param results
		 * @param hashMap
		 * @throws ExecException
		 */
		private void combineCollectedTuples() throws ExecException {

			synchronized (hashMap) {
				Set<Entry<Object, Tuple>> entrySet = hashMap.entrySet();
				for (Entry<Object, Tuple> entry : entrySet) {

					Tuple tuple = entry.getValue();
					intermediateFunc.setInputs(null);
					intermediateFunc.attachInput(tuple);

					Tuple result = (Tuple) intermediateFunc.getNextTuple().result;
					results.add(result);

				}
				hashMap.clear();
			}
		}

		@Override
		public long spill() {
			synchronized (hashMap) {
				int size = hashMap.size();
				try {
					LOG.info("Spilling Combiner Size:" + size);
					combineCollectedTuples();
				} catch (ExecException e) {
					throw new RuntimeException(e);
				}
				aggSampleTupleSize = 0;
				sampled = 0;
				return size;
			}
		}

		@Override
		public long getMemorySize() {
			if (sampled == 0)
				return 0;
			else {
				synchronized (hashMap) {
					//32 is entry size loadfactor is 0.75 and capacity over head is 4
					// so 32+4 + (100/75)*4
					return (hashMap.size() * roundToEight(aggSampleTupleSize
							/ sampled + 42));
				}
			}
		}

		private long roundToEight(long i) {
			return 8 * ((i + 7) / 8); // integer division rounds the result down
		}
	}

	private static class Tuple2ToIterable extends
			AbstractFunction1<Tuple, Tuple> implements Serializable {
		@Override
		public Tuple apply(Tuple t) {
			try {
				tupleToCollectTuple(t);
				return t;
			} catch (ExecException e) {
				throw new RuntimeException(e);
			}
		}
	}

	private static class AddTuple2ToIterable extends
			AbstractFunction2<Tuple, Tuple, Tuple> implements Serializable {
		@Override
		public Tuple apply(Tuple it, Tuple t) {
			try {
				addTupleToCollectTuple(it, t);
				return it;
			} catch (ExecException e) {
				throw new RuntimeException(e);
			}
		}

	}

	private static class AddIterableToIterable extends
			AbstractFunction2<Tuple, Tuple, Tuple> implements Serializable {
		@Override
		public Tuple apply(Tuple it, Tuple it2) {
			try {
				addColectTupleToCollectTuple(it, it2);
				return it;
			} catch (ExecException e) {
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
								throw new RuntimeException(e);
							}
						}
					});

		}

	}
}
