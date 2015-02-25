package org.apache.pig.backend.hadoop.executionengine.spark.converter;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
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
							.getIntermediateForEach(), confBytes));
			// map.saveAsTextFile("DebugOptimized1-" +
			// System.currentTimeMillis());
			RDD<Tuple> map2 = mapValues.mapPartitions(new GroupTupleFunction2(
					physicalOperator.getFinalForEach(), confBytes), false,
					SparkUtil.getManifest(Tuple.class));
			// map2.saveAsTextFile("DebugOptimized2-"
			// + System.currentTimeMillis());
			return map2;

		} else {
			throw new RuntimeException(
					"Rearrange Optimization is only for 1 predecessor");
		}
	}

	private static class ReduceFunction extends
			AbstractFunction2<Tuple, Tuple, Tuple> implements Serializable {

		POForEach intermediateFunc;
		private byte[] confBytes;
		private transient JobConf jobConf;

		public ReduceFunction(POForEach intermediateFunc, byte[] confBytes) {
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
		public Tuple apply(Tuple t1, Tuple t2) {
			initializeJobConf();
			try {
				for (int i = 1; i < t1.size(); i++) {
					DataBag bag1 = (DataBag) t1.get(i);
					DataBag bag2 = (DataBag) t2.get(i);
					bag1.addAll(bag2);
					t1.set(i, bag1);
				}
				intermediateFunc.setInputs(null);
				intermediateFunc.attachInput(t1);

				Tuple result = (Tuple) intermediateFunc.getNextTuple().result;

				for (int i = 1; i < t1.size(); i++) {
					DataBag bag1 = (DataBag) t1.get(i);
					bag1.clear();
					bag1.add((Tuple) result.get(i));
					t1.set(i, bag1);
				}

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

								Tuple item = (Tuple) v1.get(2);
								// Flatten Tuple
								LinkedList<Object> objs = new LinkedList<Object>();
								List<Object> all = item.getAll();
								for (Object obj : all) {
									Tuple onTuple = (Tuple) obj;
									DataBag newDefaultBag = bf.newDefaultBag();
									newDefaultBag.add(onTuple);
									objs.add(newDefaultBag);
								}
								objs.addFirst(key);
								Tuple newTupleNoCopy = tf.newTupleNoCopy(objs);
								return new Tuple2<Object, Tuple>(key,
										newTupleNoCopy);
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
		private byte[] confBytes;
		private transient JobConf jobConf;

		public GroupTupleFunction2(POForEach finalFunc, byte[] confBytes) {
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

		public Iterator<Tuple> apply(Iterator<Tuple2<Object, Tuple>> input) {
			initializeJobConf();
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
