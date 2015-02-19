package org.apache.pig.backend.hadoop.executionengine.spark.converter;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import org.apache.pig.backend.hadoop.executionengine.spark.running.PigInputFormatSpark;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.spark.LoadSparkFunc;

import com.google.common.collect.Lists;

import scala.Function1;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.runtime.AbstractFunction1;

import org.apache.spark.rdd.RDD;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.SparkContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Converter that loads data via POLoad and converts it to RRD&lt;Tuple>. Abuses
 * the interface a bit in that there is no inoput RRD to convert in this case.
 * Instead input is the source path of the POLoad.
 * 
 */
@SuppressWarnings({ "serial" })
public class LoadConverter implements POConverter<Tuple, Tuple, POLoad> {

	private static final ToTupleFunction TO_TUPLE_FUNCTION = new ToTupleFunction();
	private static Log log = LogFactory.getLog(LoadConverter.class);
	private PigContext pigContext;
	private PhysicalPlan physicalPlan;
	private SparkContext sparkContext;

	public LoadConverter(PigContext pigContext, PhysicalPlan physicalPlan,
			SparkContext sparkContext) {
		this.pigContext = pigContext;
		this.physicalPlan = physicalPlan;
		this.sparkContext = sparkContext;
	}

	@Override
	public RDD<Tuple> convert(List<RDD<Tuple>> predecessorRdds, POLoad poLoad)
			throws IOException {
		// if (predecessors.size()!=0) {
		// throw new
		// RuntimeException("Should not have predecessors for Load. Got : "+predecessors);
		// }

		JobConf loadJobConf = SparkUtil.newJobConf(pigContext);
		configureLoader(physicalPlan, poLoad, loadJobConf);

		int parallelism = SparkUtil.getParallelism(predecessorRdds, poLoad);

		String parallelismStr = System.getenv("SPARK_PARALLELISM");
		if (parallelismStr != null) {
			parallelism = Integer.parseInt(parallelismStr);
		}
		log.info("Parallelism for Spark load repartition: " + parallelism);

		String repart = System.getenv("SPARK_REPARTITION_LOAD");
		boolean repartition = false;
		if (repart != null && repart.equalsIgnoreCase("true")) {
			repartition = true;
		}
		log.info("Spark repartition: " + repartition);

		String storageLevel = System.getenv("SPARK_STORAGE_LEVEL");
		log.info("StorageLevel for Spark: " + storageLevel);

		LoadFunc loadFunc = poLoad.getLoadFunc();
		RDD<Tuple> loadedRDD;
		if (loadFunc instanceof LoadSparkFunc) {
			LoadSparkFunc loadSparkFunc = (LoadSparkFunc) loadFunc;
			loadedRDD = loadSparkFunc.getRDDfromContext(sparkContext, poLoad
					.getLFile().getFileName(), loadJobConf);

		} else {
			RDD<Tuple2<Text, Tuple>> hadoopRDD;
			hadoopRDD = sparkContext.newAPIHadoopFile(poLoad.getLFile()
					.getFileName(), PigInputFormatSpark.class, Text.class,
					Tuple.class, loadJobConf);

			registerUdfFiles();
			return hadoopRDD.mapPartitions(TO_TUPLE_FUNCTION, true,
					SparkUtil.getManifest(Tuple.class));
		}

		if (repartition)
			return loadedRDD.repartition(parallelism, null);

		if (storageLevel != null) {

			loadedRDD.persist(StorageLevel.fromString(storageLevel));
		}
		return loadedRDD;
	}

	private void registerUdfFiles() {
		Map<String, File> scriptFiles = pigContext.getScriptFiles();
		for (Map.Entry<String, File> scriptFile : scriptFiles.entrySet()) {
			try {
				File script = scriptFile.getValue();
				if (script.exists()) {
					sparkContext.addFile(script.toURI().toURL()
							.toExternalForm());
				}
			} catch (MalformedURLException e) {
				String msg = "Problem while registering UDF jars and files in LoadConverter.";
				throw new RuntimeException(msg, e);
			}
		}
	}

	private static class ToTupleFunction extends
			AbstractFunction1<Iterator<Tuple2<Text, Tuple>>, Iterator<Tuple>> implements Serializable {

		@Override
		public Iterator<Tuple> apply(Iterator<Tuple2<Text, Tuple>> input) {
			
			return input.map(new AbstractFunction1<Tuple2<Text,Tuple>, Tuple>() {
				@Override
				public Tuple apply(Tuple2<Text, Tuple> v1) {
					return v1._2();
				}
			});
		}

		
	}

	/**
	 * stolen from JobControlCompiler TODO: refactor it to share this
	 * 
	 * @param physicalPlan
	 * @param poLoad
	 * @param jobConf
	 * @return
	 * @throws java.io.IOException
	 */
	private static JobConf configureLoader(PhysicalPlan physicalPlan,
			POLoad poLoad, JobConf jobConf) throws IOException {

		Job job = new Job(jobConf);
		LoadFunc loadFunc = poLoad.getLoadFunc();

		loadFunc.setLocation(poLoad.getLFile().getFileName(), job);

		// stolen from JobControlCompiler
		ArrayList<FileSpec> pigInputs = new ArrayList<FileSpec>();
		// Store the inp filespecs
		pigInputs.add(poLoad.getLFile());

		ArrayList<List<OperatorKey>> inpTargets = Lists.newArrayList();
		ArrayList<String> inpSignatures = Lists.newArrayList();
		ArrayList<Long> inpLimits = Lists.newArrayList();
		// Store the target operators for tuples read
		// from this input
		List<PhysicalOperator> loadSuccessors = physicalPlan
				.getSuccessors(poLoad);
		List<OperatorKey> loadSuccessorsKeys = Lists.newArrayList();
		if (loadSuccessors != null) {
			for (PhysicalOperator loadSuccessor : loadSuccessors) {
				loadSuccessorsKeys.add(loadSuccessor.getOperatorKey());
			}
		}
		inpTargets.add(loadSuccessorsKeys);
		inpSignatures.add(poLoad.getSignature());
		inpLimits.add(poLoad.getLimit());

		jobConf.set("pig.inputs", ObjectSerializer.serialize(pigInputs));
		jobConf.set("pig.inpTargets", ObjectSerializer.serialize(inpTargets));
		jobConf.set("pig.inpSignatures",
				ObjectSerializer.serialize(inpSignatures));
		jobConf.set("pig.inpLimits", ObjectSerializer.serialize(inpLimits));

		return jobConf;
	}
}
