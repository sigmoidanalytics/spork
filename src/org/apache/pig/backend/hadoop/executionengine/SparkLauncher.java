package org.apache.pig.backend.hadoop.executionengine;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobCreationException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.Launcher;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigInputFormat;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.tools.pigstats.PigStats;
import org.python.google.common.collect.Lists;

import scala.Tuple2;
import scala.reflect.ClassManifest;
import scala.reflect.ClassManifest$;

import spark.PairRDDFunctions;
import spark.RDD;
import spark.SparkContext;

/**
 * @author billg
 */
public class SparkLauncher extends Launcher {
    private static final Log LOG = LogFactory.getLog(SparkLauncher.class);

    @Override
    public PigStats launchPig(PhysicalPlan physicalPlan, String grpName, PigContext pigContext)
            throws PlanException, VisitorException, IOException, ExecException, JobCreationException, Exception {
        LOG.info("!!!!!!!!!!  Launching Spark (woot) !!!!!!!!!!!!");

        // Example of how to launch Spark
        SparkContext sc = new SparkContext("local", "Spork", null, null);
        LinkedList<POLoad> poLoads = PlanHelper.getLoads(physicalPlan);
        for (POLoad poLoad : poLoads) {

            JobConf jobConf = configureLoader(physicalPlan, pigContext, poLoad);
            // don't know why but just doing this cast for now
            RDD<Tuple2<Text, Tuple>> hadoopRDD = sc.newAPIHadoopFile(poLoad.getLFile().getFileName(),PigInputFormat.class, Text.class, Tuple.class, jobConf);
            
            System.out.println(hadoopRDD.first());
            PairRDDFunctions<Text, Tuple> pairRDDFunctions = new PairRDDFunctions<Text, Tuple>(hadoopRDD, getManifest(Text.class), getManifest(Tuple.class));
//            pairRDDFunctions.saveAsNHadoopFile(path, keyClass, valueClass, outputFormatClass, conf)(path, fm)
            
            
        }

        RDD<String> textFile = sc.textFile("README.txt", 2);
        System.out.println("First line of file: " + textFile.first());
        
        // TODO: run physical plan on Spark

        return PigStats.get();
    }

    private <T> ClassManifest<T> getManifest(Class<T> clazz) {
        return ClassManifest$.MODULE$.fromClass(clazz);
    }

    /**
     * stolen from JobControlCompiler
     * TODO: refactor it to share this
     * @param physicalPlan
     * @param pigContext
     * @param poLoad
     * @return
     * @throws IOException
     */
    private JobConf configureLoader(PhysicalPlan physicalPlan,
            PigContext pigContext, POLoad poLoad) throws IOException {
        JobConf jobConf = new JobConf(ConfigurationUtil.toConfiguration(pigContext.getProperties()));
        Job job = new Job(jobConf);
        LoadFunc loadFunc = poLoad.getLoadFunc();

        loadFunc.setLocation(poLoad.getLFile().getFileName(), job);

        // stolen from JobControlCompiler
        ArrayList<FileSpec> pigInputs = new ArrayList<FileSpec>();
        //Store the inp filespecs
        pigInputs.add(poLoad.getLFile());

        ArrayList<List<OperatorKey>> inpTargets = Lists.newArrayList();
        ArrayList<String> inpSignatures = Lists.newArrayList();
        ArrayList<Long> inpLimits = Lists.newArrayList();
        //Store the target operators for tuples read
        //from this input
        List<PhysicalOperator> loadSuccessors = physicalPlan.getSuccessors(poLoad);
        List<OperatorKey> loadSuccessorsKeys = Lists.newArrayList();
        if(loadSuccessors!=null){
            for (PhysicalOperator loadSuccessor : loadSuccessors) {
                loadSuccessorsKeys.add(loadSuccessor.getOperatorKey());
            }
        }
        inpTargets.add(loadSuccessorsKeys);
        inpSignatures.add(poLoad.getSignature());
        inpLimits.add(poLoad.getLimit());

        jobConf.set("pig.inputs", ObjectSerializer.serialize(pigInputs));
        jobConf.set("pig.inpTargets", ObjectSerializer.serialize(inpTargets));
        jobConf.set("pig.inpSignatures", ObjectSerializer.serialize(inpSignatures));
        jobConf.set("pig.inpLimits", ObjectSerializer.serialize(inpLimits));
        jobConf.set("pig.pigContext", ObjectSerializer.serialize(pigContext));
        jobConf.set("udf.import.list", ObjectSerializer.serialize(PigContext.getPackageImportList()));

        UDFContext.getUDFContext().serialize(jobConf);
        return jobConf;
    }

    @Override
    public void explain(PhysicalPlan pp, PigContext pc, PrintStream ps, String format, boolean verbose)
            throws PlanException, VisitorException, IOException {
    }
}
