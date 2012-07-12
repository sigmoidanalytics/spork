package org.apache.pig.backend.hadoop.executionengine.spark;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.PigException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.Launcher;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MRCompiler;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.POPackageAnnotator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFilter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POGlobalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.FilterConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.ForEachConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.LoadConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.StoreConverter;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.tools.pigstats.PigStats;
import org.python.google.common.collect.Lists;

import spark.RDD;
import spark.SparkContext;

/**
 * @author billg
 */
public class SparkLauncher extends Launcher {

    private static final Log LOG = LogFactory.getLog(SparkLauncher.class);

    // Our connection to Spark. It needs to be static so that it can be reused across jobs, because a
    // new SparkLauncher gets created for each job.
    private static SparkContext sparkContext = null;
    
    @Override
    public PigStats launchPig(PhysicalPlan physicalPlan, String grpName, PigContext pigContext) throws Exception {
        LOG.info("!!!!!!!!!!  Launching Spark (woot) !!!!!!!!!!!!");
        LOG.debug(physicalPlan);
/////////
// stolen from MapReduceLauncher
        MRCompiler mrCompiler = new MRCompiler(physicalPlan, pigContext);
        mrCompiler.compile();
        MROperPlan plan = mrCompiler.getMRPlan();
        POPackageAnnotator pkgAnnotator = new POPackageAnnotator(plan);
        pkgAnnotator.visit();
//        // this one: not sure
//        KeyTypeDiscoveryVisitor kdv = new KeyTypeDiscoveryVisitor(plan);
//        kdv.visit();

        
/////////
        
        startSparkIfNeeded();

        Map<OperatorKey, RDD<Tuple>> rdds = new HashMap<OperatorKey, RDD<Tuple>>();
        
        LinkedList<POStore> stores = PlanHelper.getStores(physicalPlan);
        for (POStore poStore : stores) {
            physicalToRDD(pigContext, physicalPlan, poStore, rdds, sparkContext);
        }

        return PigStats.get();
    }
    
    private static void startSparkIfNeeded() throws PigException {
        if (sparkContext == null) {
            String master = System.getenv("SPARK_MASTER");
            if (master == null) {
                LOG.info("SPARK_MASTER not specified, using \"local\"");
                master = "local";
            }

            String sparkHome = System.getenv("SPARK_HOME"); // It's okay if this is null for local mode

            // TODO: Don't hardcode this JAR
            List<String> jars = Collections.singletonList("build/pig-0.11.0-SNAPSHOT-withdependencies.jar");

            if (!master.startsWith("local")) {
                // Check that we have the Mesos native library and Spark home are set
                if (sparkHome == null) {
                    System.err.println("You need to set SPARK_HOME to run on a Mesos cluster!");
                    throw new PigException("SPARK_HOME is not set");
                }
                if (System.getenv("MESOS_NATIVE_LIBRARY") == null) {
                    System.err.println("You need to set MESOS_NATIVE_LIBRARY to run on a Mesos cluster!");
                    throw new PigException("MESOS_NATIVE_LIBRARY is not set");
                }
            }

            sparkContext = new SparkContext(master, "Spork", sparkHome, SparkUtil.toScalaSeq(jars));
        }
    }

    // You can use this in unit tests to stop the SparkContext between tests.
    static void stopSpark() {
        if (sparkContext != null) {
            sparkContext.stop();
            sparkContext = null;
        }
    }



    private void physicalToRDD(PigContext pigContext, PhysicalPlan plan,
                               PhysicalOperator physicalOperator, Map<OperatorKey, RDD<Tuple>> rdds, SparkContext sc) throws IOException {
        RDD<Tuple> nextRDD = null;
        List<PhysicalOperator> predecessors = plan.getPredecessors(physicalOperator);
        List<RDD<Tuple>> predecessorRdds = Lists.newArrayList();
        if (predecessors!=null) {
            for (PhysicalOperator predecessor : predecessors) {
                physicalToRDD(pigContext, plan, predecessor, rdds, sc);
                predecessorRdds.add(rdds.get(predecessor.getOperatorKey()));
            }
        }
        
        LOG.info("Converting operator " + physicalOperator.getClass().getSimpleName()+" "+physicalOperator);
        // TODO: put these converters in a Map and look up which one to invoke
        if (physicalOperator instanceof POLoad) {
            
            LoadConverter loadConverter = new LoadConverter(pigContext, plan, sc);
            nextRDD = loadConverter.convert(predecessorRdds, (POLoad)physicalOperator);
            
        } else if (physicalOperator instanceof POStore) {
            
            StoreConverter storeConverter = new StoreConverter(pigContext);
            storeConverter.convert(predecessorRdds, (POStore)physicalOperator);
            return;
            
        } else if (physicalOperator instanceof POForEach) {

            ForEachConverter filterConverter = new ForEachConverter();
            nextRDD = filterConverter.convert(predecessorRdds, (POForEach)physicalOperator);

        } else if (physicalOperator instanceof POFilter) {
            
            FilterConverter filterConverter = new FilterConverter();
            nextRDD = filterConverter.convert(predecessorRdds, (POFilter)physicalOperator);

        } else if (physicalOperator instanceof POLocalRearrange) {
           
            LocalRearrangeConverter localRearrangeConverter = new LocalRearrangeConverter();
            nextRDD = localRearrangeConverter.convert(predecessorRdds, (POLocalRearrange)physicalOperator);

        } else if (physicalOperator instanceof POGlobalRearrange) {
           
            GlobalRearrangeConverter globalRearrangeConverter = new GlobalRearrangeConverter();
            nextRDD = globalRearrangeConverter.convert(predecessorRdds, (POGlobalRearrange)physicalOperator);

        } else if (physicalOperator instanceof POPackage) {
            
            PackageConverter packageConverter = new PackageConverter();
            nextRDD = packageConverter.convert(predecessorRdds, (POPackage)physicalOperator);
            
        }

        if (nextRDD == null) {
            throw new IllegalArgumentException("Spork unsupported PhysicalOperator: " + physicalOperator);
        }
        rdds.put(physicalOperator.getOperatorKey(), nextRDD);
    }

    @Override
    public void explain(PhysicalPlan pp, PigContext pc, PrintStream ps, String format, boolean verbose)
            throws IOException { }
}
