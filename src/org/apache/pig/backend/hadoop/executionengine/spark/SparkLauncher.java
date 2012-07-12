package org.apache.pig.backend.hadoop.executionengine.spark;

import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.*;
import java.util.*;

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
import org.apache.pig.backend.hadoop.executionengine.spark.converter.*;
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

        // initialize the supported converters
        Map<Class<? extends PhysicalOperator>, POConverter> convertMap =
                new HashMap<Class<? extends PhysicalOperator>, POConverter>();

        convertMap.put(POLoad.class,    new LoadConverter(pigContext, physicalPlan, sparkContext));
        convertMap.put(POStore.class,   new StoreConverter(pigContext));
        convertMap.put(POForEach.class, new ForEachConverter());
        convertMap.put(POFilter.class,  new FilterConverter());
        convertMap.put(POLocalRearrange.class,  new LocalRearrangeConverter());
        convertMap.put(POGlobalRearrange.class, new GlobalRearrangeConverter());
        convertMap.put(POPackage.class,         new PackageConverter());

        Map<OperatorKey, RDD<Tuple>> rdds = new HashMap<OperatorKey, RDD<Tuple>>();
        
        LinkedList<POStore> stores = PlanHelper.getStores(physicalPlan);
        for (POStore poStore : stores) {
            physicalToRDD(physicalPlan, poStore, rdds, convertMap);
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

    private void physicalToRDD(PhysicalPlan plan, PhysicalOperator physicalOperator,
                               Map<OperatorKey, RDD<Tuple>> rdds,
                               Map<Class<? extends PhysicalOperator>, POConverter> convertMap)
            throws IOException {

        RDD<Tuple> nextRDD = null;
        List<PhysicalOperator> predecessors = plan.getPredecessors(physicalOperator);
        List<RDD<Tuple>> predecessorRdds = Lists.newArrayList();
        if (predecessors!=null) {
            for (PhysicalOperator predecessor : predecessors) {
                physicalToRDD(plan, predecessor, rdds, convertMap);
                predecessorRdds.add(rdds.get(predecessor.getOperatorKey()));
            }
        }

        POConverter converter = convertMap.get(physicalOperator.getClass());
        if (converter == null) {
            throw new IllegalArgumentException("Spork unsupported PhysicalOperator: " + physicalOperator);
        }

        LOG.info("Converting operator " + physicalOperator.getClass().getSimpleName()+" "+physicalOperator);

        // invoke the converter
        try {
            Method m = converter.getClass().getMethod("convert", List.class, PhysicalOperator.class);
            nextRDD = (RDD<Tuple>) m.invoke(converter, predecessorRdds, physicalOperator);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Could not access converter method: " + converter, e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException("Could not invoke converter method: " + converter, e);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Could not find converter method: " + converter, e);
        }

        if (POStore.class.equals(physicalOperator.getClass())) {
            return;
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
