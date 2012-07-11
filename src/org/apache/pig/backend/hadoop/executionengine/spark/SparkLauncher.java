package org.apache.pig.backend.hadoop.executionengine.spark;

import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
        
        // Example of how to launch Spark
        SparkContext sc = new SparkContext("local", "Spork", null, null);

        Map<OperatorKey, RDD<Tuple>> rdds = new HashMap<OperatorKey, RDD<Tuple>>();
        
        LinkedList<POStore> stores = PlanHelper.getStores(physicalPlan);
        for (POStore poStore : stores) {
            physicalToRDD(pigContext, physicalPlan, poStore, rdds, sc);
        }

        return PigStats.get();
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
            if (predecessorRdds.size()>0) {
                throw new RuntimeException("Should not have predecessors for Load. Got : "+predecessors);
            }
            LoadConverter loadConverter = new LoadConverter(pigContext, plan, sc);
            nextRDD = loadConverter.convert(null, (POLoad)physicalOperator);
            
        } else if (physicalOperator instanceof POStore) {
            if (predecessors.size()!=1) {
                throw new RuntimeException("Should not have 1 predecessors for Store. Got : "+predecessors);
            }
            RDD<Tuple> rdd = predecessorRdds.get(0);
            StoreConverter storeConverter = new StoreConverter(pigContext);
            storeConverter.convert(rdd, (POStore)physicalOperator);
            return;

        } else if (physicalOperator instanceof POForEach) {
            if (predecessors.size()!=1) {
                throw new RuntimeException("Should not have 1 predecessors for ForEach. Got : "+predecessors);
            }
            RDD<Tuple> rdd = predecessorRdds.get(0);
            ForEachConverter filterConverter = new ForEachConverter();
            nextRDD = filterConverter.convert(rdd, (POForEach)physicalOperator);

        } else if (physicalOperator instanceof POFilter) {
            if (predecessors.size()!=1) {
                throw new RuntimeException("Should not have 1 predecessors for Filter. Got : "+predecessors);
            }
            RDD<Tuple> rdd = predecessorRdds.get(0);
            FilterConverter filterConverter = new FilterConverter();
            nextRDD = filterConverter.convert(rdd, (POFilter)physicalOperator);

        } else if (physicalOperator instanceof POLocalRearrange) {
            if (predecessors.size()!=1) {
                throw new RuntimeException("Should not have 1 predecessors for LocalRearrange. Got : "+predecessors);
            }
            RDD<Tuple> rdd = predecessorRdds.get(0);
            LocalRearrangeConverter localRearrangeConverter = new LocalRearrangeConverter();
            nextRDD = localRearrangeConverter.convert(rdd, (POLocalRearrange)physicalOperator);

        } else if (physicalOperator instanceof POGlobalRearrange) {
            if (predecessors.size()<1) {
                throw new RuntimeException("Should not have at least 1 predecessor for GlobalRearrange. Got : "+predecessors);
            }
            // just a marker that a shuffle is needed
            nextRDD = predecessorRdds.get(0); // maybe put the groupBy here
            

        } else if (physicalOperator instanceof POPackage) {
            if (predecessors.size()!=1) {
                throw new RuntimeException("Should not have 1 predecessors for LocalRearrange. Got : "+predecessors);
            }
            RDD<Tuple> rdd = predecessorRdds.get(0);
            PackageConverter packageConverter = new PackageConverter();
            nextRDD = packageConverter.convert(rdd, (POPackage)physicalOperator);
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
