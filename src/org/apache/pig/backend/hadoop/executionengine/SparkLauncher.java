package org.apache.pig.backend.hadoop.executionengine;

import java.io.IOException;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.LoadFunc;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobControlCompiler;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.Launcher;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigInputFormat;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigOutputFormat;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFilter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.tools.pigstats.PigStats;
import org.python.google.common.collect.Lists;

import scala.Function1;
import scala.Tuple2;
import scala.Tuple2$;
import scala.reflect.ClassManifest;
import scala.reflect.ClassManifest$;
import scala.runtime.AbstractFunction1;
import spark.PairRDDFunctions;
import spark.RDD;
import spark.SparkContext;

/**
 * @author billg
 */
public class SparkLauncher extends Launcher {
    private static final Log LOG = LogFactory.getLog(SparkLauncher.class);

    private static final ToTupleFunction TO_TUPLE_FUNCTION = new ToTupleFunction();
    private static final FromTupleFunction FROM_TUPLE_FUNCTION = new FromTupleFunction();

    @Override
    public PigStats launchPig(PhysicalPlan physicalPlan, String grpName, PigContext pigContext) throws Exception {
        LOG.info("!!!!!!!!!!  Launching Spark (woot) !!!!!!!!!!!!");

        // Example of how to launch Spark
        SparkContext sc = new SparkContext("local", "Spork", null, null);

        LinkedList<POLoad> poLoads = PlanHelper.getLoads(physicalPlan);
        for (POLoad poLoad : poLoads) {
            JobConf loadJobConf = newJobConf(pigContext);
            configureLoader(physicalPlan, poLoad, loadJobConf);

            // don't know why but just doing this cast for now
            RDD<Tuple2<Text, Tuple>> hadoopRDD = sc.newAPIHadoopFile(
                    poLoad.getLFile().getFileName(), PigInputFormat.class,
                    Text.class, Tuple.class, loadJobConf);

            // map to get just RDD<Tuple>
            RDD<Tuple> pigTupleRDD = hadoopRDD.map(TO_TUPLE_FUNCTION,
                    ClassManifest$.MODULE$.fromClass(Tuple.class));

            for (PhysicalOperator successor : physicalPlan.getSuccessors(poLoad)) {
                physicalToRDD(pigContext, physicalPlan, successor, pigTupleRDD);
            }

        }

        return PigStats.get();
    }

    private void physicalToRDD(PigContext pigContext, PhysicalPlan plan,
                               PhysicalOperator physicalOperator, RDD<Tuple> rdd) throws IOException {
        RDD<Tuple> nextRDD = null;

        if (physicalOperator instanceof POStore) {
            // convert back to KV pairs
            RDD<Tuple2> rddPairs = rdd.map(FROM_TUPLE_FUNCTION, getManifest(Tuple2.class));
            PairRDDFunctions<Text, Tuple> pairRDDFunctions = new PairRDDFunctions<Text, Tuple>(
                    (RDD<Tuple2<Text, Tuple>>) ((Object) rddPairs),
                    getManifest(Text.class), getManifest(Tuple.class));

            JobConf storeJobConf = newJobConf(pigContext);
            POStore poStore = configureStorer(storeJobConf, physicalOperator);

            pairRDDFunctions.saveAsNewAPIHadoopFile(poStore.getSFile().getFileName(),
                        Text.class, Tuple.class, PigOutputFormat.class, storeJobConf);
            return;
        } else if (physicalOperator instanceof POForEach) {
            nextRDD = rdd;
        } else if (physicalOperator instanceof POFilter) {
            Function1 filterFunction = new FilterFunction((POFilter)physicalOperator);
            nextRDD = rdd.filter(filterFunction);
        }

        if (nextRDD == null) {
            throw new IllegalArgumentException("Spork unsupported PhysicalOperator: " + physicalOperator);
        }

        for (PhysicalOperator succcessor : plan.getSuccessors(physicalOperator)) {
            physicalToRDD(pigContext, plan, succcessor, nextRDD);
        }
    }

    private JobConf newJobConf(PigContext pigContext) throws IOException {
        JobConf jobConf = new JobConf(ConfigurationUtil.toConfiguration(pigContext.getProperties()));
        jobConf.set("pig.pigContext", ObjectSerializer.serialize(pigContext));
        UDFContext.getUDFContext().serialize(jobConf);
        jobConf.set("udf.import.list", ObjectSerializer.serialize(PigContext.getPackageImportList()));
        return jobConf;
    }

    private POStore configureStorer(JobConf jobConf,
            PhysicalOperator physicalOperator) throws IOException {
        ArrayList<POStore> storeLocations = Lists.newArrayList();
        POStore poStore = (POStore)physicalOperator;
        storeLocations.add(poStore);
        StoreFuncInterface sFunc = poStore.getStoreFunc();
        sFunc.setStoreLocation(poStore.getSFile().getFileName(), new org.apache.hadoop.mapreduce.Job(jobConf));
        poStore.setInputs(null); 
        poStore.setParentPlan(null);
        
        jobConf.set(JobControlCompiler.PIG_MAP_STORES, ObjectSerializer.serialize(Lists.newArrayList()));
        jobConf.set(JobControlCompiler.PIG_REDUCE_STORES, ObjectSerializer.serialize(storeLocations));
        return poStore;
    }

    private <T> ClassManifest<T> getManifest(Class<T> clazz) {
        return ClassManifest$.MODULE$.fromClass(clazz);
    }

    /**
     * stolen from JobControlCompiler
     * TODO: refactor it to share this
     * @param physicalPlan
     * @param poLoad
     * @param jobConf 
     * @return
     * @throws IOException
     */
    private JobConf configureLoader(PhysicalPlan physicalPlan, POLoad poLoad, JobConf jobConf) throws IOException {
        
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

        return jobConf;
    }

    @Override
    public void explain(PhysicalPlan pp, PigContext pc, PrintStream ps, String format, boolean verbose)
            throws IOException { }


    private static class ToTupleFunction extends AbstractFunction1<Tuple2<Text, Tuple>, Tuple>
            implements Function1<Tuple2<Text, Tuple>, Tuple>, Serializable {

        public Tuple apply(Tuple2<Text, Tuple> v1) {
            return v1._2();
        }
    }

    private static class FromTupleFunction extends AbstractFunction1<Tuple, Tuple2>
            implements Function1<Tuple, Tuple2>, Serializable {

        private static Text EMPTY_TEXT = new Text();

        public Tuple2 apply(Tuple v1) {
            return new Tuple2(EMPTY_TEXT, v1);
        }
    }

    // TODO: the filter code below is broken
    private static class FilterFunction extends AbstractFunction1<Tuple, Boolean>
            implements Function1<Tuple, Boolean>, Serializable {

        private POFilter poFilter;

        private FilterFunction(POFilter poFilter) {
            this.poFilter = poFilter;
        }

        public Boolean apply(Tuple v1) {
            Result result;
            try {
                POContainer container = new POContainer(null, v1);
                List<PhysicalOperator> pos = new ArrayList<PhysicalOperator>();
                pos.add(container);

                poFilter.setInputs(pos);
                result = poFilter.getNext(v1);
            } catch (ExecException e) {
                throw new RuntimeException("Couldn't filter tuple", e);
            }

            if (result == null) { return false; }

            switch (result.returnStatus) {
                case POStatus.STATUS_OK:
                    return Boolean.TRUE.equals(result.result);
                default:
                    throw new RuntimeException("Unexpected response code from filter: " + result);
            }
        }
    }

    private static class POContainer extends PhysicalOperator {
        private Tuple tuple;
        boolean returned = false;

        private POContainer(OperatorKey k, Tuple tuple) {
            super(k);
            this.tuple = tuple;
        }

        @Override
        public Result getNext(Tuple t) throws ExecException {
            Result result = new Result();
            result.result = tuple;
            result.returnStatus = returned ? POStatus.STATUS_EOP : POStatus.STATUS_OK;
            returned = true;
            return result;
        }

        @Override
        public void visit(PhyPlanVisitor v) throws VisitorException {
        }

        public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
            return null;
        }

        @Override
        public boolean supportsMultipleInputs() {
            return false;
        }

        @Override
        public boolean supportsMultipleOutputs() {
            return false;
        }

        @Override
        public String name() {
            return null;
        }
    }
}
