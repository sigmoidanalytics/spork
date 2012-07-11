package org.apache.pig.backend.hadoop.executionengine;

import java.io.IOException;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
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
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.KeyTypeDiscoveryVisitor;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.Launcher;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MRCompiler;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigInputFormat;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigOutputFormat;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.POPackageAnnotator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFilter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POGlobalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.io.PigNullableWritable;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.tools.pigstats.PigStats;
import org.python.google.common.collect.Lists;

import scala.Function1;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.Seq;
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

            Function1 forEachFunction = new ForEachFunction((POForEach)physicalOperator);
            nextRDD = rdd.map(forEachFunction, getManifest(Tuple.class));

        } else if (physicalOperator instanceof POFilter) {
            
            Function1 filterFunction = new FilterFunction((POFilter)physicalOperator);
            nextRDD = rdd.filter(filterFunction);
            
        } else if (physicalOperator instanceof POLocalRearrange) {
            nextRDD = rdd
                    // call local rearrange to get key and value
                    .map(new LocalRearangeFunction((POLocalRearrange)physicalOperator), getManifest(Tuple.class))
                    // group by key
                    .groupBy(new GetKeyFunction(), getManifest(Object.class))
                    // convert result to a tuple (key, { values })
                    .map(new GroupTupleFunction(), getManifest(Tuple.class));
        } else if (physicalOperator instanceof POGlobalRearrange) {
            // just a marker that a shuffle is needed
            nextRDD = rdd; // maybe put the groupBy here
        } else if (physicalOperator instanceof POPackage) {
            Function1<Tuple, Tuple> packageFunction = new PackageFunction((POPackage)physicalOperator);
            // package will generate the group from the result of the local rearange
            nextRDD = rdd.map(packageFunction, getManifest(Tuple.class));
        }
        
        if (nextRDD == null) {
            throw new IllegalArgumentException("Spork unsupported PhysicalOperator: " + physicalOperator.getClass().getSimpleName() + " "+physicalOperator);
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
                poFilter.setInputs(null);
                poFilter.attachInput(v1);
                result = poFilter.getNext(v1);
            } catch (ExecException e) {
                throw new RuntimeException("Couldn't filter tuple", e);
            }

            if (result == null) { return false; }

            switch (result.returnStatus) {
                case POStatus.STATUS_OK:
                    return true;
                case POStatus.STATUS_EOP: // TODO: probably also ok for EOS, END_OF_BATCH
                    return false;
                default:
                    throw new RuntimeException("Unexpected response code from filter: " + result);
            }
        }
    }

    private static class ForEachFunction extends AbstractFunction1<Tuple, Tuple>
            implements Function1<Tuple, Tuple>, Serializable {

        private POForEach poForEach;

        private ForEachFunction(POForEach poForEach) {
            this.poForEach = poForEach;
        }

        public Tuple apply(Tuple v1) {
            Result result;
            try {
                poForEach.setInputs(null);
                poForEach.attachInput(v1);
                result = poForEach.getNext(v1);
            } catch (ExecException e) {
                throw new RuntimeException("Couldn't do forEach on tuple: " + v1, e);
            }

            if (result == null) {
                throw new RuntimeException("Null response found for forEach on tuple: " + v1);
            }

            switch (result.returnStatus) {
                case POStatus.STATUS_OK:
                    return (Tuple)result.result;
                default:
                    throw new RuntimeException("Unexpected response code from filter: " + result);
            }
        }
    }

    private static class LocalRearangeFunction extends AbstractFunction1<Tuple, Tuple> implements Serializable {
        
        private static TupleFactory tf = TupleFactory.getInstance();
        private final POLocalRearrange physicalOperator;

        public LocalRearangeFunction(POLocalRearrange physicalOperator) {
            this.physicalOperator = physicalOperator;
        }

        @Override
        public Tuple apply(Tuple t) {
            Result result;
            try {
                physicalOperator.setInputs(null);
                physicalOperator.attachInput(t);
                result = physicalOperator.getNext((Tuple)null);

                if (result == null) {
                    throw new RuntimeException("Null response found for LocalRearange on tuple: " + t);
                }

                switch (result.returnStatus) {
                case POStatus.STATUS_OK:
                    // we don't need the index used for namespacing in Pig
                    Tuple resultTuple = (Tuple)result.result;
                    Tuple newTuple = tf.newTuple(2);
                    newTuple.set(0, resultTuple.get(1)); // key
                    newTuple.set(1, resultTuple.get(2)); // value (stripped of the key, reconstructed in package)
                    return newTuple;
                default:
                    throw new RuntimeException("Unexpected response code from operator "+physicalOperator+" : " + result);
                }
            } catch (ExecException e) {
                throw new RuntimeException("Couldn't do LocalRearange on tuple: " + t, e);
            }
        }

    }

    private static class PackageFunction extends AbstractFunction1<Tuple, Tuple> implements Serializable {

        private static TupleFactory tf = TupleFactory.getInstance();
        private final POPackage physicalOperator;

        public PackageFunction(POPackage physicalOperator) {
            this.physicalOperator = physicalOperator;
        }

        @Override
        public Tuple apply(final Tuple t) {
            Result result;
            try {
                PigNullableWritable key = new PigNullableWritable() {
                    public Object getValueAsPigType() {
                        try {
                            Object keyTuple = t.get(0);
                            return keyTuple;
                        } catch (ExecException e) {
                           throw new RuntimeException(e);
                        }
                    }
                };
                final Iterator<Tuple> bagIterator = ((DataBag)t.get(1)).iterator();
                Iterator<NullableTuple> iterator = new Iterator<NullableTuple>() {
                    public boolean hasNext() {
                        return bagIterator.hasNext();
                    }
                    public NullableTuple next() {
                        Tuple next = bagIterator.next();
                        
                        return new NullableTuple(next);
                    }
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
                physicalOperator.setInputs(null);
                physicalOperator.attachInput(key, iterator);
                result = physicalOperator.getNext((Tuple)null);
            } catch (ExecException e) {
                throw new RuntimeException("Couldn't do Package on tuple: " + t, e);
            }

            if (result == null) {
                throw new RuntimeException("Null response found for Package on tuple: " + t);
            }

            switch (result.returnStatus) {
                case POStatus.STATUS_OK:
                    // (key, {(value)...})
                return (Tuple)result.result;
                default:
                    throw new RuntimeException("Unexpected response code from operator "+physicalOperator+" : " + result);
            }
        }

    }
    private static class GetKeyFunction extends AbstractFunction1<Tuple, Object> implements Serializable {

        public Object apply(Tuple t) {
            try {
                // see PigGenericMapReduce For the key
                Object key = t.get(0);
                return key;
            } catch (ExecException e) {
                throw new RuntimeException(e);
            }
        }
    }
    
    private static class GroupTupleFunction extends AbstractFunction1<Tuple2<Object, Seq<Tuple>>, Tuple> implements Serializable {

        private static BagFactory bf = BagFactory.getInstance();
        private static TupleFactory tf = TupleFactory.getInstance();        

        public Tuple apply(Tuple2<Object, Seq<Tuple>> v1) {
            try {
                DataBag bag = bf.newDefaultBag();
                Seq<Tuple> gp = v1._2();
                Iterable<Tuple> asJavaIterable = JavaConversions.asJavaIterable(gp);
                for (Tuple tuple : asJavaIterable) {
                    // keeping only the value
                    bag.add((Tuple)tuple.get(1));
                }
                Tuple tuple = tf.newTuple(2);
                tuple.set(0, v1._1()); // the key
                tuple.set(1, bag);
                return tuple;
            } catch (ExecException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
