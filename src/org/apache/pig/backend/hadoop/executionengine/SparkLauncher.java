package org.apache.pig.backend.hadoop.executionengine;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.Launcher;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigInputFormat;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFilter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
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

import scala.reflect.ClassManifest$;
import scala.Function1;
import scala.Tuple2;
import scala.reflect.ClassManifest;
import scala.runtime.AbstractFunction1;
import spark.RDD;
import spark.SparkContext;

import java.io.IOException;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.*;

/**
 * @author billg
 */
public class SparkLauncher extends Launcher {
    private static final Log LOG = LogFactory.getLog(SparkLauncher.class);

    private static final ToTupleFunction toTupleFunction = new ToTupleFunction();

    @Override
    public PigStats launchPig(PhysicalPlan php, String grpName, PigContext pc) throws Exception {
        LOG.info("!!!!!!!!!!  Launching Spark (woot) !!!!!!!!!!!!");

        // Example of how to launch Spark
        SparkContext sc = new SparkContext("local", "Spork", null, null);
        LinkedList<POLoad> loads = PlanHelper.getLoads(php);
        for (POLoad poLoad : loads) {

            JobConf conf = new JobConf();
            Properties properties = pc.getProperties();
            Set<Map.Entry<Object, Object>> entries = properties.entrySet();
            for (Map.Entry<Object, Object> entry : entries) {
                String key = (String)entry.getKey();
                String value = (String)entry.getValue();
                conf.set(key, value);
            }
            Job job = new Job(conf);
            LoadFunc lf = poLoad.getLoadFunc();

            lf.setLocation(poLoad.getLFile().getFileName(), job);

            // stolen from JobControlCompiler
            ArrayList<FileSpec> inp = new ArrayList<FileSpec>();
            //Store the inp filespecs
            inp.add(poLoad.getLFile());

            ArrayList<List<OperatorKey>> inpTargets = new ArrayList<List<OperatorKey>>();
            ArrayList<String> inpSignatureLists = new ArrayList<String>();
            ArrayList<Long> inpLimits = new ArrayList<Long>();
            //Store the target operators for tuples read
            //from this input
            List<PhysicalOperator> ldSucs = php.getSuccessors(poLoad);
            List<OperatorKey> ldSucKeys = new ArrayList<OperatorKey>();
            if(ldSucs!=null){
                for (PhysicalOperator operator2 : ldSucs) {
                    ldSucKeys.add(operator2.getOperatorKey());
                }
            }
            inpTargets.add(ldSucKeys);
            inpSignatureLists.add(poLoad.getSignature());
            inpLimits.add(poLoad.getLimit());

            conf.set("pig.inputs", ObjectSerializer.serialize(inp));
            conf.set("pig.inpTargets", ObjectSerializer.serialize(inpTargets));
            conf.set("pig.inpSignatures", ObjectSerializer.serialize(inpSignatureLists));
            conf.set("pig.inpLimits", ObjectSerializer.serialize(inpLimits));
            conf.set("pig.pigContext", ObjectSerializer.serialize(pc));
            conf.set("udf.import.list", ObjectSerializer.serialize(PigContext.getPackageImportList()));

            UDFContext.getUDFContext().serialize(conf);
            // don't know why but just doing this cast for now
            RDD<Tuple2<Text, Tuple>> hadoopRDD = sc.newAPIHadoopFile(
                    poLoad.getLFile().getFileName(), PigInputFormat.class, Text.class, Tuple.class, conf);

            //TODO: map to get just RDD<Tuple>
            RDD<Tuple> pigTupleRDD = hadoopRDD.map(toTupleFunction,
                    ClassManifest$.MODULE$.fromClass(Tuple.class));

            for (PhysicalOperator successor : php.getSuccessors(poLoad)) {
                physicalToRDD(pigTupleRDD, php, successor);
            }
        }

        return PigStats.get();
    }

    private void physicalToRDD(RDD<Tuple> rdd, PhysicalPlan plan, PhysicalOperator po) {
        RDD<Tuple> nextRDD = null;

        if (po instanceof POStore) {
            //TODO: this should use the OutputFormat
            rdd.saveAsTextFile(((POStore)po).getSFile().getFileName());
            return;
        } else if (po instanceof POForEach) {
            nextRDD = rdd;
        } else if (po instanceof POFilter) {
            Function1 filterFunction = new FilterFunction((POFilter)po);
            nextRDD = rdd.filter(filterFunction);
        }

        if (nextRDD == null) {
            throw new IllegalArgumentException("Spork unsupported PhysicalOperator: " + po);
        }

        for (PhysicalOperator succcessor : plan.getSuccessors(po)) {
            physicalToRDD(nextRDD, plan, succcessor);
        }
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
