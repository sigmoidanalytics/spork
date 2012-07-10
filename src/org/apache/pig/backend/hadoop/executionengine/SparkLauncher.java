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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.executionengine.ExecException;
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

import scala.Tuple2;
import spark.RDD;
import spark.SparkContext;

/**
 * @author billg
 */
public class SparkLauncher extends Launcher {
    private static final Log LOG = LogFactory.getLog(SparkLauncher.class);

    @Override
    public PigStats launchPig(PhysicalPlan php, String grpName, PigContext pc)
            throws PlanException, VisitorException, IOException, ExecException, JobCreationException, Exception {
        LOG.info("!!!!!!!!!!  Launching Spark (woot) !!!!!!!!!!!!");

        // Example of how to launch Spark
        SparkContext sc = new SparkContext("local", "Spork", null, null);
        LinkedList<POLoad> loads = PlanHelper.getLoads(php);
        for (POLoad poLoad : loads) {

            JobConf conf = new JobConf();
            Properties properties = pc.getProperties();
            Set<Entry<Object, Object>> entries = properties.entrySet();
            for (Entry<Object, Object> entry : entries) {
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
            RDD<Tuple2<Text, Tuple>> hadoopRDD = sc.newAPIHadoopFile(poLoad.getLFile().getFileName(),PigInputFormat.class, Text.class, Tuple.class, conf);

            System.out.println(hadoopRDD.first());
        }

        RDD<String> textFile = sc.textFile("README.txt", 2);
        System.out.println("First line of file: " + textFile.first());
        
        // TODO: run physical plan on Spark

        return PigStats.get();
    }

    @Override
    public void explain(PhysicalPlan pp, PigContext pc, PrintStream ps, String format, boolean verbose)
            throws PlanException, VisitorException, IOException {
    }
}
