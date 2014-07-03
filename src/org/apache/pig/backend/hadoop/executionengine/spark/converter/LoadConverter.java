package org.apache.pig.backend.hadoop.executionengine.spark.converter;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigInputFormat;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import org.apache.pig.backend.hadoop.executionengine.util.MapRedUtil;
import org.apache.pig.backend.hadoop.hbase.HBaseStorage;
//import org.apache.pig.backend.hadoop.hbase.HBaseStorage.ColumnInfo;
import org.apache.pig.backend.hadoop.hbase.ColumnInfo;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.UDFContext;

import scala.Function1;
import scala.Tuple2;
import scala.runtime.AbstractFunction1;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.NewHadoopRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.SparkContext;

import com.google.common.collect.Lists;

/**
 * Converter that loads data via POLoad and converts it to RRD&lt;Tuple>. Abuses the interface a bit
 * in that there is no inoput RRD to convert in this case. Instead input is the source path of the
 * POLoad.
 *
 * @author billg
 */
@SuppressWarnings({ "serial"})
public class LoadConverter implements POConverter<Tuple, Tuple, POLoad> {

    private static final ToTupleHBaseFunction TO_TUPLE_HBASE_FUNCTION = new ToTupleHBaseFunction();
    private static final ToTupleFunction TO_TUPLE_FUNCTION = new ToTupleFunction();
    
    private PigContext pigContext;
    private PhysicalPlan physicalPlan;
    private SparkContext sparkContext;

    public static HBaseStorage hbs;
    
    public LoadConverter(PigContext pigContext, PhysicalPlan physicalPlan, SparkContext sparkContext) {
        this.pigContext = pigContext;
        this.physicalPlan = physicalPlan;
        this.sparkContext = sparkContext;
    }

    @Override
    public RDD<Tuple> convert(List<RDD<Tuple>> predecessorRdds, POLoad poLoad) throws IOException {
//        if (predecessors.size()!=0) {
//            throw new RuntimeException("Should not have predecessors for Load. Got : "+predecessors);
//        }
        	
        JobConf loadJobConf = SparkUtil.newJobConf(pigContext);
                
        //	HBase stuff
        String location = poLoad.getLFile().getFileName();

        if (location.startsWith("hbase://")) {            
//        	configureLoader(physicalPlan, poLoad, loadJobConf);
            hbs = configureHBaseLoader(physicalPlan, poLoad, loadJobConf);           
            List<ColumnInfo> columnInfo = hbs.columnInfo_;
            
            //            SparkUtil.saveObject(hbs, "HBaseStorage");            
            SparkUtil.saveObject((Serializable) columnInfo, "columnInfo_");
            
            JobConf conf = hbs.m_conf;
            // Used by UDFContext
            
            RDD<Tuple2<ImmutableBytesWritable, Result>> rdd = sparkContext.newAPIHadoopRDD(conf, 
            		TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

            return rdd.map(TO_TUPLE_HBASE_FUNCTION, SparkUtil.getManifest(Tuple.class));
        } else {
          configureLoader(physicalPlan, poLoad, loadJobConf);
            // don't know why but just doing this cast for now
          RDD<Tuple2<Text, Tuple>> rdd = sparkContext.newAPIHadoopFile(
                  poLoad.getLFile().getFileName(), PigInputFormat.class,
                  Text.class, Tuple.class, loadJobConf);
          
          return rdd.map(TO_TUPLE_FUNCTION, SparkUtil.getManifest(Tuple.class));
        }
    }

    private static class ToTupleFunction extends AbstractFunction1<Tuple2<Text, Tuple>, Tuple>
            implements Function1<Tuple2<Text, Tuple>, Tuple>, Serializable {

        @Override
        public Tuple apply(Tuple2<Text, Tuple> v1) {
            return v1._2();
        }
    }
    
    private static class ToTupleHBaseFunction extends AbstractFunction1<Tuple2<ImmutableBytesWritable, Result>, Tuple>
            implements Function1<Tuple2<ImmutableBytesWritable, Result>, Tuple>, Serializable {
        
		@SuppressWarnings("unchecked")
		public Tuple apply(Tuple2<ImmutableBytesWritable, Result> v1) {
			Result result = v1._2;
			
			// TODO
			boolean loadRowKey_ = true;
			
			// Copied below stuff from HBaseStorage getNext()			
            ImmutableBytesWritable rowKey = v1._1;
            List<ColumnInfo> columnInfo_ = null;
			try {
				columnInfo_ = (List<ColumnInfo> )SparkUtil.readObject("columnInfo_");
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
            
			int tupleSize = columnInfo_.size();

            // use a map of families -> qualifiers with the most recent
            // version of the cell. Fetching multiple versions could be a
            // useful feature.
            NavigableMap<byte[], NavigableMap<byte[], byte[]>> resultsMap =
                    result.getNoVersionMap();

            if (loadRowKey_){
                tupleSize++;
            }
            Tuple tuple=TupleFactory.getInstance().newTuple(tupleSize);

            int startIndex=0;
            if (loadRowKey_){
                try {
					tuple.set(0, new DataByteArray(rowKey.get()));
				} catch (ExecException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
                startIndex++;
            }
            for (int i = 0;i < columnInfo_.size(); ++i){
                int currentIndex = startIndex + i;

                ColumnInfo columnInfo = columnInfo_.get(i);
                if (columnInfo.isColumnMap()) {
                    // It's a column family so we need to iterate and set all
                    // values found
                    NavigableMap<byte[], byte[]> cfResults =
                            resultsMap.get(columnInfo.getColumnFamily());
                    Map<String, DataByteArray> cfMap =
                            new HashMap<String, DataByteArray>();

                    if (cfResults != null) {
                        for (byte[] quantifier : cfResults.keySet()) {
                            // We need to check against the prefix filter to
                            // see if this value should be included. We can't
                            // just rely on the server-side filter, since a
                            // user could specify multiple CF filters for the
                            // same CF.
                            if (columnInfo.getColumnPrefix() == null ||
                                    columnInfo.hasPrefixMatch(quantifier)) {

                                byte[] cell = cfResults.get(quantifier);
                                DataByteArray value =
                                        cell == null ? null : new DataByteArray(cell);
                                cfMap.put(Bytes.toString(quantifier), value);
                            }
                        }
                    }
                    try {
						tuple.set(currentIndex, cfMap);
					} catch (ExecException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
                } else {
                    // It's a column so set the value
                    byte[] cell= result.getValue(columnInfo.getColumnFamily(),
                                                columnInfo.getColumnName());
                    DataByteArray value =
                            cell == null ? null : new DataByteArray(cell);
                    try {
						tuple.set(currentIndex, value);
					} catch (ExecException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
                }
            }

            return tuple;			
        }
    }    

    private HBaseStorage configureHBaseLoader(PhysicalPlan physicalPlan,
            POLoad poLoad, JobConf jobConf) throws IOException {    	
    	// copied from configureLoader
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
        
    	return (HBaseStorage)loadFunc;
    }
    
    /**
     * stolen from JobControlCompiler
     * TODO: refactor it to share this
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
}
