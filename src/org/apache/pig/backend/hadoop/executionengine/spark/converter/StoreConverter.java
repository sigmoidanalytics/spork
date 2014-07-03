package org.apache.pig.backend.hadoop.executionengine.spark.converter;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobControlCompiler;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigOutputFormat;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import org.apache.pig.backend.hadoop.hbase.HBaseStorage;
import org.apache.pig.builtin.LOG;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.util.ObjectSerializer;

import scala.Function1;
import scala.Function2;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.reflect.ClassTag;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;

import org.apache.spark.rdd.PairRDDFunctions;
import org.apache.spark.rdd.RDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;

import com.google.common.collect.Lists;

/**
 * Converter that takes a POStore and stores it's content.
 *
 * @author billg
 */
@SuppressWarnings({ "serial"})
public class StoreConverter implements POConverter<Tuple, Tuple2<Text, Tuple>, POStore> {

	private static final FromTupleFunction FROM_TUPLE_FUNCTION = new FromTupleFunction();
//	private static final WriteToHBase WRITE_TO_HBASE_FUNCTION = new WriteToHBase();
	
	private PigContext pigContext;
	private static HBaseStorage hbs;

	public StoreConverter(PigContext pigContext) {
		this.pigContext = pigContext;
	}

	@Override
	public RDD<Tuple2<Text, Tuple>> convert(List<RDD<Tuple>> predecessors, POStore physicalOperator) throws IOException {
		SparkUtil.assertPredecessorSize(predecessors, physicalOperator, 1);
		RDD<Tuple> rdd = predecessors.get(0);
		// convert back to KV pairs
		RDD<Tuple2<Text, Tuple>> rddPairs = rdd.map(FROM_TUPLE_FUNCTION, SparkUtil.<Text, Tuple>getTuple2Manifest());
		PairRDDFunctions<Text, Tuple> pairRDDFunctions = new PairRDDFunctions<Text, Tuple>(rddPairs,
				SparkUtil.getManifest(Text.class), SparkUtil.getManifest(Tuple.class));

		JobConf storeJobConf = SparkUtil.newJobConf(pigContext);
		POStore poStore = configureStorer(storeJobConf, physicalOperator);

		pairRDDFunctions.saveAsNewAPIHadoopFile(poStore.getSFile().getFileName(),Text.class, Tuple.class, PigOutputFormat.class, storeJobConf);
		//        pairRDDFunctions.saveAsHadoopFile(poStore.getSFile().getFileName(), ImmutableBytesWritable.class, Result.class, 
		//        		TableOutputFormat.class, storeJobConf);

//		pairRDDFunctions.mapValues(WRITE_TO_HBASE_FUNCTION, SparkUtil.getManifest(Text.class));
//		pairRDDFunctions.mapValues(WRITE_TO_HBASE_FUNCTION);
		
//	    JavaPairRDD<String, Integer> counts = pairRDDFunctions.mapValues(new Function2<Integer, Integer, Integer>() {
//	        @Override
//	        public Integer call(Integer i1, Integer i2) {
//	          return i1 + i2;
//	        }
//	      });

		
//		Map<Text, Tuple> pRdd = JavaConversions.mapAsJavaMap(pairRDDFunctions.collectAsMap());
//        pairRDDFunctions.mapValues(WRITE_TO_HBASE_FUNCTION, SparkUtil.getManifest(Tuple.class));		

		// HBase writing stuff
//		String outTable = "outTable3";
//
//		try {
//
//			org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
//			conf.addResource("/usr/local/Cellar/hbase/hbase-0.94.1/conf/hbase-site.xml");    			
//
//			HBaseAdmin admin=new HBaseAdmin(conf);
//			if(!admin.isTableAvailable(outTable)) {
//				HTableDescriptor tableDesc = new HTableDescriptor(outTable);
//				admin.createTable(tableDesc);
//
//				HColumnDescriptor hcd=new HColumnDescriptor("cf");
//				admin.addColumn(outTable, hcd);
//				System.out.println("table created and family added");
//			}		
//			HTable ht = new HTable(conf, outTable);    				    		
//
//			for (int i=0; i<10; i++) {
//				Put p = new Put(Bytes.toBytes("row100"));
//				p.add(Bytes.toBytes("cf"), Bytes.toBytes("M"), Bytes.toBytes("H"));
//				ht.put(p);	
//			}
//		} catch (Exception e){
//			e.printStackTrace();
//		}	
		
		return rddPairs;
	}


	private static POStore configureStorer(JobConf jobConf,
			PhysicalOperator physicalOperator) throws IOException {
		ArrayList<POStore> storeLocations = Lists.newArrayList();
		POStore poStore = (POStore)physicalOperator;
		storeLocations.add(poStore);
		StoreFuncInterface sFunc = poStore.getStoreFunc();
		sFunc.setStoreLocation(poStore.getSFile().getFileName(), new org.apache.hadoop.mapreduce.Job(jobConf));
		
//		hbs = (HBaseStorage) poStore.getStoreFunc();
//		hbs.setStoreLocation(poStore.getSFile().getFileName(), new org.apache.hadoop.mapreduce.Job(jobConf));
		
		poStore.setInputs(null);
		poStore.setParentPlan(null);

		jobConf.set(JobControlCompiler.PIG_MAP_STORES, ObjectSerializer.serialize(Lists.newArrayList()));
		jobConf.set(JobControlCompiler.PIG_REDUCE_STORES, ObjectSerializer.serialize(storeLocations));
		
		return poStore;
	}

	private static class FromTupleFunction extends Function<Tuple, Tuple2<Text, Tuple>>
	implements Serializable {

		private static Text EMPTY_TEXT = new Text();

		public Tuple2<Text, Tuple> call(Tuple v1) {
			
//			String tablename = "out1";
			
//			org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
//			conf.addResource("/usr/local/Cellar/hbase/hbase-0.94.1/conf/hbase-site.xml");
//	
//			HBaseAdmin admin = null;
//			try {
//				admin = new HBaseAdmin(conf);
//			} catch (MasterNotRunningException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			} catch (ZooKeeperConnectionException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}	
//			
//			try {
//				if(!admin.isTableAvailable(tablename)) {
//					HTableDescriptor tableDesc = new HTableDescriptor(tablename);
//					admin.createTable(tableDesc);
//	
//					HColumnDescriptor hcd=new HColumnDescriptor("cf");
//					admin.addColumn(tablename, hcd);
//					System.out.println("table created and family added");
//				}
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//	
//			HTable ht = null;;
//			try {
//				ht = new HTable(conf, tablename);
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}	
//			
//			for (Object txt : v1.getAll().subList(2, v1.size())) {
//				try {
//					Put p = new Put(Bytes.toBytes(v1.get(0).toString()));					
//					p.add(Bytes.toBytes("cf"), Bytes.toBytes("100"), Bytes.toBytes(txt.toString()));
//					ht.put(p);
//				} catch (ExecException e1) {
//					// TODO Auto-generated catch block
//					e1.printStackTrace();
//				} catch (IOException e1) {
//					// TODO Auto-generated catch block
//					e1.printStackTrace();
//				}
//			}
//			
//			try {
//				ht.close();
//				admin.close();
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
			
			return new Tuple2<Text, Tuple>(EMPTY_TEXT, v1);
		}
	}    

		
//	private static abstract class WriteToHBase extends AbstractFunction1<Tuple, Text>
//	implements Function1<Tuple, Text>, Serializable {
				
//	private static final class WriteToHBase extends AbstractFunction1<Tuple, Text> implements Serializable {
//
//		@Override
//		public Text apply(Tuple arg0) {
//
//			org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
//			conf.addResource("/usr/local/Cellar/hbase/hbase-0.94.1/conf/hbase-site.xml");
//	
//			HBaseAdmin admin = null;
//			try {
//				admin = new HBaseAdmin(conf);
//			} catch (MasterNotRunningException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			} catch (ZooKeeperConnectionException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//	
//			try {
//				if(!admin.isTableAvailable("out1")) {
//					HTableDescriptor tableDesc = new HTableDescriptor("out1");
//					admin.createTable(tableDesc);
//	
//					HColumnDescriptor hcd=new HColumnDescriptor("cf");
//					admin.addColumn("out1", hcd);
//					System.out.println("table created and family added");
//				}
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//	
//			HTable ht = null;;
//			try {
//				ht = new HTable(conf, "out1");
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//	
//			Put p=new Put(Bytes.toBytes("row100"));
//			p.add(Bytes.toBytes("cf"), Bytes.toBytes("100"), Bytes.toBytes("ABC"));
//			try {
//				ht.put(p);
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//	
//			return null;
//		}
//	}
}
