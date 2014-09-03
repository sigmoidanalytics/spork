package org.apache.pig.backend.hadoop.executionengine.spark.converter;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.mapred.JobConf;
import org.apache.pig.Main;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStream;
import org.apache.pig.backend.hadoop.executionengine.spark.BroadCastClient;
import org.apache.pig.backend.hadoop.executionengine.spark.BroadCastServer;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;

import scala.collection.Iterator;
import scala.collection.JavaConversions;

public class StreamConverterMain implements POConverter<Tuple, Tuple, POStream> {

	private PigContext pigContext;
	private PhysicalPlan physicalPlan;
	private SparkContext sparkContext;

	public static JobConf sjobConf;

	public StreamConverterMain(PigContext pigContext, PhysicalPlan physicalPlan, SparkContext sparkContext) {
		this.pigContext = pigContext;
		this.physicalPlan = physicalPlan;
		this.sparkContext = sparkContext;
	}
	@Override
	public RDD<Tuple> convert(List<RDD<Tuple>> predecessors, POStream physicalOperator)
			throws IOException {

		SparkUtil.assertPredecessorSize(predecessors, physicalOperator, 1);
		RDD<Tuple> rdd = predecessors.get(0);

		JobConf loadJobConf = SparkUtil.newJobConf(pigContext);

		sjobConf = loadJobConf;
		//Setting some random values, to prevent crash in HadoopExecutableManager
		sjobConf.set("pig.streaming.task.output.dir", "/1");
		sjobConf.set("pig.streaming.log.dir", "/1/_logs");
		sjobConf.set("mapred.task.id","/attempt_m_000000_0");

		physicalOperator.setParentPlan(physicalPlan);

		RDD<Tuple> rdd2 = rdd.coalesce(1, false);
		long count = 0;
		try{

			if(physicalPlan.toString().toLowerCase().contains("stream")){
				count = rdd2.count();
				long ccount = 0;
				BroadCastServer bs = Main.bcaster;
				bs.addResource("end_of_input", count);
				bs.addResource("current_count", ccount);
			}

		}catch(Exception e){

		}
		return rdd2.mapPartitions(new StreamFunctionMain(physicalOperator,count),true, SparkUtil.getManifest(Tuple.class));

	}
	
	private static class StreamFunctionMain extends Function<Iterator<Tuple>, Iterator<Tuple>>
	implements Serializable {

		private static final long serialVersionUID = 2L;

		private POStream poStream;		
	    public long current_val;
	    public long total_limit;

	    boolean proceed;
	    
		private StreamFunctionMain(POStream poStream, long count) {
			this.poStream = poStream;
			total_limit = count;
			current_val = 0;
			proceed = false;
		}

		public Iterator<Tuple> call(Iterator<Tuple> i) {
			final java.util.Iterator<Tuple> input = JavaConversions.asJavaIterator(i);
			Iterator<Tuple> output = JavaConversions.asScalaIterator(new POOutputConsumerIterator(input) {
				protected void attach(Tuple tuple) {
					poStream.setInputs(null);
					poStream.attachInput(tuple);
					 try{
		            	
		            	current_val = current_val + 1;
		            	//System.out.println("Row: =>" + current_val);
		            	if(current_val == total_limit){
		                	proceed = true;
		                }else{
		                	proceed = false;
		                }
		                
		            }catch(Exception e){
		            	System.out.println("Crashhh in StreamConverterMain :" + e);
		            	e.printStackTrace();
		            }
					
				}

				protected Result getNextResult() throws ExecException {
					 					
					return poStream.getNextTuple(proceed);
					
				}
			});
			return output;
		}
	}
	
/* Previous hack, Not using now.
	private static class StreamFunctionMain2 extends Function<Iterator<Tuple>, Iterator<Tuple>>
	implements Serializable {

		private static final long serialVersionUID = 2L;

		private POStream poStream;

		private StreamFunctionMain2(POStream poStream, long count) {
			this.poStream = poStream;
			
		}

		public Iterator<Tuple> call(Iterator<Tuple> i) {
			final java.util.Iterator<Tuple> input = JavaConversions.asJavaIterator(i);
			Iterator<Tuple> output = JavaConversions.asScalaIterator(new POOutputConsumerIterator(input) {
				protected void attach(Tuple tuple) {
					poStream.setInputs(null);
					poStream.attachInput(tuple);
				}

				protected Result getNextResult() throws ExecException {
										
					return poStream.getNextTuple();
					
				}
			});
			return output;
		}
	}*/
	

}
