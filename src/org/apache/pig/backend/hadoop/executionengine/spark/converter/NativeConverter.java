package org.apache.pig.backend.hadoop.executionengine.spark.converter;

import java.io.IOException;
import java.util.List;

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.PONative;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import org.apache.pig.data.Tuple;
import org.apache.spark.rdd.RDD;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MRCompiler;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.NativeMapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.POPackageAnnotator;

public class NativeConverter implements POConverter<Tuple, Tuple, PONative> {

	@Override
	public RDD<Tuple> convert(List<RDD<Tuple>> rdd, PONative poNative)
			throws IOException {
		
		NativeMapReduceOper nativeMROper = new NativeMapReduceOper(poNative.getOperatorKey(), poNative.getNativeMRjar(), poNative.getParams());
		nativeMROper.runJob();
		
		
		/*
		
		MRCompiler mrCompiler = new MRCompiler(physicalPlan, pigContext);
        mrCompiler.compile();
        MROperPlan plan = mrCompiler.getMRPlan();
        POPackageAnnotator pkgAnnotator = new POPackageAnnotator(plan);
        pkgAnnotator.visit();
        
		if(plan.size() == 1 && plan.getRoots().get(0) instanceof NativeMapReduceOper)
	        {
	        	NativeMapReduceOper nativeOper = (NativeMapReduceOper)plan.getRoots().get(0);
	        	//tezScriptState.emitInitialPlanNotification(tezPlan);
	        	//tezScriptState.emitLaunchStartedNotification(tezPlan.size());
	        	//tezScriptState.emitJobsSubmittedNotification(1);
	        	nativeOper.runJob();
	        } */
		//NativeMapReduceOper nativeOper = (NativeMapReduceOper)
		//runJob();
		
		//SparkUtil.assertPredecessorSize(rdd, poNative, 1);
        return null;//rdd.get(0);
	}

}
