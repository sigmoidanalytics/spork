package org.apache.pig.backend.hadoop.executionengine.spark;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.PigException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POUserFunc;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.PODistinct;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POGlobalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLimit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPartialAgg;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPreCombinerLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.backend.hadoop.executionengine.spark.plans.POOptimizedGlobalRearrange;
import org.apache.pig.backend.hadoop.executionengine.util.CombinerOptimizerUtil;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.plan.optimizer.OptimizerException;
import org.apache.pig.impl.util.Pair;

import com.google.common.collect.Maps;

public class SparkCombinerOptimizerUtil extends CombinerOptimizerUtil {

	private static final Log LOG = LogFactory
			.getLog(SparkCombinerOptimizerUtil.class);

	protected SparkCombinerOptimizerUtil() {
	}

	public static void addCombiner(PhysicalPlan plan) throws VisitorException,
			PlanException, CloneNotSupportedException {
		LinkedList<POLocalRearrange> physicalOperators = PlanHelper
				.getPhysicalOperators(plan, POLocalRearrange.class);
		for (POLocalRearrange localRearrange : physicalOperators) {
			List<PhysicalOperator> localSuccessors = plan
					.getSuccessors(localRearrange);
			if (localSuccessors.size() != 1) {
				// NO IDEA WHATS GOING ON
				return;
			}
			PhysicalOperator physicalOperator = localSuccessors.get(0);
			if (!(physicalOperator instanceof POGlobalRearrange)) {
				// Global rearrange should be followed by Package
				return;
			}
			POGlobalRearrange globalRearrange = (POGlobalRearrange) physicalOperator;
			List<PhysicalOperator> globalSuccessors = plan
					.getSuccessors(globalRearrange);
			if (globalSuccessors.size() != 1) {
				// NO IDEA WHATS GOING ON
				return;
			}
			PhysicalOperator physicalOperator2 = globalSuccessors.get(0);
			if (!(physicalOperator2 instanceof POPackage)) {
				// Global rearrange should be followed by Package
				return;
			}
			POPackage packageOp = (POPackage) physicalOperator2;
			List<PhysicalOperator> packSuccessors = plan
					.getSuccessors(packageOp);
			if (packSuccessors == null || packSuccessors.size() != 1) {
				return;
			}
			PhysicalOperator successor = packSuccessors.get(0);

			if (successor instanceof POLimit) {
				// POLimit is acceptable, as long has it has a single foreach as
				// successor
				List<PhysicalOperator> limitSucs = plan
						.getSuccessors(successor);
				if (limitSucs != null && limitSucs.size() == 1
						&& limitSucs.get(0) instanceof POForEach) {
					// the code below will now further examine the foreach
					successor = limitSucs.get(0);
				}
			}

			if (successor instanceof POForEach) {
				POForEach foreach = (POForEach) successor;
				List<PhysicalPlan> feInners = foreach.getInputPlans();
				// LOG.info(feInners);
				// find algebraic operators and also check if the foreach
				// statement
				// is suitable for combiner use
				List<Pair<PhysicalOperator, PhysicalPlan>> algebraicOps = findAlgebraicOps(feInners);
				if (algebraicOps == null || algebraicOps.size() == 0) {
					// the plan is not combinable or there is nothing to combine
					// we're done
					return;
				}

				for (PhysicalPlan iplan : feInners) {
					LOG.info(iplan);
				}

				try {
					// replace PODistinct->Project[*] with distinct udf (which
					// is Algebraic)
					for (Pair<PhysicalOperator, PhysicalPlan> op2plan : algebraicOps) {
						if (!(op2plan.first instanceof PODistinct)) {
							continue;
						}
						DistinctPatcher distinctPatcher = new DistinctPatcher(
								op2plan.second);
						distinctPatcher.visit();
						if (distinctPatcher.getDistinct() == null) {
							int errCode = 2073;
							String msg = "Problem with replacing distinct operator with distinct built-in function.";
							throw new PlanException(msg, errCode,
									PigException.BUG);
						}
						op2plan.first = distinctPatcher.getDistinct();
					}

					// create new map foreach
					POForEach mfe = createForEachWithGrpProj(foreach,
							localRearrange.getKeyType());
					Map<PhysicalOperator, Integer> op2newpos = Maps
							.newHashMap();
					Integer pos = 1;
					// create plan for each algebraic udf and add as inner plan
					// in map-foreach
					for (Pair<PhysicalOperator, PhysicalPlan> op2plan : algebraicOps) {
						PhysicalPlan udfPlan = createPlanWithPredecessors(
								op2plan.first, op2plan.second);
						mfe.addInputPlan(udfPlan, false);
						op2newpos.put(op2plan.first, pos++);
					}
					changeFunc(mfe, POUserFunc.INITIAL);

					// since we will only be creating SingleTupleBag as input to
					// the map foreach, we should flag the POProjects in the map
					// foreach inner plans to also use SingleTupleBag
					for (PhysicalPlan mpl : mfe.getInputPlans()) {
						try {
							new fixMapProjects(mpl).visit();
						} catch (VisitorException e) {
							int errCode = 2089;
							String msg = "Unable to flag project operator to use single tuple bag.";
							throw new PlanException(msg, errCode,
									PigException.BUG, e);
						}
					}

					// create new combine foreach
					POForEach cfe = createForEachWithGrpProj(foreach,
							localRearrange.getKeyType());
					// add algebraic functions with appropriate projection
					addAlgebraicFuncToCombineFE(cfe, op2newpos);
					changeFunc(cfe, POUserFunc.INTERMEDIATE);

					// fix projection and function time for algebraic functions
					// in reduce foreach
					for (Pair<PhysicalOperator, PhysicalPlan> op2plan : algebraicOps) {
						setProjectInput(op2plan.first, op2plan.second,
								op2newpos.get(op2plan.first));
						((POUserFunc) op2plan.first)
								.setAlgebraicFunction(POUserFunc.FINAL);
					}

					// we have modified the foreach inner plans - so set them
					// again
					// for the foreach so that foreach can do any
					// re-initialization
					// around them.
					// FIXME - this is a necessary evil right now because the
					// leaves
					// are explicitly stored in the POForeach as a list rather
					// than
					// computed each time at run time from the plans for
					// optimization. Do we want to have the Foreach compute the
					// leaves each time and have Java optimize it (will Java
					// optimize?)?
					mfe.setInputPlans(mfe.getInputPlans());
					cfe.setInputPlans(cfe.getInputPlans());
					foreach.setInputPlans(foreach.getInputPlans());

					// tell POCombinerPackage which fields need projected and
					// which
					// placed in bags. First field is simple project rest need
					// to go
					// into bags
					int numFields = algebraicOps.size() + 1; // algebraic funcs
																// + group key
					boolean[] bags = new boolean[numFields];
					bags[0] = false;
					for (int i = 1; i < numFields; i++) {
						bags[i] = true;
					}

					LOG.info(mfe);
					for (PhysicalPlan iplan : mfe.getInputPlans()) {
						LOG.info(iplan);
					}

					LOG.info(cfe);
					for (PhysicalPlan iplan : cfe.getInputPlans()) {
						LOG.info(iplan);
					}

					LOG.info(foreach);
					for (PhysicalPlan iplan : foreach.getInputPlans()) {
						LOG.info(iplan);
					}

					POOptimizedGlobalRearrange poOptimizedGlobalRearrange = new POOptimizedGlobalRearrange(
							globalRearrange, mfe, cfe, foreach);
					plan.replace(globalRearrange, poOptimizedGlobalRearrange);


					POLocalRearrange mlr = getNewRearrange(localRearrange);
					// POPartialAgg mapAgg = null;
					// if (doMapAgg) {
					// mapAgg = createPartialAgg(cfe);
					// }
					//

					patchUpMap(plan, getPreCombinerLR(localRearrange), mfe,
							null, mlr, localRearrange,
							poOptimizedGlobalRearrange);
					
					plan.removeAndReconnect(packageOp);
					plan.removeAndReconnect(foreach);
					
				} catch (Exception e) {
					int errCode = 2018;
					String msg = "Internal error. Unable to introduce the combiner for optimization.";
					throw new OptimizerException(msg, errCode,
							PigException.BUG, e);
				}

				// globalRearrange..setP.set(foreach.getInputPlans());

			}
		}
	}

	protected static void patchUpMap(PhysicalPlan plan,
			POPreCombinerLocalRearrange preCombinerLR, POForEach mfe,
			POPartialAgg mapAgg, POLocalRearrange mlr, POLocalRearrange oldLR,
			POOptimizedGlobalRearrange optimizedGR) throws PlanException {

		plan.add(preCombinerLR);
		plan.add(mfe);
		plan.add(mlr);

		
		plan.doInsertBetween(oldLR, preCombinerLR, optimizedGR, true);
		plan.doInsertBetween(preCombinerLR, mfe, optimizedGR, true);
		plan.doInsertBetween(mfe, mlr, optimizedGR, true);
		plan.removeAndReconnect(oldLR);
		//plan.replace(oldLR, preCombinerLR);

//		mapPlan.add(mfe);
//		mapPlan.connect(preCombinerLR, mfe);
//
//		// the operator before local rearrange
//		PhysicalOperator opBeforeLR = mfe;
//
//		if (mapAgg != null) {
//			mapPlan.add(mapAgg);
//			mapPlan.connect(mfe, mapAgg);
//			opBeforeLR = mapAgg;
//		}
//
//		mapPlan.add(mlr);
//		mapPlan.connect(opBeforeLR, mlr);
	}

}
