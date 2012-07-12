package org.apache.pig.backend.hadoop.executionengine.spark.converter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PlanPrinter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POCache;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFilter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.VisitorException;

import spark.RDD;

import com.google.common.annotations.VisibleForTesting;

@SuppressWarnings("serial")
public class CacheConverter implements POConverter<Tuple, Tuple, POCache> {

    private static final Log LOG = LogFactory.getLog(CacheConverter.class);

    private Map<String, RDD<Tuple>> cachedRdds = new HashMap<String, RDD<Tuple>>();

    @Override
    public RDD<Tuple> convert(List<RDD<Tuple>> predecessors, POCache physicalOperator) throws IOException {
        throw new IOException("Don't use. Janking while waiting for BillG.");
    }

    public RDD<Tuple> convert(List<RDD<Tuple>> predecessors, POCache physicalOperator, PhysicalPlan plan) throws IOException {
        String key = computeCacheKey(plan, physicalOperator.getInputs());
        if (key != null) {
            // TODO deal with collisions!!
            key = String.valueOf(key.hashCode());
            if (cachedRdds.get(key) != null) {
                return cachedRdds.get(key);
            } else {
                RDD<Tuple> rdd = predecessors.get(0);
                rdd.cache();
                cachedRdds.put(key, rdd);
                return rdd;
            }
        } else {
            return predecessors.get(0);
        }
    }

    /**
     * Get a cache key for the given operator, or null if we don't know how to handle its type (or one of
     * its predcesessors' types) and want to not cache this subplan at all.
     *
     * Right now, this only handles loads. Unless we figure out a nice way to turn the PO plan into a
     * string or compare two PO plans, we'll probably have to handle each type of physical operator
     * recursively to generate a cache key.
     * @param plan
     * @throws IOException
     */
    @VisibleForTesting
    protected String computeCacheKey(PhysicalPlan plan, List<PhysicalOperator> preds) throws IOException {
        StringBuilder sb = new StringBuilder();
        for (PhysicalOperator operator : preds) {
            if (operator instanceof POLoad) {
                // Load operators are equivalent if the file is the same
                // and the loader is the same
                // Potential problems down the line:
                // * not checking LoadFunc arguments
                sb.append("LOAD: " + ((POLoad) operator).getLFile().getFileName()
                        + ((POLoad) operator).getLoadFunc().getClass().getName());
            } else if (operator instanceof POForEach) {
                // We consider ForEach operators to be equivalent if their inner plans
                // have the same explain plan after dropping scope markers.
                // Potential problems downstream:
                // * not checking for Nondeterministic UDFs
                // * jars / class defs changing under us
                StringBuilder foreachPlanKeysBuilder = new StringBuilder();
                for (PhysicalPlan innerPlan : ((POForEach) operator).getInputPlans()) {
                    foreachPlanKeysBuilder.append(innerPlanKey(innerPlan));
                }
                sb.append(foreachPlanKeysBuilder.toString());
                String inputKey = computeCacheKey(plan, operator.getInputs());
                if (inputKey == null) {
                    return null;
                } else {
                    sb.append(inputKey);
                    LOG.info("Input key: " + inputKey);
                }
            } else if (operator instanceof POFilter) {
                // Similar to foreach.
                PhysicalPlan innerPlan = ((POFilter) operator).getPlan();
                sb.append(innerPlanKey(innerPlan));
                String inputKey = computeCacheKey(plan, operator.getInputs());
                if (inputKey == null) {
                    return null;
                } else {
                    sb.append(inputKey);
                    LOG.info("Input key: " + inputKey);
                }
            } else {
                LOG.info("Don't know how to generate cache key for " + operator.getClass() + "; not caching");
                return null;
            }
        }
        return sb.toString();
    }

    private String innerPlanKey(PhysicalPlan plan) throws VisitorException, IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PlanPrinter<PhysicalOperator, PhysicalPlan> pp =
                new PlanPrinter<PhysicalOperator, PhysicalPlan>(plan);
        pp.print(baos);
        String explained = baos.toString();

        // get rid of scope numbers in these inner plans.
        return explained.replaceAll("scope-\\d+", "");
    }

}
