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
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.data.Tuple;
import spark.RDD;

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
    private String computeCacheKey(PhysicalPlan plan, List<PhysicalOperator> preds) throws IOException {
        StringBuilder sb = new StringBuilder();
        for (PhysicalOperator operator : preds) {
            if (operator instanceof POLoad) {
                // Load operators are equivalent if the file is the same
                // and the loader is the same
                // Potential problems down the line:
                // * not checking LoadFunc arguments
                sb.append("LOAD: " + ((POLoad) operator).getLFile().getFileName()
                        + ((POLoad) operator).getLoadFunc());
            } else if (operator instanceof POForEach) {
                // We consider ForEach operators to be equivalent if their inner plans
                // have the same explain plan after dropping scope markers.
                // Potential problems downstream:
                // * not checking for Nondeterministic UDFs
                // * jars / class defs changing under us
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                for (PhysicalPlan innerPlan : ((POForEach) operator).getInputPlans()) {
                    PlanPrinter<PhysicalOperator, PhysicalPlan> pp =
                            new PlanPrinter<PhysicalOperator, PhysicalPlan>(innerPlan);
                    pp.print(baos);
                }
                String explained = baos.toString();

                // get rid of scope numbers in these inner plans.
                sb.append(explained.replaceAll("scope-\\d+", ""));
                sb.append(computeCacheKey(plan, operator.getInputs()));

            } else {
                LOG.info("Don't know how to generate cache key for " + operator.getClass() + "; not caching");
                return null;
            }
        }
        return sb.toString();
    }

}
