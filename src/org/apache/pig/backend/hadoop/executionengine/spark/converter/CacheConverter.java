package org.apache.pig.backend.hadoop.executionengine.spark.converter;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POCache;
import org.apache.pig.data.Tuple;

import org.apache.spark.rdd.RDD;

public class CacheConverter implements POConverter<Tuple, Tuple, POCache> {

    private static final Log LOG = LogFactory.getLog(CacheConverter.class);

    private Map<String, RDD<Tuple>> cachedRdds = new HashMap<String, RDD<Tuple>>();

    @Override
    public RDD<Tuple> convert(List<RDD<Tuple>> predecessors, POCache physicalOperator) throws IOException {
        String key = physicalOperator.computeCacheKey();
        if (key != null) {
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
}
