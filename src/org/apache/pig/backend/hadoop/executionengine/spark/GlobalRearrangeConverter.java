package org.apache.pig.backend.hadoop.executionengine.spark;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POGlobalRearrange;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.POConverter;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.runtime.AbstractFunction1;
import spark.RDD;

public class GlobalRearrangeConverter implements POConverter<Tuple, Tuple, POGlobalRearrange> {

    private static final Log LOG = LogFactory.getLog(POGlobalRearrange.class);
    
    private static final GetKeyFunction GET_KEY_FUNCTION = new GetKeyFunction();
    private static final GroupTupleFunction GROUP_TUPLE_FUNCTION = new GroupTupleFunction();
    
    @Override
    public RDD<Tuple> convert(List<RDD<Tuple>> predecessors,
            POGlobalRearrange physicalOperator) throws IOException {
        if (predecessors.size()<1) {
            throw new RuntimeException("Should not have at least 1 predecessor for GlobalRearrange. Got : "+predecessors);
        }
        int parallelism = physicalOperator.getRequestedParallelism();
        if (parallelism <= 0) {
            // Parallelism wasn't set in Pig, so set it to whatever Spark thinks is reasonable.
            parallelism = predecessors.get(0).context().defaultParallelism();
        }
        LOG.info("Parallelism for Spark groupBy: " + parallelism);
        return predecessors.get(0)
                // group by key
                .groupBy(GET_KEY_FUNCTION, parallelism, SparkUtil.getManifest(Object.class))
                // convert result to a tuple (key, { values })
                .map(GROUP_TUPLE_FUNCTION, SparkUtil.getManifest(Tuple.class));
    }
    
    private static class GetKeyFunction extends AbstractFunction1<Tuple, Object> implements Serializable {

        public Object apply(Tuple t) {
            try {
                // see PigGenericMapReduce For the key
                Object key = t.get(0);
                return key;
            } catch (ExecException e) {
                throw new RuntimeException(e);
            }
        }
    }
    
    private static class GroupTupleFunction extends AbstractFunction1<Tuple2<Object, Seq<Tuple>>, Tuple> implements Serializable {

        private static BagFactory bf = BagFactory.getInstance();
        private static TupleFactory tf = TupleFactory.getInstance();        

        public Tuple apply(Tuple2<Object, Seq<Tuple>> v1) {
            try {
                DataBag bag = bf.newDefaultBag();
                Seq<Tuple> gp = v1._2();
                Iterable<Tuple> asJavaIterable = JavaConversions.asJavaIterable(gp);
                for (Tuple tuple : asJavaIterable) {
                    // keeping only the value
                    bag.add((Tuple)tuple.get(1));
                }
                Tuple tuple = tf.newTuple(2);
                tuple.set(0, v1._1()); // the key
                tuple.set(1, bag);
                return tuple;
            } catch (ExecException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
