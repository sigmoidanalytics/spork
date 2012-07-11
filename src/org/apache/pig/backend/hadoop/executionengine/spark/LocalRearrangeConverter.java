package org.apache.pig.backend.hadoop.executionengine.spark;

import java.io.IOException;
import java.io.Serializable;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
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

public class LocalRearrangeConverter implements POConverter<Tuple, Tuple, POLocalRearrange> {

    private static final GetKeyFunction GET_KEY_FUNCTION = new GetKeyFunction();
    private static final GroupTupleFunction GROUP_TUPLE_FUNCTION = new GroupTupleFunction();
    
    @Override
    public RDD<Tuple> convert(RDD<Tuple> rdd, POLocalRearrange physicalOperator)
            throws IOException {
        return rdd
        // call local rearrange to get key and value
        .map(new LocalRearrangeFunction(physicalOperator), SparkUtil.getManifest(Tuple.class))
        // group by key
        .groupBy(GET_KEY_FUNCTION, SparkUtil.getManifest(Object.class))
        // convert result to a tuple (key, { values })
        .map(GROUP_TUPLE_FUNCTION, SparkUtil.getManifest(Tuple.class));
    }
    
    private static class LocalRearrangeFunction extends AbstractFunction1<Tuple, Tuple> implements Serializable {
        
        private static TupleFactory tf = TupleFactory.getInstance();
        private final POLocalRearrange physicalOperator;

        public LocalRearrangeFunction(POLocalRearrange physicalOperator) {
            this.physicalOperator = physicalOperator;
        }

        @Override
        public Tuple apply(Tuple t) {
            Result result;
            try {
                physicalOperator.setInputs(null);
                physicalOperator.attachInput(t);
                result = physicalOperator.getNext((Tuple)null);

                if (result == null) {
                    throw new RuntimeException("Null response found for LocalRearange on tuple: " + t);
                }

                switch (result.returnStatus) {
                case POStatus.STATUS_OK:
                    // we don't need the index used for namespacing in Pig
                    Tuple resultTuple = (Tuple)result.result;
                    Tuple newTuple = tf.newTuple(2);
                    newTuple.set(0, resultTuple.get(1)); // key
                    newTuple.set(1, resultTuple.get(2)); // value (stripped of the key, reconstructed in package)
                    return newTuple;
                default:
                    throw new RuntimeException("Unexpected response code from operator "+physicalOperator+" : " + result);
                }
            } catch (ExecException e) {
                throw new RuntimeException("Couldn't do LocalRearange on tuple: " + t, e);
            }
        }

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
