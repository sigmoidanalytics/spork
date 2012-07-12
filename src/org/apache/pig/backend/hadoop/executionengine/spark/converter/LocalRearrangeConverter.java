package org.apache.pig.backend.hadoop.executionengine.spark.converter;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
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

    
    @Override
    public RDD<Tuple> convert(List<RDD<Tuple>> predecessors, POLocalRearrange physicalOperator)
            throws IOException {
        SparkUtil.assertPredecessorSize(predecessors, physicalOperator, 1);
        RDD<Tuple> rdd = predecessors.get(0);
        return rdd
        // call local rearrange to get key and value
        .map(new LocalRearrangeFunction(physicalOperator), SparkUtil.getManifest(Tuple.class));
        
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

}
