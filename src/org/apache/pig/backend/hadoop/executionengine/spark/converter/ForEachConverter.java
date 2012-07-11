package org.apache.pig.backend.hadoop.executionengine.spark.converter;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import org.apache.pig.data.Tuple;
import scala.Function1;
import scala.runtime.AbstractFunction1;
import spark.RDD;

import java.io.Serializable;

/**
 * Convert that is able to convert an RRD to another RRD using a POForEach
 * @author billg
 */
public class ForEachConverter implements POConverter<Tuple, Tuple, POForEach> {

    @Override
    public RDD<Tuple> convert(RDD<Tuple> rdd, POForEach physicalOperator) {
        Function1 forEachFunction = new ForEachFunction(physicalOperator);
        return (RDD<Tuple>) rdd.map(forEachFunction, SparkUtil.getManifest(Tuple.class));
    }

    private static class ForEachFunction extends AbstractFunction1<Tuple, Tuple>
            implements Function1<Tuple, Tuple>, Serializable {

        private POForEach poForEach;

        private ForEachFunction(POForEach poForEach) {
            this.poForEach = poForEach;
        }

        public Tuple apply(Tuple v1) {
            Result result;
            try {
                poForEach.setInputs(null);
                poForEach.attachInput(v1);
                result = poForEach.getNext(v1);
            } catch (ExecException e) {
                throw new RuntimeException("Couldn't do forEach on tuple: " + v1, e);
            }

            if (result == null) {
                throw new RuntimeException("Null response found for forEach on tuple: " + v1);
            }

            switch (result.returnStatus) {
                case POStatus.STATUS_OK:
                    return (Tuple)result.result;
                default:
                    throw new RuntimeException("Unexpected response code from filter: " + result);
            }
        }
    }
}
