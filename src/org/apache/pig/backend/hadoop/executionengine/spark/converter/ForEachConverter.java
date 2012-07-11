package org.apache.pig.backend.hadoop.executionengine.spark.converter;

import java.io.Serializable;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import org.apache.pig.data.Tuple;

import scala.collection.Iterator;
import scala.collection.JavaConversions;
import scala.runtime.AbstractFunction1;
import spark.RDD;

/**
 * Convert that is able to convert an RRD to another RRD using a POForEach
 * @author billg
 */
public class ForEachConverter implements POConverter<Tuple, Tuple, POForEach> {

    @Override
    public RDD<Tuple> convert(RDD<Tuple> rdd, POForEach physicalOperator) {
        ForEachFunction forEachFunction = new ForEachFunction(physicalOperator);
        return rdd.mapPartitions(forEachFunction, SparkUtil.getManifest(Tuple.class));
    }

    private static class ForEachFunction extends AbstractFunction1<Iterator<Tuple>, Iterator<Tuple>>
            implements Serializable {

        private POForEach poForEach;

        private ForEachFunction(POForEach poForEach) {
            this.poForEach = poForEach;
        }

        public Iterator<Tuple> apply(Iterator<Tuple> i) {
            final java.util.Iterator<Tuple> input = JavaConversions.asJavaIterator(i);
            Iterator<Tuple> output = JavaConversions.asScalaIterator(new java.util.Iterator<Tuple>() {

                private Result result = null;
                private boolean returned = true;
                private boolean finished = false;

                private void readNext() {
                    try {
                        if (result != null && !returned) {
                            return;
                        }
                        // see PigGenericMapBase
                        if (result == null) {
                            Tuple v1 = input.next();
                            poForEach.setInputs(null);
                            poForEach.attachInput(v1);
                        }
                        result = poForEach.getNext((Tuple)null);
                        returned = false;
                        switch (result.returnStatus) {
                        case POStatus.STATUS_OK:
                            returned = false;
                            break;
                        case POStatus.STATUS_NULL:
                            returned = true; // skip: see PigGenericMapBase
                            readNext();
                            break;
                        case POStatus.STATUS_EOP:
                            finished = !input.hasNext();
                            if (!finished) {
                                result = null;
                                readNext();
                            }
                            break;
                        case POStatus.STATUS_ERR:
                            throw new RuntimeException("Error while processing "+result);
                        }
                    } catch (ExecException e) {
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public boolean hasNext() {
                    readNext();
                    return !finished;
                }

                @Override
                public Tuple next() {
                    readNext();
                    if (finished) {
                        throw new RuntimeException("Passed the end. call hasNext() first");
                    }
                    if (result == null || result.returnStatus!=POStatus.STATUS_OK) {
                        throw new RuntimeException("Unexpected response code in ForEach: " + result);
                    }
                    returned = true;
                    return (Tuple)result.result;
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            });
            return output;
        }
    }
}
