package org.apache.pig.backend.hadoop.executionengine.spark;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.POConverter;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.io.PigNullableWritable;

import scala.runtime.AbstractFunction1;
import spark.RDD;

public class PackageConverter implements POConverter<Tuple, Tuple, POPackage> {

    @Override
    public RDD<Tuple> convert(List<RDD<Tuple>> predecessors, POPackage physicalOperator)
            throws IOException {
        if (predecessors.size()!=1) {
            throw new RuntimeException("Should have 1 predecessor for Package. Got : "+predecessors);
        }
        RDD<Tuple> rdd = predecessors.get(0);
        // package will generate the group from the result of the local rearrange
        return rdd.map(new PackageFunction(physicalOperator), SparkUtil.getManifest(Tuple.class));
    }

    private static class PackageFunction extends AbstractFunction1<Tuple, Tuple> implements Serializable {

        private static TupleFactory tf = TupleFactory.getInstance();
        private final POPackage physicalOperator;

        public PackageFunction(POPackage physicalOperator) {
            this.physicalOperator = physicalOperator;
        }

        @Override
        public Tuple apply(final Tuple t) {
            Result result;
            try {
                PigNullableWritable key = new PigNullableWritable() {
                    public Object getValueAsPigType() {
                        try {
                            Object keyTuple = t.get(0);
                            return keyTuple;
                        } catch (ExecException e) {
                           throw new RuntimeException(e);
                        }
                    }
                };
                final Iterator<Tuple> bagIterator = ((DataBag)t.get(1)).iterator();
                Iterator<NullableTuple> iterator = new Iterator<NullableTuple>() {
                    public boolean hasNext() {
                        return bagIterator.hasNext();
                    }
                    public NullableTuple next() {
                        Tuple next = bagIterator.next();
                        
                        return new NullableTuple(next);
                    }
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
                physicalOperator.setInputs(null);
                physicalOperator.attachInput(key, iterator);
                result = physicalOperator.getNext((Tuple)null);
            } catch (ExecException e) {
                throw new RuntimeException("Couldn't do Package on tuple: " + t, e);
            }

            if (result == null) {
                throw new RuntimeException("Null response found for Package on tuple: " + t);
            }

            switch (result.returnStatus) {
                case POStatus.STATUS_OK:
                    // (key, {(value)...})
                return (Tuple)result.result;
                default:
                    throw new RuntimeException("Unexpected response code from operator "+physicalOperator+" : " + result);
            }
        }

    }

}
