package org.apache.pig.backend.hadoop.executionengine.spark.converter;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFilter;
import org.apache.pig.data.Tuple;
import scala.Function1;
import scala.runtime.AbstractFunction1;
import spark.RDD;

import java.io.Serializable;
import java.util.List;

/**
 * Converter that converts an RDD to a filtered RRD using POFilter
 * @author billg
 */
public class FilterConverter implements POConverter<Tuple, Tuple, POFilter> {

    @Override
    public RDD<Tuple> convert(List<RDD<Tuple>> predecessors, POFilter physicalOperator) {
        if (predecessors.size()!=1) {
            throw new RuntimeException("Should not have 1 predecessors for Filter. Got : "+predecessors);
        }
        RDD<Tuple> rdd = predecessors.get(0);
        Function1 filterFunction = new FilterFunction(physicalOperator);
        return rdd.filter(filterFunction);
    }

    private static class FilterFunction extends AbstractFunction1<Tuple, Boolean>
            implements Function1<Tuple, Boolean>, Serializable {

        private POFilter poFilter;

        private FilterFunction(POFilter poFilter) {
            this.poFilter = poFilter;
        }

        public Boolean apply(Tuple v1) {
            Result result;
            try {
                poFilter.setInputs(null);
                poFilter.attachInput(v1);
                result = poFilter.getNext(v1);
            } catch (ExecException e) {
                throw new RuntimeException("Couldn't filter tuple", e);
            }

            if (result == null) { return false; }

            switch (result.returnStatus) {
                case POStatus.STATUS_OK:
                    return true;
                case POStatus.STATUS_EOP: // TODO: probably also ok for EOS, END_OF_BATCH
                    return false;
                default:
                    throw new RuntimeException("Unexpected response code from filter: " + result);
            }
        }
    }
}
