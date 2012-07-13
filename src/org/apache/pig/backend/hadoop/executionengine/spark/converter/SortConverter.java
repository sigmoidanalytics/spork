package org.apache.pig.backend.hadoop.executionengine.spark.converter;

import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSort;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import org.apache.pig.data.Tuple;

import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConversions;
import scala.math.Ordered;
import scala.runtime.AbstractFunction1;
import spark.OrderedRDDFunctions;
import spark.RDD;

@SuppressWarnings("serial")
public class SortConverter implements POConverter<Tuple, Tuple, POSort> {
    private static final Log LOG = LogFactory.getLog(SortConverter.class);

    private static final ToValueFuction TO_VALUE_FUCTION = new ToValueFuction();

    @Override
    public RDD<Tuple> convert(List<RDD<Tuple>> predecessors, POSort sortOperator)
            throws IOException {
        SparkUtil.assertPredecessorSize(predecessors, sortOperator, 1);
        RDD<Tuple> rdd = predecessors.get(0);
        RDD<Tuple2<Tuple, Object>> rddPair =
                rdd.map(new ToKeyValueFunction(),
                        SparkUtil.<Tuple, Object>getTuple2Manifest());
        RDD<Tuple2<Tuple, Object>> sorted =
                new OrderedRDDFunctions<Tuple, Object>(
                        rddPair,
                        new SortFunction(sortOperator.getmComparator()),
                        SparkUtil.getManifest(Tuple.class),
                        SparkUtil.getManifest(Object.class)
                ).sortByKey(true);
        // problem
        // result is sorted but mapping the output shuffles it ... :(
        return sorted.mapPartitions(TO_VALUE_FUCTION, SparkUtil.getManifest(Tuple.class));
    }

    private static class SortFunction extends AbstractFunction1<Tuple, Ordered<Tuple>> implements Serializable {
        private final Comparator<Tuple> comparator;

        public SortFunction(Comparator<Tuple> comparator) {
            this.comparator = comparator;
        }

        @Override
        public Ordered<Tuple> apply(Tuple tuple) {
            return new OrderedTuple(tuple, comparator);
        }

    }

    private static class ToValueFuction extends AbstractFunction1<Iterator<Tuple2<Tuple, Object>>, Iterator<Tuple>> implements Serializable {
        @Override
        public Iterator<Tuple> apply(Iterator<Tuple2<Tuple, Object>> input) {
            return JavaConversions.asScalaIterator(new IteratorTransform<Tuple2<Tuple, Object>, Tuple>(JavaConversions.asJavaIterator(input)) {
                @Override
                protected Tuple transform(Tuple2<Tuple, Object> next) {
                    return next._1();
                }
            });
        }
    }

    private static class OrderedTuple implements Ordered<Tuple>, Serializable {
        private final Tuple tuple;
        private final Comparator<Tuple> comparator;

        public OrderedTuple(Tuple tuple, Comparator<Tuple> comparator) {
            this.tuple = tuple;
            this.comparator = comparator;
        }

        @Override
        public boolean $greater(Tuple o) {
            return compareTo(o) > 0;
        }

        @Override
        public boolean $greater$eq(Tuple o) {
            return compareTo(o) >= 0;
        }

        @Override
        public boolean $less(Tuple o) {
            return compareTo(o) < 0;
        }

        @Override
        public boolean $less$eq(Tuple o) {
            return compareTo(o) <= 0;
        }

        @Override
        public int compare(Tuple o) {
            return compareTo(o);
        }

        @Override
        public int compareTo(Tuple o) {
            return comparator.compare(tuple, o);
        }

    }

    private static class ToKeyValueFunction extends AbstractFunction1<Tuple,Tuple2<Tuple, Object>> implements Serializable {

        @Override
        public Tuple2<Tuple, Object> apply(Tuple t) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Sort ToKeyValueFunction in "+t);
            }
            Tuple key = t;
            Object value = null;
            // (key, value)
            Tuple2<Tuple, Object> out = new Tuple2<Tuple, Object>(key, value);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Sort ToKeyValueFunction out "+out);
            }
            return out;
        }
    }

}
