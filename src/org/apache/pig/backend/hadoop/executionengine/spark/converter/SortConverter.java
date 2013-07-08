package org.apache.pig.backend.hadoop.executionengine.spark.converter;

import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSort;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import org.apache.pig.data.Tuple;

import scala.Tuple2;
//import scala.collection.Iterator;
import scala.collection.JavaConversions;
import scala.math.Ordered;
import scala.runtime.AbstractFunction1;

import spark.api.java.JavaRDD;
import spark.api.java.JavaPairRDD;
import spark.api.java.function.FlatMapFunction;
import spark.RDD;

@SuppressWarnings("serial")
public class SortConverter implements POConverter<Tuple, Tuple, POSort> {
    private static final Log LOG = LogFactory.getLog(SortConverter.class);

    private static final FlatMapFunction<Iterator<Tuple2<Tuple, Object>>, Tuple> TO_VALUE_FUNCTION = new ToValueFunction();

    @Override
    public RDD<Tuple> convert(List<RDD<Tuple>> predecessors, POSort sortOperator)
            throws IOException {
        SparkUtil.assertPredecessorSize(predecessors, sortOperator, 1);
        RDD<Tuple> rdd = predecessors.get(0);
        RDD<Tuple2<Tuple, Object>> rddPair =
                rdd.map(new ToKeyValueFunction(),
                        SparkUtil.<Tuple, Object>getTuple2Manifest());

        JavaPairRDD<Tuple, Object> r = new JavaPairRDD<Tuple, Object>(rddPair, SparkUtil.getManifest(Tuple.class),
                                                        SparkUtil.getManifest(Object.class));

        JavaPairRDD<Tuple, Object> sorted = r.sortByKey(sortOperator.getmComparator(), true);
        JavaRDD<Tuple> mapped = sorted.mapPartitions(TO_VALUE_FUNCTION);

        return mapped.rdd();
    }

    private static class ToValueFunction extends FlatMapFunction<Iterator<Tuple2<Tuple, Object>>, Tuple> implements Serializable {

        private class Tuple2TransformIterable implements Iterable<Tuple> {

          Iterator<Tuple2<Tuple, Object>> in;

          Tuple2TransformIterable(Iterator<Tuple2<Tuple, Object>> input) {
            in = input;
          }

          public Iterator<Tuple> iterator() {
            return new IteratorTransform<Tuple2<Tuple, Object>, Tuple>(in) {
              @Override
              protected Tuple transform(Tuple2<Tuple, Object> next) {
                return next._1();
              }
            };
          }
        }

        @Override
        public Iterable<Tuple> call(Iterator<Tuple2<Tuple, Object>> input) {
            return new Tuple2TransformIterable(input);
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
