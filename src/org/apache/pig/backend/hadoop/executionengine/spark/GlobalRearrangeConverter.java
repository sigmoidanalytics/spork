package org.apache.pig.backend.hadoop.executionengine.spark;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POGlobalRearrange;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.POConverter;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.python.google.common.collect.Lists;

import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.reflect.ClassManifest;
import scala.runtime.AbstractFunction1;
import spark.CoGroupedRDD;
import spark.HashPartitioner;
import spark.RDD;

public class GlobalRearrangeConverter implements POConverter<Tuple, Tuple, POGlobalRearrange> {
    private static final Log LOG = LogFactory.getLog(GlobalRearrangeConverter.class);

    private static final TupleFactory tf = TupleFactory.getInstance();

    // GROUP FUNCTIONS
    private static final ToKeyValueFunction TO_KEY_VALUE_FUNCTION = new ToKeyValueFunction();
    private static final GetKeyFunction GET_KEY_FUNCTION = new GetKeyFunction();
    // COGROUP FUNCTIONS
    private static final GroupTupleFunction GROUP_TUPLE_FUNCTION = new GroupTupleFunction();
    private static final ToGroupKeyValueFunction TO_GROUP_KEY_VALUE_FUNCTION = new ToGroupKeyValueFunction();

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
        if (predecessors.size() == 1) {
            //GROUP
            return predecessors.get(0)
                // group by key
                .groupBy(GET_KEY_FUNCTION, parallelism, SparkUtil.getManifest(Object.class))
                // convert result to a tuple (key, { values })
                .map(GROUP_TUPLE_FUNCTION, SparkUtil.getManifest(Tuple.class));
        } else {
            //COGROUP
            // each pred returns (index, key, value)
            ClassManifest<Tuple2<Object, Tuple>> tuple2ClassManifest = (ClassManifest<Tuple2<Object, Tuple>>)(Object)SparkUtil.getManifest(Tuple2.class);

            List<RDD<Tuple2<Object, Tuple>>> rddPairs = Lists.newArrayList();
            for (RDD<Tuple> rdd : predecessors) {
                RDD<Tuple2<Object, Tuple>> rddPair = rdd.map(TO_KEY_VALUE_FUNCTION, tuple2ClassManifest);
                rddPairs.add(rddPair);
            }

            // Something's wrong with the type parameters of CoGroupedRDD
            // key and value are the same type ???
            CoGroupedRDD<Object> coGroupedRDD = new CoGroupedRDD<Object>(
                    (Seq<RDD<Tuple2<?, ?>>>)(Object)JavaConversions.asScalaBuffer(rddPairs),
                    new HashPartitioner(1)); // TODO: set parallelism

            RDD<Tuple2<Object,Seq<Seq<Tuple>>>> rdd = (RDD<Tuple2<Object,Seq<Seq<Tuple>>>>)(Object)coGroupedRDD;
            return rdd.map(TO_GROUP_KEY_VALUE_FUNCTION,  SparkUtil.getManifest(Tuple.class));
        }
    }

    private static class GetKeyFunction
    extends AbstractFunction1<Tuple, Object>
    implements Serializable {

        public Object apply(Tuple t) {
            try {
                LOG.debug("GetKeyFunction in "+t);
                // see PigGenericMapReduce For the key
                Object key = t.get(1);
                LOG.debug("GetKeyFunction out "+key);
                return key;
            } catch (ExecException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static class GroupTupleFunction
    extends AbstractFunction1<Tuple2<Object, Seq<Tuple>>, Tuple>
    implements Serializable {

        public Tuple apply(Tuple2<Object, Seq<Tuple>> v1) {
            try {
                LOG.debug("GroupTupleFunction in "+v1);
                Tuple tuple = tf.newTuple(2);
                tuple.set(0, v1._1()); // the (index, key) tuple
                tuple.set(1, v1._2()); // the Seq<Tuple> aka bag of values
                LOG.debug("GroupTupleFunction out "+tuple);
                return tuple;
            } catch (ExecException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static class ToKeyValueFunction
    extends AbstractFunction1<Tuple,Tuple2<Object, Tuple>>
    implements Serializable {

        @Override
        public Tuple2<Object, Tuple> apply(Tuple t) {
            try {
                // (index, key, value)
                LOG.debug("ToKeyValueFunction in "+t);
                Object key = t.get(1);
                Tuple value = (Tuple)t.get(2); //value
                // (key, value)
                Tuple2<Object, Tuple> out = new Tuple2<Object, Tuple>(key, value);
                LOG.debug("ToKeyValueFunction out "+out);
                return out;
            } catch (ExecException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static class ToGroupKeyValueFunction
    extends AbstractFunction1<Tuple2<Object,Seq<Seq<Tuple>>>,Tuple>
    implements Serializable {

        @Override
        public Tuple apply(Tuple2<Object, Seq<Seq<Tuple>>> input) {
            try {
                LOG.debug("ToGroupKeyValueFunction2 in "+input);
                Object key = input._1();
                Seq<Seq<Tuple>> bags = input._2();
                Iterable<Seq<Tuple>> bagsList = JavaConversions.asJavaIterable(bags);
                int i = 0;
//                // I would call scala's .union(...) if it did not take a second argument
//                // TODO: improve this it would be better to wrap this without materializing the data
//                // all we need is an iterator
                List<Tuple> tuples = Lists.newArrayList();
                for (Seq<Tuple> bag : bagsList) {
                    for (Tuple t : JavaConversions.asJavaCollection(bag)) {
                        Tuple toadd = tf.newTuple(3);
                        toadd.set(0, i);
                        toadd.set(1, key);
                        toadd.set(2, t);
                        tuples.add(toadd);
                    }
                    ++i;
                }
                Tuple out = tf.newTuple(2);
                out.set(0, key);
                out.set(1, JavaConversions.asScalaBuffer(tuples));
                LOG.debug("ToGroupKeyValueFunction2 out "+out);
                return out;
            } catch(Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
