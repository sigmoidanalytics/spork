package org.apache.pig.backend.hadoop.executionengine.spark;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POGlobalRearrange;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.POConverter;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.python.google.common.collect.Lists;

import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.reflect.ClassManifest;
import scala.runtime.AbstractFunction1;
import spark.PairRDDFunctions;
import spark.RDD;

public class GlobalRearrangeConverter implements POConverter<Tuple, Tuple, POGlobalRearrange> {
    private static final Log LOG = LogFactory.getLog(GlobalRearrangeConverter.class);
    
    public class ToGroupKeyValueFunction extends AbstractFunction1<Tuple2<Object,Tuple2<Seq<Tuple>,Seq<Tuple>>>, Tuple2<Object,Tuple>> implements Serializable {

        @Override
        public Tuple2<Object, Tuple> apply(
                Tuple2<Object, Tuple2<Seq<Tuple>, Seq<Tuple>>> tuple2) {
            try {
                LOG.debug("ToGroupKeyValueFunction in "+tuple2);
                Object key = tuple2._1();
                Tuple2<Seq<Tuple>, Seq<Tuple>> bags = tuple2._2();
                // I would call scala's .union(...) if it did not take a second argument
                List<Tuple> tuples = Lists.newArrayList();
                Collection<Tuple> bag1 = JavaConversions.asJavaCollection(bags._1());
                for (Tuple t1 : bag1) {
                    Tuple toadd = tf.newTuple(3);
                    toadd.set(0, 0);
                    toadd.set(1, key);
                    toadd.set(2, t1);
                    tuples.add(toadd);
                }
                Collection<Tuple> bag2 = JavaConversions.asJavaCollection(bags._2());
                for (Tuple t2 : bag2) {
                    Tuple toadd = tf.newTuple(3);
                    toadd.set(0, 1);
                    toadd.set(1, key);
                    toadd.set(2, t2);
                    tuples.add(toadd);
                }
                Tuple newTuple = tf.newTuple(1);
                newTuple.set(0, JavaConversions.asScalaBuffer(tuples));
                Tuple2<Object, Tuple> out = new Tuple2<Object, Tuple>(key, newTuple);
                LOG.debug("ToGroupKeyValueFunction out "+out);
                return out;
            } catch(ExecException e) {
                throw new RuntimeException(e);
            }
        }
    
    }

    private static BagFactory bf = BagFactory.getInstance();
    private static TupleFactory tf = TupleFactory.getInstance();

    public static class ToValueFunction extends AbstractFunction1<Tuple2<Object,Tuple>,Tuple> implements Serializable{

        @Override
        public Tuple apply(Tuple2<Object, Tuple> in) {
            try {
                LOG.debug("ToValueFunction (in) "+in);
                Tuple out = tf.newTuple(2);
                out.set(0, in._1);
                out.set(1, in._2.get(0));
                LOG.debug("ToValueFunction (out) "+out);
                return out;
            } catch (ExecException e) {
                throw new RuntimeException(e);
            }
        }
    
    }

    public static class ToKeyValueFunction extends AbstractFunction1<Tuple,Tuple2<Object, Tuple>> implements Serializable {

        @Override
        public Tuple2<Object, Tuple> apply(Tuple t) {
            try {
                // (index, key, value)
                LOG.debug("ToKeyValueFunction in "+t);
                Object key = t.get(1);
                Tuple value = (Tuple)t.get(2); //value
                // (key, (index, value))
                Tuple2<Object, Tuple> out = new Tuple2<Object, Tuple>(key, value);
                LOG.debug("ToKeyValueFunction out "+out);
                return out;
            } catch (ExecException e) {
                throw new RuntimeException(e);
            }
        }
    
    }

    private static final GetKeyFunction GET_KEY_FUNCTION = new GetKeyFunction();
    private static final GroupTupleFunction GROUP_TUPLE_FUNCTION = new GroupTupleFunction();
    
    @Override
    public RDD<Tuple> convert(List<RDD<Tuple>> predecessors,
            POGlobalRearrange physicalOperator) throws IOException {
        if (predecessors.size()<1) {
            throw new RuntimeException("Should not have at least 1 predecessor for GlobalRearrange. Got : "+predecessors);
        }
        if (predecessors.size() == 1) {
            //GROUP
            return predecessors.get(0)
                // group by key
                .groupBy(GET_KEY_FUNCTION, SparkUtil.getManifest(Object.class))
                // convert result to a tuple (key, { values })
                .map(GROUP_TUPLE_FUNCTION, SparkUtil.getManifest(Tuple.class));
        } else {
          //COGROUP
            // each pred returns (index, key, value)
            Iterator<RDD<Tuple>> iterator = predecessors.iterator();
            ClassManifest<Tuple2<Object, Tuple>> tuple2ClassManifest = (ClassManifest<Tuple2<Object, Tuple>>)(Object)SparkUtil.getManifest(Tuple2.class);
            
            RDD<Tuple2<Object, Tuple>> rddPairs = iterator.next().map(new ToKeyValueFunction(), tuple2ClassManifest);
            while(iterator.hasNext()) {
                PairRDDFunctions<Object, Tuple> rdd = 
                        new PairRDDFunctions<Object, Tuple>(
                                rddPairs,
                                SparkUtil.getManifest(Object.class), 
                                SparkUtil.getManifest(Tuple.class));
                rddPairs = rdd
                        .cogroup(
                                iterator.next().map(new ToKeyValueFunction(), tuple2ClassManifest))
                        .map(new ToGroupKeyValueFunction(), tuple2ClassManifest);
            }
            return rddPairs.map(new ToValueFunction(),  SparkUtil.getManifest(Tuple.class));
        }
    }
    
    private static class GetKeyFunction extends AbstractFunction1<Tuple, Object> implements Serializable {

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
    
    private static class GroupTupleFunction extends AbstractFunction1<Tuple2<Object, Seq<Tuple>>, Tuple> implements Serializable {

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
}
