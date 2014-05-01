package org.apache.pig.backend.hadoop.executionengine.spark_streaming.converter;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.PODistinct;
import org.apache.pig.backend.hadoop.executionengine.spark_streaming.SparkUtil;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;

import scala.Function1;
import scala.Function2;
import scala.Tuple2;
import scala.reflect.ClassManifest;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.PairRDDFunctions;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.DStream;
import org.apache.spark.streaming.api.java.JavaDStream;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLimit;
import org.apache.pig.data.Tuple;

import scala.collection.Iterator;
import scala.collection.JavaConversions;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;

@SuppressWarnings({ "serial"})
public class LimitConverter implements POConverter<Tuple, Tuple, POLimit> {
	@Override
	public JavaDStream<Tuple> convert(List<JavaDStream<Tuple>> predecessors,
			POLimit poLimit) throws IOException {
        SparkUtil.assertPredecessorSize(predecessors, poLimit, 1);
        JavaDStream<Tuple> rdd = predecessors.get(0);
        LimitFunction limitFunction = new LimitFunction(poLimit);
        return new JavaDStream<Tuple>(rdd.dstream().mapPartitions(limitFunction, false, SparkUtil.getManifest(Tuple.class)), SparkUtil.getManifest(Tuple.class));
    }

    private static class LimitFunction extends Function<Iterator<Tuple>, Iterator<Tuple>> implements Serializable {

        private final POLimit poLimit;

        public LimitFunction(POLimit poLimit) {
            this.poLimit = poLimit;
        }

        @Override
        public Iterator<Tuple> call(Iterator<Tuple> i) {
            final java.util.Iterator<Tuple> tuples = JavaConversions.asJavaIterator(i);

            return JavaConversions.asScalaIterator(new POOutputConsumerIterator(tuples) {

                protected void attach(Tuple tuple) {
                    poLimit.setInputs(null);
                    poLimit.attachInput(tuple);
                }

                protected Result getNextResult() throws ExecException {
                    return poLimit.getNextTuple();
                }
            });
        }

    }

	
}