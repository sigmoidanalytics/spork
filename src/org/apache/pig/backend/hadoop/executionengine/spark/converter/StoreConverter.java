package org.apache.pig.backend.hadoop.executionengine.spark.converter;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobControlCompiler;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigOutputFormat;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.util.ObjectSerializer;
import org.python.google.common.collect.Lists;
import scala.Function1;
import scala.Tuple2;
import scala.runtime.AbstractFunction1;
import spark.PairRDDFunctions;
import spark.RDD;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;

/**
 * Converter that takes a POStore and stores it's content.
 *
 * @author billg
 */
public class StoreConverter implements POConverter<Tuple, Tuple2<Text, Tuple>, POStore> {

    private static final FromTupleFunction FROM_TUPLE_FUNCTION = new FromTupleFunction();

    private PigContext pigContext;

    public StoreConverter(PigContext pigContext) {
        this.pigContext = pigContext;
    }

    @Override
    public RDD<Tuple2<Text, Tuple>> convert(RDD<Tuple> rdd, POStore physicalOperator) throws IOException {
        // convert back to KV pairs
        RDD<Tuple2<Text, Tuple>> rddPairs =
                (RDD<Tuple2<Text, Tuple>>)((Object)rdd.map(FROM_TUPLE_FUNCTION, SparkUtil.getManifest(Tuple2.class)));
        PairRDDFunctions<Text, Tuple> pairRDDFunctions = new PairRDDFunctions<Text, Tuple>(rddPairs,
                SparkUtil.getManifest(Text.class), SparkUtil.getManifest(Tuple.class));

        JobConf storeJobConf = SparkUtil.newJobConf(pigContext);
        POStore poStore = configureStorer(storeJobConf, physicalOperator);

        pairRDDFunctions.saveAsNewAPIHadoopFile(poStore.getSFile().getFileName(),
                    Text.class, Tuple.class, PigOutputFormat.class, storeJobConf);

        return rddPairs;
    }

    private POStore configureStorer(JobConf jobConf,
            PhysicalOperator physicalOperator) throws IOException {
        ArrayList<POStore> storeLocations = Lists.newArrayList();
        POStore poStore = (POStore)physicalOperator;
        storeLocations.add(poStore);
        StoreFuncInterface sFunc = poStore.getStoreFunc();
        sFunc.setStoreLocation(poStore.getSFile().getFileName(), new org.apache.hadoop.mapreduce.Job(jobConf));
        poStore.setInputs(null);
        poStore.setParentPlan(null);

        jobConf.set(JobControlCompiler.PIG_MAP_STORES, ObjectSerializer.serialize(Lists.newArrayList()));
        jobConf.set(JobControlCompiler.PIG_REDUCE_STORES, ObjectSerializer.serialize(storeLocations));
        return poStore;
    }

    private static class FromTupleFunction extends AbstractFunction1<Tuple, Tuple2>
            implements Function1<Tuple, Tuple2>, Serializable {

        private static Text EMPTY_TEXT = new Text();

        public Tuple2 apply(Tuple v1) {
            return new Tuple2(EMPTY_TEXT, v1);
        }
    }
}
