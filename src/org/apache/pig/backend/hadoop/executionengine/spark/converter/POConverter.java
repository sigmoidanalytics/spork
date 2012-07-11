package org.apache.pig.backend.hadoop.executionengine.spark.converter;

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import spark.RDD;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

/**
 * Given an RDD and a PhysicalOperater, and implementation of this class can convert the RRD to
 * another RRD.
 *
 * @author billg
 */
public interface POConverter<IN, OUT, T extends PhysicalOperator> extends Serializable {
    RDD<OUT> convert(List<RDD<IN>> rdd, T physicalOperator) throws IOException;
}
