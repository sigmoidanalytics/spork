package org.apache.pig.backend.hadoop.executionengine.spark;

import org.apache.hadoop.mapred.JobConf;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.UDFContext;

import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.reflect.ClassManifest;
import scala.reflect.ClassManifest$;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * @author billg
 */
public class SparkUtil {

    public static <T> ClassManifest<T> getManifest(Class<T> clazz) {
        return ClassManifest$.MODULE$.fromClass(clazz);
    }

    public static JobConf newJobConf(PigContext pigContext) throws IOException {
        JobConf jobConf = new JobConf(ConfigurationUtil.toConfiguration(pigContext.getProperties()));
        jobConf.set("pig.pigContext", ObjectSerializer.serialize(pigContext));
        UDFContext.getUDFContext().serialize(jobConf);
        jobConf.set("udf.import.list", ObjectSerializer.serialize(PigContext.getPackageImportList()));
        return jobConf;
    }
    
    public static <T> Seq<T> toScalaSeq(List<T> list) {
      return JavaConversions.asScalaBuffer(list);
    }
}
