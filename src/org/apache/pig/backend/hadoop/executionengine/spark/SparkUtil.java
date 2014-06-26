package org.apache.pig.backend.hadoop.executionengine.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POGlobalRearrange;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.UDFContext;

import scala.Tuple2;
import scala.Product2;
import scala.collection.JavaConversions;
import scala.collection.Seq;
//import scala.reflect.ClassManifest;
//import scala.reflect.ClassManifest$;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import org.apache.spark.rdd.RDD;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.List;
import java.util.Properties;

/**
 * @author billg
 */
public class SparkUtil {

    public static <T> ClassTag<T> getManifest(Class<T> clazz) {
        return ClassTag$.MODULE$.apply(clazz);
    }

    @SuppressWarnings("unchecked")
    public static <K,V> ClassTag<Tuple2<K, V>> getTuple2Manifest() {
        return (ClassTag<Tuple2<K, V>>)(Object)getManifest(Tuple2.class);
    }
    
    @SuppressWarnings("unchecked")
    public static <K,V> ClassTag<Product2<K, V>> getProduct2Manifest() {
        return (ClassTag<Product2<K, V>>)(Object)getManifest(Product2.class);
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

    public static void assertPredecessorSize(List<RDD<Tuple>> predecessors,
                                             PhysicalOperator physicalOperator, int size) {
        if (predecessors.size() != size) {
            throw new RuntimeException("Should have " + size + " predecessors for " +
                    physicalOperator.getClass() + ". Got : " + predecessors.size());
        }
    }

    public static void assertPredecessorSizeGreaterThan(List<RDD<Tuple>> predecessors,
                                             PhysicalOperator physicalOperator, int size) {
        if (predecessors.size() <= size) {
            throw new RuntimeException("Should have greater than" + size + " predecessors for " +
                    physicalOperator.getClass() + ". Got : " + predecessors.size());
        }
    }

    public static  int getParallelism(List<RDD<Tuple>> predecessors, PhysicalOperator physicalOperator) {
        int parallelism = physicalOperator.getRequestedParallelism();
        if (parallelism <= 0) {
            // Parallelism wasn't set in Pig, so set it to whatever Spark thinks is reasonable.
            parallelism = predecessors.get(0).context().defaultParallelism();
        }
        return parallelism;
    }

    public static void saveObject(Serializable obj, String refName) throws IOException {
    	if (obj != null) {
	    	Configuration confF = new Configuration();
	        confF.addResource(new Path(System.getenv("HADOOP_CONF_DIR") + "/core-site.xml"));
	        confF.addResource(new Path(System.getenv("HADOOP_CONF_DIR") + "/hdfs-site.xml"));
	        
	        FileSystem fileSystem = FileSystem.get(confF);
	        
	        // Check if the file already exists
	        Path pathF = new Path("hdfs://localhost:9000/tmp/props/"+refName);
	        if (fileSystem.exists(pathF)) {
	        	fileSystem.delete(pathF, true);            
	        }

	        // Create a new file and write data to it.
	        FSDataOutputStream outF = fileSystem.create(pathF);
	        outF.writeBytes(ObjectSerializer.serialize(obj));
	        outF.close();
    	}
    }
    
    public static Serializable readObject(String refName) throws IOException {

		Configuration confF = new Configuration();
        confF.addResource(new Path(System.getenv("HADOOP_CONF_DIR") + "/core-site.xml"));
        confF.addResource(new Path(System.getenv("HADOOP_CONF_DIR") + "/hdfs-site.xml"));
        
		Path pt=new Path("hdfs://localhost:9000/tmp/props/"+refName);
        FileSystem fileSystem = FileSystem.get(pt.toUri(), confF);
        
		BufferedReader br=new BufferedReader(new InputStreamReader(fileSystem.open(pt)));		
		Serializable obj = (Serializable) ObjectSerializer.deserialize(br.readLine());
		
		return obj;
    }
}
