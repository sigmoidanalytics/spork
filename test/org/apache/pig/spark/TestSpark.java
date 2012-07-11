package org.apache.pig.spark;

import static org.apache.pig.builtin.mock.Storage.bag;
import static org.apache.pig.builtin.mock.Storage.tuple;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import javax.management.RuntimeErrorException;

import org.apache.pig.ExecType;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.builtin.mock.Storage;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.Tuple;
import org.apache.pig.pigunit.pig.PigServer;
import org.junit.Test;

public class TestSpark {

    @Test
    public void testLoadStore() throws Exception {
        PigServer pigServer = new PigServer(ExecType.SPARK);
        Data data = Storage.resetData(pigServer);
        data.set("input", 
                tuple("test1"), 
                tuple("test2"));
        pigServer.setBatchOn();
        pigServer.registerQuery("A = LOAD 'input' using mock.Storage;");
        pigServer.registerQuery("STORE A INTO 'output' using mock.Storage;");
        List<ExecJob> executeBatch = pigServer.executeBatch();
        // TODO: good stats
        //		assertEquals(1, executeBatch.size());
        //		assertTrue(executeBatch.get(0).hasCompleted());

        assertEquals(
                Arrays.asList(tuple("test1"), tuple("test2")),
                data.get("output"));
    }

    @Test
    public void testGroupBy() throws Exception {
        PigServer pigServer = new PigServer(ExecType.SPARK);
        Data data = Storage.resetData(pigServer);
        data.set("input", 
                tuple("foo", "key1", "test1"),
                tuple("bar", "key1", "test2"), 
                tuple("baz", "key2", "test3"));

        pigServer.registerQuery("A = LOAD 'input' using mock.Storage;");
        pigServer.registerQuery("B = GROUP A BY $1;");
        pigServer.registerQuery("STORE B INTO 'output' using mock.Storage;");

        assertEquals(
                Arrays.asList(
                        tuple("key1", bag(tuple("foo", "key1", "test1"), tuple("bar", "key1", "test2"))),
                        tuple("key2", bag(tuple("baz", "key2", "test3")))),
                sortByIndex(data.get("output"), 0));
    }

    private List<Tuple> sortByIndex(List<Tuple> out, final int i) {
        List<Tuple> result = new ArrayList<Tuple>(out);
        Collections.sort(result, new Comparator<Tuple>() {
            public int compare(Tuple o1, Tuple o2) {
                try {
                Comparable c1 = (Comparable)o1.get(i);
                Comparable c2 = (Comparable)o2.get(i);
                return c1.compareTo(c2);
                } catch (ExecException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        return result;
    }
    
    @Test
    public void testGroupByFlatten() throws Exception {
        PigServer pigServer = new PigServer(ExecType.SPARK);
        Data data = Storage.resetData(pigServer);
        data.set("input", 
                tuple("test1"),
                tuple("test1"), 
                tuple("test2"));

        pigServer.registerQuery("A = LOAD 'input' using mock.Storage;");
        pigServer.registerQuery("B = GROUP A BY $0;");
        pigServer.registerQuery("C = FOREACH B GENERATE FLATTEN(A);");
        pigServer.registerQuery("STORE C INTO 'output' using mock.Storage;");

        assertEquals(
                Arrays.asList(tuple("test1"), tuple("test1"), tuple("test2")),
                data.get("output"));
    }

    @Test
    public void testCount() throws Exception {
        PigServer pigServer = new PigServer(ExecType.SPARK);
        Data data = Storage.resetData(pigServer);
        data.set("input", 
                tuple("test1"),
                tuple("test1"), 
                tuple("test2"));

        pigServer.registerQuery("A = LOAD 'input' using mock.Storage;");
        pigServer.registerQuery("B = GROUP A BY $0;");
        pigServer.registerQuery("C = FOREACH B GENERATE COUNT(A);");
        pigServer.registerQuery("STORE B INTO 'output' using mock.Storage;");

        assertEquals(
                Arrays.asList(tuple(2), tuple(1)),
                data.get("output"));
    }

    @Test
    public void testForEach() throws Exception {
        PigServer pigServer = new PigServer(ExecType.SPARK);
        Data data = Storage.resetData(pigServer);
        data.set("input", 
                tuple("1"),
                tuple("12"), 
                tuple("123"));

        pigServer.registerQuery("A = LOAD 'input' using mock.Storage;");
        pigServer.registerQuery("B = FOREACH A GENERATE StringSize($0);");
        pigServer.registerQuery("STORE B INTO 'output' using mock.Storage;");

        assertEquals(
                Arrays.asList(tuple(1l), tuple(2l), tuple(3l)),
                data.get("output"));
    }

    
    @Test
    public void testForEachFlatten() throws Exception {
        PigServer pigServer = new PigServer(ExecType.SPARK);
        Data data = Storage.resetData(pigServer);
        data.set("input", 
                tuple(bag(tuple("1"), tuple("2"), tuple("3"))),
                tuple(bag(tuple("4"), tuple("5"), tuple("6"))));

        pigServer.registerQuery("A = LOAD 'input' using mock.Storage;");
        pigServer.registerQuery("B = FOREACH A GENERATE FLATTEN($0);");
        pigServer.registerQuery("STORE B INTO 'output' using mock.Storage;");

        assertEquals(
                Arrays.asList(tuple("1"), tuple("2"), tuple("3"), tuple("4"), tuple("5"), tuple("6")),
                data.get("output"));
    }
    
    @Test
    public void testFilter() throws Exception {
        PigServer pigServer = new PigServer(ExecType.SPARK);
        Data data = Storage.resetData(pigServer);
        data.set("input", 
                tuple("1"),
                tuple("2"), 
                tuple("3"), 
                tuple("1"));

        pigServer.registerQuery("A = LOAD 'input' using mock.Storage;");
        pigServer.registerQuery("B = FILTER A BY $0 == '1';");
        pigServer.registerQuery("STORE B INTO 'output' using mock.Storage;");

        assertEquals(
                Arrays.asList(tuple("1"), tuple("1")),
                data.get("output"));
    }
}
