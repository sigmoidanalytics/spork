package org.apache.pig.spark;

import static org.apache.pig.builtin.mock.Storage.tuple;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.apache.pig.ExecType;
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

        List<Tuple> output = data.get("output");
        assertEquals(
                Arrays.asList(tuple("test1"), tuple("test2")),
                output);
    }

    @Test
    public void testGroupBy() throws Exception {
        PigServer pigServer = new PigServer(ExecType.SPARK);
        Data data = Storage.resetData(pigServer);
        data.set("input", 
                tuple("test1"),
                tuple("test1"), 
                tuple("test2"));

        pigServer.registerQuery("A = LOAD 'input' using mock.Storage;");
        pigServer.registerQuery("B = GROUP A BY $0;");
        pigServer.registerQuery("STORE B INTO 'output' using mock.Storage;");

        List<Tuple> output = data.get("output");
        assertEquals(2, output.size());
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

        List<Tuple> output = data.get("output");
        assertEquals(
                Arrays.asList(tuple(2), tuple(1)),
                output);
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

        List<Tuple> output = data.get("output");
        assertEquals(
                Arrays.asList(tuple(1), tuple(2), tuple(3)),
                output);
    }
}
