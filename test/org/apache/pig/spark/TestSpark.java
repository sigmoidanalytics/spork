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
	public void testSparkMode() throws Exception {
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
}
