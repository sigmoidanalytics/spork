package org.apache.pig.backend.hadoop.executionengine.spark;

import java.util.UUID;

import org.apache.pig.backend.hadoop.executionengine.HExecutionEngine;
import org.apache.pig.impl.PigContext;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.ScriptState;

public class SparkExecutionEngine extends HExecutionEngine {

	public SparkExecutionEngine(PigContext pigContext) {
		super(pigContext);
		this.launcher = new SparkLauncher();
	}

	@Override
	public ScriptState instantiateScriptState() {
		//SparkScriptState ss = new SparkScriptState(UUID.randomUUID().toString());
		//ss.setPigContext(pigContext);
		return null;
	}

	@Override
	public PigStats instantiatePigStats() {
		return null;//new SimplePigStats();
	}
}
