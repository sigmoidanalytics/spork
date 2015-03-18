package org.apache.pig.backend.hadoop.executionengine.spark.plans;

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POGlobalRearrange;
import org.apache.pig.data.DataType;

public class POOptimizedGlobalRearrange extends POGlobalRearrange {

	POGlobalRearrange rearrange;
	POForEach initForEach;
	POForEach intermediateForEach;
	POForEach finalForEach;

	public POOptimizedGlobalRearrange(
			POGlobalRearrange rearrange, POForEach init,
			POForEach intermediate, POForEach finale) {
		super(rearrange);
		this.rearrange = rearrange;
		this.initForEach = init;
		this.intermediateForEach = intermediate;
		this.finalForEach = finale;

	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public String name() {
		// TODO Auto-generated method stub
		return getAliasString() + "Optimized Global Rearrange" + "["
				+ DataType.findTypeName(resultType) + "]" + " - "
				+ mKey.toString();
	}

	public POGlobalRearrange getRearrange() {
		return rearrange;
	}

	public void setRearrange(POGlobalRearrange rearrange) {
		this.rearrange = rearrange;
	}

	public POForEach getInitForEach() {
		return initForEach;
	}

	public void setInitForEach(POForEach initForEach) {
		this.initForEach = initForEach;
	}

	public POForEach getIntermediateForEach() {
		return intermediateForEach;
	}

	public void setIntermediateForEach(POForEach intermediateForEach) {
		this.intermediateForEach = intermediateForEach;
	}

	public POForEach getFinalForEach() {
		return finalForEach;
	}

	public void setFinalForEach(POForEach finalForEach) {
		this.finalForEach = finalForEach;
	}
}
