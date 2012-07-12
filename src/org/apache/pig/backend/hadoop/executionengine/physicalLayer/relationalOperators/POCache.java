/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators;

import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.pen.util.ExampleTuple;

public class POCache extends PhysicalOperator {

    private static final long serialVersionUID = 1L;

    // Counts for outputs processed
    private long soFar = 0;

    // Number of limited outputs
    long mLimit;

    // The expression plan
    PhysicalPlan expressionPlan;

    public POCache(OperatorKey k) {
        this(k, null);
    }

    public POCache(OperatorKey k, List<PhysicalOperator> inputs) {
        super(k, -1, inputs);
    }

    /**
     * Counts the number of tuples processed into static variable soFar, if the number of tuples processed reach the
     * limit, return EOP; Otherwise, return the tuple
     */
    @Override
    public Result getNext(Tuple t) throws ExecException {
        return processInput();
    }

    @Override
    public String name() {
        return getAliasString() + "Cache - " + mKey.toString();
    }

    @Override
    public boolean supportsMultipleInputs() {
        return false;
    }

    @Override
    public boolean supportsMultipleOutputs() {
        return false;
    }

    @Override
    public void visit(PhyPlanVisitor v) throws VisitorException {
        v.visitCache(this);
    }

    @Override
    public POCache clone() throws CloneNotSupportedException {
        POCache newCache = new POCache(new OperatorKey(this.mKey.scope,
            NodeIdGenerator.getGenerator().getNextNodeId(this.mKey.scope)),
            this.inputs);
        return newCache;
    }

    @Override
    public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
        if(illustrator != null) {
            ExampleTuple tIn = (ExampleTuple) in;
            illustrator.getEquivalenceClasses().get(eqClassIndex).add(tIn);
            illustrator.addData((Tuple) in);
        }
        return (Tuple) in;
    }

}
