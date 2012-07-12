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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PlanPrinter;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.pen.util.ExampleTuple;

public class POCache extends PhysicalOperator {

    private static final Log LOG = LogFactory.getLog(POCache.class);
    private static final long serialVersionUID = 1L;

    // The expression plan
    transient PhysicalPlan plan;
    String key;

    public POCache(OperatorKey k, PhysicalPlan plan) {
        super(k);
        this.plan = plan;
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
            this.plan.clone());
        newCache.setInputs(inputs);
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


    /**
     * Get a cache key for the given operator, or null if we don't know how to handle its type (or one of
     * its predcesessors' types) and want to not cache this subplan at all.
     *
     * Right now, this only handles loads. Unless we figure out a nice way to turn the PO plan into a
     * string or compare two PO plans, we'll probably have to handle each type of physical operator
     * recursively to generate a cache key.
     * @param plan
     * @throws IOException
     */
    public String computeCacheKey() throws IOException {
        if (key == null) {
            key = computeRawCacheKey(inputs);
            if (key != null) {
                // TODO deal with collisions!!
                key = UUID.nameUUIDFromBytes(key.getBytes()).toString();
            }
        }
        return key;
    }

    private String computeRawCacheKey(List<PhysicalOperator> preds) throws IOException {
        StringBuilder sb = new StringBuilder();
        for (PhysicalOperator operator : preds) {
            if (operator instanceof POLoad) {
                // Load operators are equivalent if the file is the same
                // and the loader is the same
                // Potential problems down the line:
                // * not checking LoadFunc arguments
                sb.append("LOAD: " + ((POLoad) operator).getLFile().getFileName()
                        + ((POLoad) operator).getLoadFunc().getClass().getName());
            } else if (operator instanceof POForEach) {
                // We consider ForEach operators to be equivalent if their inner plans
                // have the same explain plan after dropping scope markers.
                // Potential problems downstream:
                // * not checking for Nondeterministic UDFs
                // * jars / class defs changing under us
                StringBuilder foreachPlanKeysBuilder = new StringBuilder();
                for (PhysicalPlan innerPlan : ((POForEach) operator).getInputPlans()) {
                    foreachPlanKeysBuilder.append(innerPlanKey(innerPlan));
                }
                sb.append(foreachPlanKeysBuilder.toString());
                String inputKey = computeRawCacheKey(operator.getInputs());
                if (inputKey == null) {
                    return null;
                } else {
                    sb.append(inputKey);
                    LOG.info("Input key: " + inputKey);
                }
            } else if (operator instanceof POFilter) {
                // Similar to foreach.
                PhysicalPlan innerPlan = ((POFilter) operator).getPlan();
                sb.append(innerPlanKey(innerPlan));
                String inputKey = computeRawCacheKey(operator.getInputs());
                if (inputKey == null) {
                    return null;
                } else {
                    sb.append(inputKey);
                    LOG.info("Input key: " + inputKey);
                }
            } else {
                LOG.info("Don't know how to generate cache key for " + operator.getClass() + "; not caching");
                return null;
            }
        }
        return sb.toString();
    }

    private String innerPlanKey(PhysicalPlan plan) throws VisitorException, IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PlanPrinter<PhysicalOperator, PhysicalPlan> pp =
                new PlanPrinter<PhysicalOperator, PhysicalPlan>(plan);
        pp.print(baos);
        String explained = baos.toString();

        // get rid of scope numbers in these inner plans.
        return explained.replaceAll("scope-\\d+", "");
    }
}
