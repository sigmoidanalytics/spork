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
package org.apache.pig.newplan.logical.relational;

import java.util.List;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanVisitor;

/**
 * We can <b>optionally</b> cache intermediate relations into some caching service.
 * The default implementation can be a simple no-op.
 */
public class LOCache extends LogicalRelationalOperator {

    public LOCache(OperatorPlan plan) {
        super("LOCache", plan);
    }

    @Override
    public LogicalSchema getSchema() throws FrontendException {
        if (schema == null) {
            schema = getPredecessor().getSchema();
        }
        return schema;
    }

    @Override
    public void accept(PlanVisitor v) throws FrontendException {
        if (!(v instanceof LogicalRelationalNodesVisitor)) {
            throw new FrontendException("Expected LogicalPlanVisitor", 2223);
        }
        ((LogicalRelationalNodesVisitor)v).visit(this);
    }

    @Override
    public boolean isEqual(Operator operator) throws FrontendException {
        return operator instanceof LOCache &&
                ((LOCache) operator).getPredecessor().equals(getPredecessor());
    }

    public LogicalRelationalOperator getPredecessor() throws FrontendException {
        List<Operator> preds = plan.getPredecessors(this);
        if (preds != null && preds.size() == 1) {
            return (LogicalRelationalOperator) preds.get(0);
        } else {
            throw new FrontendException("LOCache expects exactly 1 predecessor. Saw " + preds.size());
        }
    }
}
