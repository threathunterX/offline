package com.threathunter.bordercollie.slot.compute.graph.node.propertyhandler.condition;

import com.threathunter.bordercollie.slot.compute.VariableDataContext;
import com.threathunter.bordercollie.slot.compute.graph.node.operator.LogicalOperator;
import com.threathunter.variable.condition.CompoundCondition;

import java.util.List;

/**
 * Created by daisy on 16/5/23.
 */
public class CompoundConditionHandler implements PropertyConditionHandler {
    private List<PropertyConditionHandler> subConditionHandlers;
    private LogicalOperator logicalOperator;

    public List<PropertyConditionHandler> getSubConditionHandlers() {
        return subConditionHandlers;
    }

    public void setSubConditionHandlers(final List<PropertyConditionHandler> subConditionHandlers) {
        this.subConditionHandlers = subConditionHandlers;
    }

    public LogicalOperator getLogicalOperator() {
        return logicalOperator;
    }

    public void setLogicalOperator(final LogicalOperator logicalOperator) {
        this.logicalOperator = logicalOperator;
    }

    public CompoundConditionHandler(final CompoundCondition c) {
    }

    private boolean compoundCheck(final VariableDataContext context) {
        boolean[] subs = new boolean[this.subConditionHandlers.size()];
        for (int i = 0; i < subs.length; i++) {
            subs[i] = this.subConditionHandlers.get(i).match(context);
            if (this.logicalOperator.shortCircuit(subs[i])) {
                return subs[i];
            }
        }
        return this.logicalOperator.computeLogic(subs);
    }

    @Override
    public boolean match(VariableDataContext context) {
        return compoundCheck(context);
    }
}
