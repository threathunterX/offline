package com.threathunter.bordercollie.slot.compute.graph.node.propertyhandler.condition;

import com.threathunter.bordercollie.slot.compute.VariableDataContext;
import com.threathunter.common.Identifier;
import com.threathunter.variable.condition.DoublePropertyCondition;

/**
 * Created by threathunter-dev on 16/5/23.
 */
public abstract class DoublePropertyConditionHandler implements PropertyConditionHandler {
    final double param;
    final String srcPropertyName;
    final Identifier srcId;

    public DoublePropertyConditionHandler(final DoublePropertyCondition c) {
        this.param = (Double) c.getParam();
        this.srcPropertyName = c.getSrcProperties().get(0).getName();
        this.srcId = c.getSrcProperties().get(0).getIdentifier();
    }

    public static class DoubleSmallerThanPropertyConditionHandler extends DoublePropertyConditionHandler {
        public DoubleSmallerThanPropertyConditionHandler(final DoublePropertyCondition c) {
            super(c);
        }

        @Override
        public boolean match(final VariableDataContext context) {
            Object obj = context.getFromContext(this.srcId, srcPropertyName);
            if (obj == null) {
                return false;
            }
            if (!(obj instanceof Double)) {
                return false;
            }
            return (Double) obj < this.param;
        }
    }
}
