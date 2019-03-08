package com.threathunter.bordercollie.slot.compute.graph.node.propertyhandler.condition;

import com.threathunter.bordercollie.slot.compute.VariableDataContext;
import com.threathunter.common.Identifier;
import com.threathunter.variable.condition.StringPropertyCondition;

/**
 * Created by daisy on 16/5/13.
 */
public abstract class StringPropertyConditionHandler implements PropertyConditionHandler {
    final String param;
    final String srcPropertyName;
    final Identifier srcId;

    public StringPropertyConditionHandler(final StringPropertyCondition c) {
        this.param = (String) c.getParam();
        this.srcPropertyName = c.getSrcProperties().get(0).getName();
        this.srcId = c.getSrcProperties().get(0).getIdentifier();
    }

    public static class StringContainsPropertyConditionHandler extends StringPropertyConditionHandler {

        public StringContainsPropertyConditionHandler(final StringPropertyCondition c) {
            super(c);
        }

        @Override
        public boolean match(final VariableDataContext context) {
            Object obj = context.getFromContext(srcId, srcPropertyName);
            if (obj == null) {
                return false;
            }
            return obj.toString().contains(this.param);
        }
    }

    public static class StringEqualsPropertyConditionHandler extends StringPropertyConditionHandler {

        public StringEqualsPropertyConditionHandler(final StringPropertyCondition c) {
            super(c);
        }

        @Override
        public boolean match(final VariableDataContext context) {
            Object obj = context.getFromContext(this.srcId, this.srcPropertyName);
            if (obj == null) {
                return false;
            }
            return obj.toString().equals(this.param);
        }
    }

    public static class StringNotEqualsPropertyConditionHandler extends StringPropertyConditionHandler {

        public StringNotEqualsPropertyConditionHandler(final StringPropertyCondition c) {
            super(c);
        }

        @Override
        public boolean match(final VariableDataContext context) {
            Object obj = context.getFromContext(this.srcId, this.srcPropertyName);
            if (obj == null) {
                return false;
            }
            return !obj.toString().equals(this.param);
        }
    }
}
