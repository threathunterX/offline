package com.threathunter.bordercollie.slot.compute.graph.node.propertyhandler.condition;

import com.threathunter.bordercollie.slot.compute.VariableDataContext;
import com.threathunter.common.Identifier;
import com.threathunter.variable.condition.LongPropertyCondition;

/**
 * 
 */
public abstract class LongPropertyConditionHandler implements PropertyConditionHandler {
    final long param;
    final String srcPropertyName;
    final Identifier srcId;

    public LongPropertyConditionHandler(final LongPropertyCondition c) {
        this.param = (long) c.getParam();
        this.srcPropertyName = c.getSrcProperties().get(0).getName();
        this.srcId = c.getSrcProperties().get(0).getIdentifier();
    }

    public static class LongBiggerThanPropertyConditionHandler extends LongPropertyConditionHandler {

        public LongBiggerThanPropertyConditionHandler(final LongPropertyCondition c) {
            super(c);
        }

        @Override
        public boolean match(final VariableDataContext context) {
            Object obj = context.getFromContext(this.srcId, this.srcPropertyName);
            if (obj == null) {
                return false;
            }
            if (!(obj instanceof Long)) {
                return false;
            }
            return (Long) obj > this.param;
        }
    }

    public static class LongBiggerEqualsPropertyConditionHandler extends LongPropertyConditionHandler {

        public LongBiggerEqualsPropertyConditionHandler(final LongPropertyCondition c) {
            super(c);
        }

        @Override
        public boolean match(final VariableDataContext context) {
            Object obj = context.getFromContext(this.srcId, this.srcPropertyName);
            if (obj == null) {
                return false;
            }
            if (!(obj instanceof Long)) {
                return false;
            }
            return (Long) obj >= this.param;
        }
    }

    public static class LongEqualsPropertyConditionHandler extends LongPropertyConditionHandler {

        public LongEqualsPropertyConditionHandler(final LongPropertyCondition c) {
            super(c);
        }

        @Override
        public boolean match(final VariableDataContext context) {
            Object obj = context.getFromContext(this.srcId, this.srcPropertyName);
            if (obj == null) {
                return false;
            }
            if (!(obj instanceof Long)) {
                return false;
            }
            return ((Long) obj).equals(this.param);
        }
    }

    public static class LongSmallerThanPropertyConditionHandler extends LongPropertyConditionHandler {

        public LongSmallerThanPropertyConditionHandler(final LongPropertyCondition c) {
            super(c);
        }

        @Override
        public boolean match(final VariableDataContext context) {
            Object obj = context.getFromContext(this.srcId, this.srcPropertyName);
            if (obj == null) {
                return false;
            }
            if (!(obj instanceof Long)) {
                return false;
            }
            return (Long) obj < this.param;
        }
    }
}
