package com.threathunter.bordercollie.slot.compute.graph.node.operator;

// TODO need unit test

/**
 * This class is for logic operations, input is arrays of {@code boolean},
 * and get a result {@code true} or {@code false}.
 *
 * @author daisy
 * @since 1.4
 */
public abstract class LogicalOperator {
    public abstract boolean computeLogic(boolean... conditions);

    public abstract boolean shortCircuit(boolean current);

    public static class NotLogicalOperator extends LogicalOperator {

        @Override
        public boolean computeLogic(boolean... conditions) {
            return !conditions[0];
        }

        @Override
        public boolean shortCircuit(boolean current) {
            return false;
        }
    }

    public static class AndLogicalOperator extends LogicalOperator {

        @Override
        public boolean computeLogic(boolean... conditions) {
            for (boolean condition : conditions) {
                if (!condition) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public boolean shortCircuit(boolean current) {
            if (!current) {
                return true;
            }
            return false;
        }
    }

    public static class OrLogicalOperator extends LogicalOperator {

        @Override
        public boolean computeLogic(boolean... conditions) {
            for (boolean condition : conditions) {
                if (condition) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public boolean shortCircuit(boolean current) {
            if (current) {
                return true;
            }
            return false;
        }
    }
}
