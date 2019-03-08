package com.threathunter.bordercollie.slot.compute.graph.node.operator;

/**
 * Operation between two numbers.
 *
 * @author daisy
 * @since 1.4
 */
public abstract class LongValueOperator implements ValueOperator {

    /**
     * Subtract between two {@code Long} values.
     */
    public static class LongValueSubtractOperator extends LongValueOperator {
        @Override
        public Long doOperate(Number value1, Number value2) {
            return value2.longValue() - value1.longValue();
        }
    }
}
