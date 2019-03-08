package com.threathunter.bordercollie.slot.compute.graph.node.operator;

/**
 * Operator between two numbers.
 *
 * @author daisy
 * @since 1.4
 */
public interface ValueOperator {
    Number doOperate(Number value1, Number value2);
}
