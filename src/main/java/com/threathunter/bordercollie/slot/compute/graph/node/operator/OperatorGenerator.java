package com.threathunter.bordercollie.slot.compute.graph.node.operator;


import com.threathunter.variable.exception.NotSupportException;

/**
 * Generate specific operator for two numbers according to operator.
 *
 * @author daisy
 * @since 1.4
 */
public class OperatorGenerator {
    // TODO need to add: '+', '*', '/'
    public static ValueOperator getOperator(String operator) {
        if (operator.equals("-")) {
            return new LongValueOperator.LongValueSubtractOperator();
        }
        throw new NotSupportException("operator is not support: " + operator);
    }
}
