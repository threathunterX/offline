package com.threathunter.bordercollie.slot.compute.graph.node.operator;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 
 */

public class LongValueOperatorTest {

    @Test
    public void testLongValueSubtractOperator_DoOperator() {
        LongValueOperator.LongValueSubtractOperator longValueSubtractOperator = new LongValueOperator.LongValueSubtractOperator();
        assertThat(longValueSubtractOperator).isNotNull();
        assertThat(longValueSubtractOperator.doOperate(1, 1)).isEqualTo(0);
        assertThat(longValueSubtractOperator.doOperate(1, 2)).isEqualTo(1);
        assertThat(longValueSubtractOperator.doOperate(2, 1)).isEqualTo(-1);
    }
}
