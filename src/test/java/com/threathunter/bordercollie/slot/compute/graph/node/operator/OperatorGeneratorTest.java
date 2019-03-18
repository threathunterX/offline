package com.threathunter.bordercollie.slot.compute.graph.node.operator;

import com.threathunter.variable.exception.NotSupportException;
import org.junit.Test;

import static org.assertj.core.api.Java6Assertions.assertThat;
import static org.junit.Assert.fail;

/**
 * 
 */
public class OperatorGeneratorTest {

    @Test
    public void testGetOperator_Minus() {
        ValueOperator operator = OperatorGenerator.getOperator("-");
        assertThat(operator).isNotNull();
    }

    @Test(expected = NotSupportException.class)
    public void testGetOperator_Plus() {
        ValueOperator operator = OperatorGenerator.getOperator("+");
        fail();
    }

    @Test(expected = NotSupportException.class)
    public void testGetOperator_Multiply() {
        ValueOperator operator = OperatorGenerator.getOperator("*");
        fail();
    }

    @Test(expected = NotSupportException.class)
    public void testGetOperator_Divide() {
        ValueOperator operator = OperatorGenerator.getOperator("/");
        fail();
    }


}
