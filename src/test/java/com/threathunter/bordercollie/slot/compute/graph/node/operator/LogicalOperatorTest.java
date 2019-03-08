package com.threathunter.bordercollie.slot.compute.graph.node.operator;

import org.junit.Assert;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class LogicalOperatorTest {

    @Test
    public void testNotLogicalOperator_ShortCircuit() {
        LogicalOperator.NotLogicalOperator notLogicalOperator = new LogicalOperator.NotLogicalOperator();
        assertThat(notLogicalOperator).isNotNull();
        assertThat(notLogicalOperator.shortCircuit(Boolean.TRUE)).isFalse();
        assertThat(notLogicalOperator.shortCircuit(Boolean.FALSE)).isFalse();
    }

    @Test
    public void testAndLogicalOperator_ShortCircuit() {
        LogicalOperator.AndLogicalOperator andLogicalOperator = new LogicalOperator.AndLogicalOperator();
        assertThat(andLogicalOperator).isNotNull();
        assertThat(andLogicalOperator.shortCircuit(Boolean.TRUE)).isFalse();
        assertThat(andLogicalOperator.shortCircuit(Boolean.FALSE)).isTrue();
    }

    @Test
    public void testOrLogicalOperator_ShortCircuit() {
        LogicalOperator.OrLogicalOperator orLogicalOperator = new LogicalOperator.OrLogicalOperator();
        assertThat(orLogicalOperator).isNotNull();
        assertThat(orLogicalOperator.shortCircuit(Boolean.TRUE)).isTrue();
        assertThat(orLogicalOperator.shortCircuit(Boolean.FALSE)).isFalse();
    }

    @Test
    public void testNotLogicalOperator_ComputeLogical() {
        LogicalOperator.NotLogicalOperator notLogicalOperator = new LogicalOperator.NotLogicalOperator();
        assertThat(notLogicalOperator).isNotNull();
        assertThat(notLogicalOperator.computeLogic(false, true)).isTrue();
        assertThat(notLogicalOperator.computeLogic(false, false)).isTrue();
        assertThat(notLogicalOperator.computeLogic(true, true)).isFalse();
        assertThat(notLogicalOperator.computeLogic(true, false)).isFalse();
    }

    @Test
    public void testAndLogicalOperator_ComputeLogical() {
        LogicalOperator.AndLogicalOperator andLogicalOperator = new LogicalOperator.AndLogicalOperator();
        assertThat(andLogicalOperator).isNotNull();
        assertThat(andLogicalOperator.computeLogic(false, true)).isFalse();
        assertThat(andLogicalOperator.computeLogic(false, false)).isFalse();
        assertThat(andLogicalOperator.computeLogic(true, true)).isTrue();
        assertThat(andLogicalOperator.computeLogic(true, false)).isFalse();
    }

    @Test
    public void testOrLogicalOperator_ComputeLogical() {
        LogicalOperator.OrLogicalOperator orLogicalOperator = new LogicalOperator.OrLogicalOperator();
        assertThat(orLogicalOperator).isNotNull();
        assertThat(orLogicalOperator.computeLogic(false, true)).isTrue();
        assertThat(orLogicalOperator.computeLogic(false, false)).isFalse();
        assertThat(orLogicalOperator.computeLogic(true, true)).isTrue();
        assertThat(orLogicalOperator.computeLogic(true, false)).isTrue();
    }


}
