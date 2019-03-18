package com.threathunter.bordercollie.slot.compute.graph.node.propertyhandler.condition;


import com.threathunter.bordercollie.slot.compute.VariableDataContext;

/**
 * 
 */
public interface PropertyConditionHandler {
    boolean match(VariableDataContext context);
}
