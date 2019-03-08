package com.threathunter.bordercollie.slot.compute.graph.node.propertyhandler.condition;


import com.threathunter.bordercollie.slot.compute.VariableDataContext;

/**
 * Created by daisy on 16/5/13.
 */
public interface PropertyConditionHandler {
    boolean match(VariableDataContext context);
}
