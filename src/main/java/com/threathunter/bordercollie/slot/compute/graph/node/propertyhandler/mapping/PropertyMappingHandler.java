package com.threathunter.bordercollie.slot.compute.graph.node.propertyhandler.mapping;


import com.threathunter.bordercollie.slot.compute.VariableDataContext;

/**
 * Created by daisy on 16/5/16.
 */
public interface PropertyMappingHandler {

    String getPropertyName();

    Object getMappedPropertyValue(VariableDataContext context);
}
