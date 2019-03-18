package com.threathunter.bordercollie.slot.compute.graph.node.propertyhandler.mapping;


import com.threathunter.bordercollie.slot.compute.VariableDataContext;

/**
 * 
 */
public interface PropertyMappingHandler {

    String getPropertyName();

    Object getMappedPropertyValue(VariableDataContext context);
}
