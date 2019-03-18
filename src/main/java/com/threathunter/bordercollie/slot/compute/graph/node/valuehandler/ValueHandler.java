package com.threathunter.bordercollie.slot.compute.graph.node.valuehandler;

import com.threathunter.bordercollie.slot.compute.VariableDataContext;
import com.threathunter.common.NamedType;

/**
 * 
 */
public interface ValueHandler<T, R> {

    R addValue(T value, VariableDataContext context);

    NamedType valueType();
}
