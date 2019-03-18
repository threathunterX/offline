package com.threathunter.bordercollie.slot.compute.graph.node.propertyhandler.reduction;


import com.threathunter.bordercollie.slot.compute.VariableDataContext;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapper;

import java.util.List;

/**
 * 
 */
public interface ReductionHandler {
    String getType();

    Object doReduction(VariableDataContext context);

    List<CacheWrapper> getCacheWrappers();
    // should add getData()
}
