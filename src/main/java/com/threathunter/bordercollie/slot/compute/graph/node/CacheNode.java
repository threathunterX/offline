package com.threathunter.bordercollie.slot.compute.graph.node;

import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapper;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.PrimaryData;
import com.threathunter.variable.DimensionType;

import java.util.List;

/**
 * 
 */
public interface CacheNode<R> {
    String getVariableName();

    List<CacheWrapper> getWrappers();

    R getData(String... keys);

    Object getAll(String key);

    DimensionType getDataDimension();

    NodePrimaryData merge(NodePrimaryData data, String... keys);
}
