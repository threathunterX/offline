package com.threathunter.bordercollie.slot.compute.graph;


import com.threathunter.bordercollie.slot.compute.graph.node.CacheNode;
import com.threathunter.variable.DimensionType;

import java.util.*;

/**
 * Created by daisy on 17/3/17.
 */
public class VariableCacheIterator implements Iterator<Map.Entry<String, Map<String, Object>>> {
    private final Iterator<String> mapKeyIterator;
    private final List<CacheNode> cacheNodes;
    private final DimensionType dimensionType;

    public VariableCacheIterator(final Iterator<String> cacheMapKeyIterator, DimensionType dimensionType, final List<CacheNode> nodes) {
        this.mapKeyIterator = cacheMapKeyIterator;
        this.cacheNodes = nodes;
        this.dimensionType = dimensionType;
    }

    public DimensionType getDimensionType() {
        return dimensionType;
    }

    @Override
    public boolean hasNext() {
        return this.mapKeyIterator.hasNext();
    }

    @Override
    public Map.Entry<String, Map<String, Object>> next() {
        String nextKey = this.mapKeyIterator.next();
        Map<String, Object> result = new HashMap<>();

        this.cacheNodes.forEach(node -> {
            Object obj = node.getData(nextKey);
            if (obj != null) {
                result.put(node.getVariableName(), obj);
            }
        });

        return new AbstractMap.SimpleEntry<>(nextKey, result);
    }
}
