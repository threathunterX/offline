package com.threathunter.bordercollie.slot.compute.graph.extension.incident;

import com.threathunter.bordercollie.slot.compute.cache.CacheType;
import com.threathunter.bordercollie.slot.compute.cache.StorageType;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapper;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapperFactory;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapperMeta;
import com.threathunter.bordercollie.slot.compute.graph.node.CacheNode;
import com.threathunter.bordercollie.slot.compute.graph.node.NodePrimaryData;
import com.threathunter.model.Event;
import com.threathunter.variable.DimensionType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 
 */
public class PageVisitorHitCountNode implements CacheNode<Object>, IncidentNode {
    private final List<CacheWrapper> wrappers;
    private final String variableName = "page__visit__visitor_incident_count__1h__slot";

    public PageVisitorHitCountNode(final StorageType type) {
        this.wrappers = new ArrayList<>();
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.COUNT);
        meta.setStorageType(type);
        meta.setIndexCount(1);
        this.wrappers.add(CacheWrapperFactory.createCacheWrapper(meta));
    }

    @Override
    public void compute(final Event event) {
        Map<String, List<String>> sceneStrategies = (Map<String, List<String>>) event.getPropertyValues().get("strategies");
        if (sceneStrategies != null) {
            if (sceneStrategies.containsKey("VISITOR")) {
                this.wrappers.get(0).addData("", (String) event.getPropertyValues().get("page"));
            }
        }
    }

    @Override
    public String getName() {
        return variableName;
    }

    @Override
    public String getVariableName() {
        return variableName;
    }

    @Override
    public List<CacheWrapper> getWrappers() {
        return this.wrappers;
    }

    @Override
    public Integer getData(String... keys) {
        return (Integer) this.wrappers.get(0).getData(keys[0]);
    }

    @Override
    public Object getAll(String key) {
        return this.wrappers.get(0).readAll(key);
    }

    @Override
    public DimensionType getDataDimension() {
        return DimensionType.PAGE;
    }

    @Override
    public NodePrimaryData merge(NodePrimaryData data, String... keys) {
        return null;
    }

}
