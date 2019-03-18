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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 
 */
public class SceneHitCountNode implements CacheNode<Integer>, IncidentNode {
    private static final Map<String, String> CATEGORY_VARIABLE_MAP;
    private final List<CacheWrapper> wrappers;
    private static final String NODE_NAME = "__scene_hit_count__";

    static {
        // TODO static string
        CATEGORY_VARIABLE_MAP = new HashMap<>();
        CATEGORY_VARIABLE_MAP.put("VISITOR", "total__visit__visitor_incident_count__1h__slot");
        CATEGORY_VARIABLE_MAP.put("ACCOUNT", "total__visit__account_incident_count__1h__slot");
        CATEGORY_VARIABLE_MAP.put("MARKETING", "total__visit__marketing_incident_count__1h__slot");
        CATEGORY_VARIABLE_MAP.put("ORDER", "total__visit__order_incident_count__1h__slot");
        CATEGORY_VARIABLE_MAP.put("TRANSACTION", "total__visit__transaction_incident_count__1h__slot");
        CATEGORY_VARIABLE_MAP.put("OTHER", "total__visit__other_incident_count__1h__slot");
    }

    public SceneHitCountNode(final StorageType type) {
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
            sceneStrategies.keySet().forEach(scene -> this.wrappers.get(0).addData("", getCacheKey(scene)));
        }
    }

    @Override
    public String getName() {
        return NODE_NAME;
    }

    @Override
    public String getVariableName() {
        return NODE_NAME;
    }

    @Override
    public List<CacheWrapper> getWrappers() {
        return this.wrappers;
    }

    @Override
    public Integer getData(String... keys) {
        return (Integer) this.wrappers.get(0).getData(getCacheKey(keys[0]));
    }

    @Override
    public Object getAll(String key) {
        return this.wrappers.get(0).readAll(key);
    }

    @Override
    public DimensionType getDataDimension() {
        return DimensionType.GLOBAL;
    }

    @Override
    public NodePrimaryData merge(NodePrimaryData data, String... keys) {
        return null;
    }


    private String getCacheKey(final String scene) {
        return String.format("%s@@%s", NODE_NAME, scene).intern();
    }
}
