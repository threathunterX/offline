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
 * Created by daisy on 17/4/3.
 */
public class NamedSceneHitCountNode implements CacheNode<Integer>, IncidentNode {
    private final List<CacheWrapper> wrappers;
    private final String sceneName;
    private final String variableName;

    public NamedSceneHitCountNode(String sceneName, StorageType type, String variableName) {
        this.wrappers = new ArrayList<>();
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.GLOBAL_COUNT);
        meta.setStorageType(type);
        meta.setIndexCount(0);
        this.wrappers.add(CacheWrapperFactory.createCacheWrapper(meta));
        this.sceneName = sceneName;
        this.variableName = variableName;
    }

    @Override
    public void compute(Event event) {
        Map<String, List<String>> sceneStrategies = (Map<String, List<String>>) event.getPropertyValues().get("strategies");
        if (sceneStrategies != null) {
            if (sceneStrategies.containsKey(this.sceneName)) {
                this.wrappers.get(0).addData("", "");
            }
        }
    }

    @Override
    public String getName() {
        return this.variableName;
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
    public Integer getData(final String... keys) {
        return (Integer) this.wrappers.get(0).getData("", "");
    }

    @Override
    public Object getAll(final String key) {
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

}
