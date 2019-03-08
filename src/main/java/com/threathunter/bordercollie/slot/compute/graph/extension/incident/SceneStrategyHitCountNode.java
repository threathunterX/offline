package com.threathunter.bordercollie.slot.compute.graph.extension.incident;

import com.threathunter.bordercollie.slot.compute.cache.CacheType;
import com.threathunter.bordercollie.slot.compute.cache.StorageType;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapper;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapperFactory;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapperMeta;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.array.SecondaryCountsArrayCacheWrapper;
import com.threathunter.bordercollie.slot.compute.graph.node.CacheNode;
import com.threathunter.bordercollie.slot.compute.graph.node.NodePrimaryData;
import com.threathunter.bordercollie.slot.util.HashType;
import com.threathunter.bordercollie.slot.util.LimitMaxPriorityQueue;
import com.threathunter.model.Event;
import com.threathunter.variable.DimensionType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * key is scene@@strategy so that cache can just add 4bytes for every global
 * <p>
 * Created by daisy on 17/3/30.
 */
public class SceneStrategyHitCountNode implements CacheNode<List>, IncidentNode {
    private final List<CacheWrapper> wrappers;
    private Map<String, LimitMaxPriorityQueue> sceneTopQueue;
    private final String variableName = "scene_strategy__visit_incident_count_top20__1h__slot";

    public SceneStrategyHitCountNode(final StorageType type) {
        this.wrappers = new ArrayList<>();
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.SECONDARY_COUNT);
        meta.setStorageType(type);
        meta.setIndexCount(2);
        meta.setSecondaryKeyHashType(HashType.NORMAL);
        // get CacheWrapper by meta
        CacheWrapper cacheWrapper = new SecondaryCountsArrayCacheWrapper.SecondaryCountArrayCacheWrapper(meta);
        if (cacheWrapper == null) {
            throw new RuntimeException("create wrapper failed");
        }
        this.wrappers.add(cacheWrapper);

        this.sceneTopQueue = new HashMap<>();
    }

    @Override
    public String getVariableName() {
        return this.variableName;
    }

    @Override
    public List<CacheWrapper> getWrappers() {
        return this.wrappers;
    }

    @Override
    public List getData(final String... keys) {
        String firstKey = keys[0];
        List<Map<String, Object>> result = new ArrayList<>();
        LimitMaxPriorityQueue topQueue = sceneTopQueue.get(firstKey);
        if (topQueue == null) {
            return result;
        }

        List<Map<String, Object>> tops = topQueue.getCopy();
        return tops;
    }

    @Override
    public Object getAll(String key) {
        return this.wrappers.get(0).readAll(key);
    }

    @Override
    public DimensionType getDataDimension() {
        return DimensionType.OTHER;
    }

    @Override
    public NodePrimaryData merge(NodePrimaryData data, String... keys) {
        return null;
    }


    @Override
    public void compute(final Event event) {
        Map<String, List<String>> sceneStrategies = (Map<String, List<String>>) event.getPropertyValues().get("strategies");
        if (sceneStrategies != null) {
            sceneStrategies.forEach((scene, strategies) ->
                    strategies.forEach(strategy -> {
                        Number v = (Number) this.wrappers.get(0).addData("", scene, strategy);
                        this.sceneTopQueue.computeIfAbsent(scene, k -> new LimitMaxPriorityQueue(20)).update(strategy, v);
                    }));
        }
    }

    @Override
    public String getName() {
        return variableName;
    }
}
