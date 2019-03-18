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
 * 
 */
public class IPSceneStrategyCountNode implements CacheNode<Map>, IncidentNode {
    private final List<CacheWrapper> wrappers;
    private Map<String, Map<String, LimitMaxPriorityQueue>> keyTopQueue;
    private final String variableName = "ip_scene_strategy__visit_incident_group_count__1h__slot";

    public IPSceneStrategyCountNode(final StorageType type) {
        this.wrappers = new ArrayList<>();
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.SECONDARY_COUNT);
        meta.setStorageType(type);
        meta.setIndexCount(2);
        meta.setSecondaryKeyHashType(HashType.NORMAL);
        this.wrappers.add(new SecondaryCountsArrayCacheWrapper.SecondaryCountArrayCacheWrapper(meta));

        this.keyTopQueue = new HashMap<>();
    }

    @Override
    public void compute(final Event event) {
        Map<String, List<String>> sceneStrategies = (Map<String, List<String>>) event.getPropertyValues().get("strategies");
        if (sceneStrategies != null) {
            sceneStrategies.forEach((scene, strategies) -> strategies.forEach(strategy -> {
                String firstKey = (String) event.getPropertyValues().get("c_ip");
                String secondKey = getCacheKey(scene, strategy);
                Number v = (Number) this.wrappers.get(0).addData("", firstKey, secondKey);
                this.keyTopQueue.computeIfAbsent(firstKey, k -> new HashMap<>()).computeIfAbsent(scene,
                        q -> new LimitMaxPriorityQueue(20)).update(secondKey, v);
            }));
        }
    }

    @Override
    public String getName() {
        return this.variableName;
    }

    @Override
    public String getVariableName() {
        return this.variableName;
    }

    @Override
    public List<CacheWrapper> getWrappers() {
        return wrappers;
    }

    @Override
    public Map getData(final String... keys) {
        Map<String, LimitMaxPriorityQueue> queues = this.keyTopQueue.get(keys[0]);
        if (queues == null) {
            return null;
        }
        Map<String, Object> result = new HashMap<>();
        queues.forEach((scene, queue) -> {
            List<Map<String, Object>> list = queue.getCopy();
            List<Map<String, Object>> data = new ArrayList<>(list.size());
            list.forEach(m -> {
                Map<String, Object> dm = new HashMap<>(4);
                dm.put("key", ((String) m.get("key")).split("@@")[1]);
                dm.put("value", m.get("value"));
                data.add(dm);
            });
            result.put(scene, data);
        });
        return result;
    }

    @Override
    public Object getAll(String key) {
        return this.wrappers.get(0).readAll(key);
    }

    @Override
    public DimensionType getDataDimension() {
        return DimensionType.IP;
    }

    @Override
    public NodePrimaryData merge(NodePrimaryData data, String... keys) {
        return null;
    }

    private static String getCacheKey(String scene, String strategy) {
        return String.format("%s@@%s", scene, strategy).intern();
    }

    private static String[] unfoldCacheKey(final String cacheKey) {
        return cacheKey.split("@@");
    }

    public static void main(String[] args) {
        String cacheKey = getCacheKey("scene", "strategy");
        System.out.println(cacheKey);
        System.out.println(unfoldCacheKey(cacheKey)[1]);
    }
}
