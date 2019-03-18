package com.threathunter.bordercollie.slot.compute.graph.extension.incident;

import com.threathunter.bordercollie.slot.compute.cache.CacheType;
import com.threathunter.bordercollie.slot.compute.cache.StorageType;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapper;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapperFactory;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapperMeta;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.array.SecondaryLongArrayCacheWrapper;
import com.threathunter.bordercollie.slot.compute.graph.node.CacheNode;
import com.threathunter.bordercollie.slot.compute.graph.node.NodePrimaryData;
import com.threathunter.bordercollie.slot.util.HashType;
import com.threathunter.bordercollie.slot.util.LimitMaxPriorityQueue;
import com.threathunter.model.Event;
import com.threathunter.variable.DimensionType;
import org.apache.commons.lang3.mutable.MutableLong;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Incident score compute now is not right.
 * <p>
 * 
 */
public class IPMaxSceneScoreNode implements CacheNode<Object>, IncidentNode {
    private final List<CacheWrapper> wrappers;
    private LimitMaxPriorityQueue topQueue;
    private final String variableName = "ip__visit_incident_score__1h__slot";

    // TODO check if the count cache wrapper should be secondary cache wrapper, every scene should count its own
    public IPMaxSceneScoreNode(final StorageType type) {
        this.wrappers = new ArrayList<>();
        CacheWrapperMeta sumCacheWrapperMeta = new CacheWrapperMeta();
        sumCacheWrapperMeta.setCacheType(CacheType.SECONDARY_LAST_LONG);
        sumCacheWrapperMeta.setStorageType(type);
        sumCacheWrapperMeta.setIndexCount(2);
        sumCacheWrapperMeta.setSecondaryKeyHashType(HashType.NORMAL);

        this.wrappers.add(new SecondaryLongArrayCacheWrapper.SecondaryLastLongArrayCacheWrapper(sumCacheWrapperMeta));

        CacheWrapperMeta countCacheWrapperMeta = new CacheWrapperMeta();
        countCacheWrapperMeta.setCacheType(CacheType.COUNT);
        countCacheWrapperMeta.setStorageType(type);
        countCacheWrapperMeta.setIndexCount(1);
        this.wrappers.add(CacheWrapperFactory.createCacheWrapper(countCacheWrapperMeta));

        this.topQueue = new LimitMaxPriorityQueue(100);
    }

    @Override
    public void compute(final Event event) {
        Map<String, MutableLong> sceneScore = (Map<String, MutableLong>) event.getPropertyValues().get("scores");
        if (sceneScore != null) {
            String ip = (String) event.getPropertyValues().get("c_ip");
            this.wrappers.get(1).addData("", ip);
            sceneScore.forEach((scene, score) -> {
                Long sum = (Long) this.wrappers.get(0).getData(ip, scene);
                if (sum == null) {
                    sum = 0l;
                }
                this.wrappers.get(0).addData(sum + score.longValue(), ip, scene);
            });
            topQueue.update(ip, getIpData(ip));
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
        return this.wrappers;
    }

    @Override
    public Object getData(final String... keys) {
        if (keys == null || keys.length <= 0 || keys[0].isEmpty() || keys[0].equals("__GLOBAL__")) {
            return getTopData();
        } else {
            return getIpData(keys);
        }
    }

    private Long getIpData(final String... keys) {
        Map<Integer, Number> result = (Map<Integer, Number>) this.wrappers.get(0).readAll(keys[0]);
        MutableLong data = new MutableLong(0);
        result.values().forEach(score -> data.setValue(data.longValue() > score.longValue() ? data.longValue() : score));
        Integer count = (Integer) this.wrappers.get(1).getData(keys[0]);
        if (count == null || count == 0) {
            return null;
        }
        return data.longValue() / count;
    }

    private List getTopData() {
        return this.topQueue.getCopy();
    }

    @Override
    public Object getAll(final String key) {
        return getData(key);
    }

    @Override
    public DimensionType getDataDimension() {
        return DimensionType.IP;
    }

    @Override
    public NodePrimaryData merge(NodePrimaryData data, String... keys) {
        return null;
    }

}
