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

/**
 * Created by daisy on 17/3/31.
 */
public class IPIncidentMaxRateNode implements CacheNode<Long>, IncidentNode {
    private static final long ONE_MINUTE = 60 * 1000;

    private long currentMinuteInMillis = 0;
    private final List<CacheWrapper> wrappers;
    private final String variableName = "ip__visit_incident_max_rate__1h__slot";

    public IPIncidentMaxRateNode(final StorageType type) {
        this.wrappers = new ArrayList<>();
        CacheWrapperMeta countCacheWrapperMeta = new CacheWrapperMeta();
        countCacheWrapperMeta.setCacheType(CacheType.LAST_LONG);
        countCacheWrapperMeta.setStorageType(type);
        countCacheWrapperMeta.setIndexCount(1);
        this.wrappers.add(CacheWrapperFactory.createCacheWrapper(countCacheWrapperMeta));

        CacheWrapperMeta maxCacheWrapperMeta = new CacheWrapperMeta();
        maxCacheWrapperMeta.setCacheType(CacheType.MAX_LONG);
        maxCacheWrapperMeta.setStorageType(type);
        maxCacheWrapperMeta.setIndexCount(1);
        this.wrappers.add(CacheWrapperFactory.createCacheWrapper(maxCacheWrapperMeta));
    }

    @Override
    public void compute(Event event) {
        long minuteInMillis = event.getTimestamp() / ONE_MINUTE * ONE_MINUTE;
        String ip = (String) event.getPropertyValues().get("c_ip");
        if (minuteInMillis != currentMinuteInMillis) {
            this.currentMinuteInMillis = minuteInMillis;
            this.wrappers.get(0).addData(0l, ip);
        }
        Long current = (Long) this.wrappers.get(0).getData(ip);
        if (current == null) {
            current = 0l;
        }
        this.wrappers.get(0).addData(current + 1, ip);
        this.wrappers.get(1).addData(current + 1, ip);
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
    public Long getData(String... keys) {
        Long result = (Long) this.wrappers.get(1).getData(keys[0]);
        if (result == null) {
            return 0l;
        }
        return result;
    }

    @Override
    public Object getAll(String key) {
        return this.wrappers.get(1).readAll(key);
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
