package com.threathunter.bordercollie.slot.compute.graph.extension.incident;

import com.threathunter.bordercollie.slot.compute.cache.CacheType;
import com.threathunter.bordercollie.slot.compute.cache.StorageType;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapper;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapperFactory;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapperMeta;
import com.threathunter.bordercollie.slot.compute.graph.node.CacheNode;
import com.threathunter.bordercollie.slot.compute.graph.node.NodePrimaryData;
import com.threathunter.bordercollie.slot.util.DimensionHelper;
import com.threathunter.bordercollie.slot.util.HashType;
import com.threathunter.model.Event;
import com.threathunter.variable.DimensionType;

import java.util.*;

/**
 * Created by daisy on 17/3/31.
 */
public class DimensionTagHitCountNode implements CacheNode<List>, IncidentNode {
    private static final Map<DimensionType, String> VARIABLE_NAME;

    static {
        VARIABLE_NAME = new HashMap<>();
        VARIABLE_NAME.put(DimensionType.IP, "ip_tag__visit_incident_count_top20__1h__slot");
        VARIABLE_NAME.put(DimensionType.UID, "uid_tag__visit_incident_count_top20__1h__slot");
        VARIABLE_NAME.put(DimensionType.DID, "did_tag__visit_incident_count_top20__1h__slot");
    }

    private final List<CacheWrapper> wrappers;
    private final String keyField;
    private final String variableName;
    private final DimensionType dimensionType;

    public DimensionTagHitCountNode(final StorageType type, final DimensionType dimensionType) {
        this.keyField = DimensionHelper.getDimensionKey(dimensionType);
        this.variableName = VARIABLE_NAME.get(dimensionType);
        this.wrappers = new ArrayList<>();
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.GROUP_COUNT);
        meta.setStorageType(type);
        meta.setIndexCount(2);
        meta.setSecondaryKeyHashType(HashType.NORMAL);
        this.wrappers.add(CacheWrapperFactory.createCacheWrapper(meta));

        this.dimensionType = dimensionType;
    }

    @Override
    public void compute(final Event event) {
        Collection<String> tags = (Collection<String>) event.getPropertyValues().get("tags");
        if (tags != null) {
            tags.forEach(tag -> {
                String firstKey = (String) event.getPropertyValues().get(keyField);
                this.wrappers.get(0).addData("", firstKey, tag);
            });
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
    public List getData(String... keys) {
        if (keys == null || keys.length < 1) {
            return null;
        }
        Map<String, Number> data = (Map<String, Number>) this.wrappers.get(0).getData(keys);
        if (data == null || data.isEmpty()) {
            return null;
        }

        List<Map<String, Object>> result = new ArrayList<>(data.size());
        data.forEach((tag, count) -> {
            HashMap<String, Object> map = new HashMap<>(4);
            map.put("key", tag);
            map.put("value", count);
            result.add(map);
        });

        result.sort((d1, d2) -> ((Number) d2.get("value")).intValue() - ((Number) d1.get("value")).intValue());

        return result;
    }

    @Override
    public Object getAll(final String key) {
        return this.wrappers.get(0).readAll(key);
    }

    @Override
    public DimensionType getDataDimension() {
        return this.dimensionType;
    }

    @Override
    public NodePrimaryData merge(NodePrimaryData data, String... keys) {
        return null;
    }

}
