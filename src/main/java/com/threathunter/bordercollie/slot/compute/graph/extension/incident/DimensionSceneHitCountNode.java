package com.threathunter.bordercollie.slot.compute.graph.extension.incident;

import com.threathunter.bordercollie.slot.compute.cache.CacheType;
import com.threathunter.bordercollie.slot.compute.cache.StorageType;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapper;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapperMeta;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.array.CountsArrayCacheWrapper;
import com.threathunter.bordercollie.slot.compute.graph.node.CacheNode;
import com.threathunter.bordercollie.slot.compute.graph.node.NodePrimaryData;
import com.threathunter.bordercollie.slot.util.DimensionHelper;
import com.threathunter.bordercollie.slot.util.HashType;
import com.threathunter.model.Event;
import com.threathunter.variable.DimensionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * 
 */
public class DimensionSceneHitCountNode implements CacheNode<Map>, IncidentNode {
    private static final Logger logger = LoggerFactory.getLogger("bordercollie");
    private final List<CacheWrapper> wrappers;
    private final String variableName;
    private final String fieldName;
    private final DimensionType dimensionType;

    // support max number of distinct subkeys, or values, not only 20
    public DimensionSceneHitCountNode(final DimensionType dimensionType, final StorageType type) {
        this.wrappers = new ArrayList<>();
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.GROUP_COUNT);
        meta.setStorageType(type);
        meta.setIndexCount(1);
        meta.setSecondaryKeyHashType(HashType.NORMAL);
        this.wrappers.add(new CountsArrayCacheWrapper.GroupCountArrayCacheWrapper(meta));

        // did_scene__visit_incident_mcount__1h__slot
//        this.variableName = dimensionType + "__visit__scene_incident_count__1h__slot";
        this.variableName = dimensionType + "_scene__visit_incident_group_count__1h__slot";
        this.fieldName = DimensionHelper.getDimensionKey(dimensionType);

        this.dimensionType = dimensionType;
    }

    @Override
    public void compute(final Event event) {
        Map<String, List<String>> sceneStrategies = (Map<String, List<String>>) event.getPropertyValues().get("strategies");
        if (sceneStrategies != null) {
            String key = (String) event.getPropertyValues().get(fieldName);
            sceneStrategies.keySet().forEach(s -> {
                logger.warn("IncidentVariable compute step3,  id: {} , key: {} , sceneStrategies: {} , wrappers: {}",event.getId(),key, s,this.wrappers.get(0).getClass());
                this.wrappers.get(0).addData("", key, s);
            });
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
        if (keys == null || keys.length < 1) {
            return null;
        }
        return (Map) this.wrappers.get(0).getData(keys);
    }

    @Override
    public Object getAll(String key) {
        return this.wrappers.get(0).readAll(key);
    }

    @Override
    public DimensionType getDataDimension() {
        return dimensionType;
    }

    @Override
    public NodePrimaryData merge(NodePrimaryData data, String... keys) {
        return null;
    }

}
