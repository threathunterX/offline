package com.threathunter.bordercollie.slot.compute.graph.nodegenerator;

import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapper;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapperFactory;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapperMeta;
import com.threathunter.bordercollie.slot.compute.graph.node.DualvarVariableNode;
import com.threathunter.bordercollie.slot.compute.graph.node.VariableNode;
import com.threathunter.common.Identifier;
import com.threathunter.model.Property;
import com.threathunter.variable.meta.DualVariableMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * Created by toyld on 3/28/17.
 */
public class DualvarVariableNodeGenerator extends VariableNodeGenerator<DualVariableMeta> {
    private static final Logger LOGGER = LoggerFactory.getLogger("bordercollie");
    @Override
    public List<VariableNode> genVariableNodes() {
        DualvarVariableNode node = new DualvarVariableNode();
        DualVariableMeta meta = getVariableMeta();
        // Dual Variable's cache wrapper cache type is depend on config's valueProperty's type.

        CacheWrapperMeta cacheWrapperMeta = new CacheWrapperMeta();
        cacheWrapperMeta.setStorageType(getStorageType());
        Property valueProperty = meta.getFirstProperty();
        String valuePropertyType = valueProperty.getType().toString();
        int keyCount = meta.getGroupKeys().size();

        if (meta.getOperation().equals("/")) {
            valuePropertyType = "double";
        }
        cacheWrapperMeta.setCacheType(node.getCacheType(null, valuePropertyType, keyCount));

        node.setMeta(meta);
        node.setPriority(meta.getPriority());
        node.setIdentifier(Identifier.fromKeys(meta.getApp(), meta.getName()));
        node.setSrcIdentifiers(meta.getSrcVariableMetasID());
        node.setGroupBys(meta.getGroupKeys());
        node.setFirstVariableId(meta.getFirstId());
        node.setSecondVariableId(meta.getSecondId());

        CacheWrapper cacheWrapper = CacheWrapperFactory.createCacheWrapper(cacheWrapperMeta);
        node.setCacheWrappers(Arrays.asList(cacheWrapper));
        LOGGER.warn("TopVariableNodeGenerator , meta:{}",meta.to_json_object());
        return Arrays.asList(node);
    }
}
