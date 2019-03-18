package com.threathunter.bordercollie.slot.compute.graph.nodegenerator;

import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapper;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapperFactory;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapperMeta;
import com.threathunter.bordercollie.slot.compute.graph.node.SequenceVariableNode;
import com.threathunter.bordercollie.slot.compute.graph.node.VariableNode;
import com.threathunter.bordercollie.slot.compute.graph.node.operator.OperatorGenerator;
import com.threathunter.common.Identifier;
import com.threathunter.model.Property;
import com.threathunter.variable.meta.SequenceVariableMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * 
 */
public class SequenceVariableNodeGenerator extends VariableNodeGenerator<SequenceVariableMeta> {
    private static final Logger LOGGER = LoggerFactory.getLogger("bordercollie");
    @Override
    public List<VariableNode> genVariableNodes() {
        SequenceVariableNode node = new SequenceVariableNode();
        SequenceVariableMeta meta = getVariableMeta();
        // Sequence Variable's cache wrapper cache type is depend on config's valueProperty's type.
        CacheWrapperMeta cacheWrapperMeta = new CacheWrapperMeta();
        cacheWrapperMeta.setStorageType(getStorageType());
        Property valueProperty = meta.getTargetProperty();
        String valuePropertyType = valueProperty.getType().toString();
        int keyCount = meta.getGroupKeys().size();
        cacheWrapperMeta.setCacheType(node.getCacheType(null, valuePropertyType, keyCount));

        node.setMeta(meta);
        node.setGroupBys(meta.getGroupKeys());
        node.setPriority(meta.getPriority());
        node.setIdentifier(Identifier.fromKeys(meta.getApp(), meta.getName()));
        node.setSrcIdentifiers(meta.getSrcVariableMetasID());
        node.setValueProperty(valueProperty);
        node.setOperator(OperatorGenerator.getOperator(meta.getOperation()));

        CacheWrapper cacheWrapper = CacheWrapperFactory.createCacheWrapper(cacheWrapperMeta);
        node.setCacheWrappers(Arrays.asList(cacheWrapper));
        LOGGER.warn("TopVariableNodeGenerator , meta:{}",meta.to_json_object());
        return Arrays.asList(node);
    }
}
