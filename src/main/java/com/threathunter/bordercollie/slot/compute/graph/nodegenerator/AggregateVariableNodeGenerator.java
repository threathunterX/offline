package com.threathunter.bordercollie.slot.compute.graph.nodegenerator;

import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapperMeta;
import com.threathunter.bordercollie.slot.compute.graph.node.AggregateVariableNode;
import com.threathunter.bordercollie.slot.compute.graph.node.VariableNode;
import com.threathunter.bordercollie.slot.compute.graph.node.propertyhandler.condition.PropertyConditionHandlerGenerator;
import com.threathunter.bordercollie.slot.compute.graph.node.propertyhandler.reduction.PropertyReductionHandlerGenerator;
import com.threathunter.bordercollie.slot.util.HashType;
import com.threathunter.common.Identifier;
import com.threathunter.variable.meta.AggregateVariableMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * 
 */
public class AggregateVariableNodeGenerator extends VariableNodeGenerator<AggregateVariableMeta> {
    private static final Logger LOGGER = LoggerFactory.getLogger("bordercollie");
    @Override
    public List<VariableNode> genVariableNodes() {
        AggregateVariableNode node = new AggregateVariableNode();
        AggregateVariableMeta meta = getVariableMeta();

        node.setMeta(meta);
        node.setGroupBys(meta.getGroupKeys());
        node.setPriority(meta.getPriority());
        node.setSrcIdentifiers(meta.getSrcVariableMetasID());
        node.setIdentifier(Identifier.fromKeys(meta.getApp(), meta.getName()));

        if (meta.getPropertyCondition() != null) {
            node.setConditionHandler(PropertyConditionHandlerGenerator.generateConditionHandler(
                    meta.getPropertyCondition()
            ));
        }


        // Aggregate Variable's cache wrapper cache type is depend on config's valueProperty's type.
        CacheWrapperMeta cacheWrapperMeta = new CacheWrapperMeta();
        cacheWrapperMeta.setStorageType(getStorageType());
        String reductionType = meta.getPropertyReduction().getType();
        cacheWrapperMeta.setValueHashType(HashType.NORMAL);
        cacheWrapperMeta.setSecondaryKeyHashType(HashType.NORMAL);
        int keysCount = 0;
        if (meta.getGroupKeys() != null) {
            keysCount = meta.getGroupKeys().size();
        }
        cacheWrapperMeta.setIndexCount(keysCount);
        cacheWrapperMeta.setCacheType(node.getCacheType(reductionType, null, keysCount));

        if (meta.getPropertyReduction() != null) {
            node.setReductionHandler(PropertyReductionHandlerGenerator.generateReductionHandler(cacheWrapperMeta,
                    meta.getGroupKeys(), meta.getPropertyReduction()));
        }

        node.setCacheWrappers(node.getReductionHandler().getCacheWrappers());
        LOGGER.warn("TopVariableNodeGenerator , meta:{}",meta.to_json_object());
        return Arrays.asList(node);
    }
}
