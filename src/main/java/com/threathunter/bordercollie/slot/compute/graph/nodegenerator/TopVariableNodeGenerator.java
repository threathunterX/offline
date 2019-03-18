package com.threathunter.bordercollie.slot.compute.graph.nodegenerator;

import com.threathunter.bordercollie.slot.compute.graph.node.TopVariableNode;
import com.threathunter.bordercollie.slot.compute.graph.node.VariableNode;
import com.threathunter.bordercollie.slot.compute.graph.node.propertyhandler.condition.PropertyConditionHandlerGenerator;
import com.threathunter.bordercollie.slot.compute.graph.node.propertyhandler.reduction.PropertyReductionHandlerGenerator;
import com.threathunter.bordercollie.slot.compute.graph.node.propertyhandler.reduction.TopReductionHandler;
import com.threathunter.bordercollie.slot.util.TopType;
import com.threathunter.common.Identifier;
import com.threathunter.variable.meta.TopVariableMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * 
 */
public class TopVariableNodeGenerator extends VariableNodeGenerator<TopVariableMeta> {
    private static final Logger LOGGER = LoggerFactory.getLogger("bordercollie");
    @Override
    public List<VariableNode> genVariableNodes() {
        TopVariableNode node = new TopVariableNode();
        TopVariableMeta meta = getVariableMeta();
        node.setMeta(meta);
        node.setGroupBys(meta.getGroupKeys());
        node.setPriority(meta.getPriority());
        node.setSrcIdentifiers(meta.getSrcVariableMetasID());
        node.setIdentifier(Identifier.fromKeys(meta.getApp(), meta.getName()));
        if (node.getGroupBys() == null || node.getGroupBys().size() <= 0) {
            node.setToptype(TopType.SINGLE);
        } else {
            node.setToptype(TopType.DOUBLE);
        }
        if (meta.getPropertyCondition() != null) {
            node.setConditionHandler(PropertyConditionHandlerGenerator.generateConditionHandler(
                    meta.getPropertyCondition()
            ));
        }
        if (meta.getPropertyReduction() != null) {
            node.setReductionHandler((TopReductionHandler) PropertyReductionHandlerGenerator.generateReductionHandler(null, meta.getGroupKeys(),
                    meta.getPropertyReduction()));
        }
        LOGGER.warn("TopVariableNodeGenerator , meta:{}",meta.to_json_object());
//        node.setCacheWrappers(node.get);
        return Arrays.asList(node);
    }
}
