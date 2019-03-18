package com.threathunter.bordercollie.slot.compute.graph.nodegenerator;

import com.threathunter.bordercollie.slot.compute.graph.node.EventVariableNode;
import com.threathunter.bordercollie.slot.compute.graph.node.VariableNode;
import com.threathunter.common.Identifier;
import com.threathunter.variable.meta.EventVariableMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * 
 */
public class EventVariableNodeGenerator extends VariableNodeGenerator<EventVariableMeta> {
    private static final Logger LOGGER = LoggerFactory.getLogger("bordercollie");
    @Override
    public List<VariableNode> genVariableNodes() {
        EventVariableNode node = new EventVariableNode();
        EventVariableMeta meta = getVariableMeta();

        node.setMeta(meta);
        node.setPriority(meta.getPriority());
        node.setIdentifier(Identifier.fromKeys(meta.getApp(), meta.getName()));
        node.setSrcIdentifiers(meta.getSrcVariableMetasID());
        LOGGER.warn("TopVariableNodeGenerator , meta:{}",meta.to_json_object());
        return Arrays.asList(node);
    }
}
