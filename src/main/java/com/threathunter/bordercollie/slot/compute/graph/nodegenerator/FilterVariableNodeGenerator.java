package com.threathunter.bordercollie.slot.compute.graph.nodegenerator;

import com.threathunter.bordercollie.slot.compute.graph.node.FilterVariableNode;
import com.threathunter.bordercollie.slot.compute.graph.node.VariableNode;
import com.threathunter.bordercollie.slot.compute.graph.node.propertyhandler.condition.PropertyConditionHandlerGenerator;
import com.threathunter.bordercollie.slot.compute.graph.node.propertyhandler.mapping.PropertyMappingHandler;
import com.threathunter.bordercollie.slot.compute.graph.node.propertyhandler.mapping.PropertyMappingHandlerGenerator;
import com.threathunter.common.Identifier;
import com.threathunter.model.Property;
import com.threathunter.model.PropertyCondition;
import com.threathunter.model.PropertyMapping;
import com.threathunter.variable.meta.FilterVariableMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by daisy on 17/3/26.
 */
public class FilterVariableNodeGenerator extends VariableNodeGenerator<FilterVariableMeta> {
    private static final Logger LOGGER = LoggerFactory.getLogger("bordercollie");
    @Override
    public List<VariableNode> genVariableNodes() {
        FilterVariableNode node = new FilterVariableNode();
        FilterVariableMeta meta = getVariableMeta();
        node.setPriority(meta.getPriority());
        node.setMeta(meta);

        node.setSrcIdentifiers(meta.getSrcVariableMetasID());
        node.setIdentifier(Identifier.fromKeys(meta.getApp(), meta.getName()));

        PropertyCondition condition = meta.getPropertyCondition();
        if (condition != null) {
            List<Property> srcProperty = condition.getSrcProperties();
            if (srcProperty != null && srcProperty.get(0).getIdentifier() != null) {
                node.setConditionBefore(true);
            } else {
                node.setConditionBefore(false);
            }
            node.setConditionHandler(PropertyConditionHandlerGenerator.generateConditionHandler(meta.getPropertyCondition()));
        }

        List<PropertyMapping> mappings = meta.getPropertyMappings();
        if (mappings != null && mappings.size() > 0) {
            List<PropertyMappingHandler> mappingHandlers = new ArrayList<>();
            mappings.forEach(mapping -> mappingHandlers.add(
                    PropertyMappingHandlerGenerator.generateMappingHandler(mapping)
            ));
            node.setMappingHandlers(mappingHandlers);
        }
        LOGGER.warn("TopVariableNodeGenerator , meta:{}",meta.to_json_object());
        return Arrays.asList(node);
    }
}
