package com.threathunter.bordercollie.slot.compute.graph.node;


import com.threathunter.bordercollie.slot.compute.VariableDataContext;
import com.threathunter.bordercollie.slot.compute.graph.node.propertyhandler.condition.PropertyConditionHandler;
import com.threathunter.bordercollie.slot.compute.graph.node.propertyhandler.mapping.PropertyMappingHandler;
import com.threathunter.bordercollie.slot.util.LogUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by daisy on 17/3/20.
 */
public class FilterVariableNode extends VariableNode {
    private List<PropertyMappingHandler> mappingHandlers;
    private PropertyConditionHandler conditionHandler;
    private boolean conditionBefore;
    Logger logger = LoggerFactory.getLogger(FilterVariableNode.class);

    public void setMappingHandlers(final List<PropertyMappingHandler> handlers) {
        this.mappingHandlers = handlers;
    }

    public void setConditionHandler(final PropertyConditionHandler handler) {
        this.conditionHandler = handler;
    }

    public void setConditionBefore(boolean conditionBefore) {
        this.conditionBefore = conditionBefore;
    }

    @Override
    public boolean compute(final VariableDataContext context) {
        if (this.conditionBefore) {
            if (!this.conditionHandler.match(context)) {
                return false;
            }
        }

        Map<String, Object> propertyValues = new HashMap<>();
        if (this.mappingHandlers != null) {
            this.mappingHandlers.forEach(mapping -> propertyValues.put(mapping.getPropertyName(), mapping.getMappedPropertyValue(context)));
        }

        if (!this.conditionBefore) {
            if (!this.conditionHandler.match(context)) {
                return false;
            }
        }

        propertyValues.forEach((propertyName, propertyValue) -> context.addContextValue(this.getIdentifier(), propertyName, propertyValue));
        context.addContextValue(this.getIdentifier(), "value", context.getFromContext(this.getSrcIdentifiers().get(0), "value"));
        context.addContextValue(this.getIdentifier(), "timestamp", context.getFromContext(this.getSrcIdentifiers().get(0), "timestamp"));
        LogUtil.print(context, logger);
        return true;
    }
}
