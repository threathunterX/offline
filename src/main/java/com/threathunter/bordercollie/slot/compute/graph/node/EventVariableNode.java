package com.threathunter.bordercollie.slot.compute.graph.node;

import com.threathunter.bordercollie.slot.compute.VariableDataContext;
import com.threathunter.bordercollie.slot.util.LogUtil;
import com.threathunter.model.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
public class EventVariableNode extends VariableNode {
    Logger logger = LoggerFactory.getLogger(EventVariableNode.class);

    @Override
    public boolean compute(final VariableDataContext holder) {
        return true;
    }

    public VariableDataContext computeEvent(final Event event) {
        VariableDataContext context = new VariableDataContext();

        event.getPropertyValues().forEach((property, value) ->
                context.addContextValue(this.getIdentifier(), property, value));

        context.addContextValue(this.getIdentifier(), "app", event.getApp());
        context.addContextValue(this.getIdentifier(), "name", event.getName());
        context.addContextValue(this.getIdentifier(), "key", event.getKey());
        context.addContextValue(this.getIdentifier(), "timestamp", event.getTimestamp());
        context.addContextValue(this.getIdentifier(), "value", event.value());
        context.addContextValue("level", 1);
        LogUtil.print(context, logger);
        return context;
    }
}
