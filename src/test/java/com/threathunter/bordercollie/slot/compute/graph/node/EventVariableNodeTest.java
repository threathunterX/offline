package com.threathunter.bordercollie.slot.compute.graph.node;

import com.threathunter.bordercollie.slot.compute.VariableDataContext;
import com.threathunter.mock.util.HttpDynamicEventMaker;
import com.threathunter.model.Event;
import org.junit.Test;

/**
 * Created by daisy on 17/3/25.
 */
public class EventVariableNodeTest {
    private final HttpDynamicEventMaker eventMaker = new HttpDynamicEventMaker(10);

    @Test
    public void testContextFields() {
        EventVariableNode node = new EventVariableNode();

        Event event = eventMaker.nextEvent();
        VariableDataContext context = node.computeEvent(event);
    }
}
