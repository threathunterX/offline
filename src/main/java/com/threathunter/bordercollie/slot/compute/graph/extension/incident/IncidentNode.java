package com.threathunter.bordercollie.slot.compute.graph.extension.incident;

import com.threathunter.model.Event;

/**
 * 
 */
public interface IncidentNode {
    void compute(Event event);

    String getName();
}
