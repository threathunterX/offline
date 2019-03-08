package com.threathunter.bordercollie.slot.compute.graph.extension.incident;

import com.threathunter.model.Event;

/**
 * Created by daisy on 17/3/31.
 */
public interface IncidentNode {
    void compute(Event event);

    String getName();
}
