package com.threathunter.bordercollie.slot.compute.graph.query;

import java.util.concurrent.TimeUnit;

/**
 * 
 */
public interface VariableQuery {
    Object waitQueryResult(int timeout, TimeUnit unit);

    void addResult(Object result);
}
