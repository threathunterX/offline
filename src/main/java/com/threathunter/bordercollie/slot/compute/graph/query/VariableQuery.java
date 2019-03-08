package com.threathunter.bordercollie.slot.compute.graph.query;

import java.util.concurrent.TimeUnit;

/**
 * Created by daisy on 17/4/3.
 */
public interface VariableQuery {
    Object waitQueryResult(int timeout, TimeUnit unit);

    void addResult(Object result);
}
