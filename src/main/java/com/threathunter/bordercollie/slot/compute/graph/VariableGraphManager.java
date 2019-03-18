package com.threathunter.bordercollie.slot.compute.graph;

import com.threathunter.common.Identifier;
import com.threathunter.model.Event;
import com.threathunter.model.VariableMeta;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * 
 */
public interface VariableGraphManager {
    int getShardCount();

    boolean compute(Event computeEvent);

    void sendQueryEvent(Event queryEvent);

    void sendQueryEvent(int shard, Event queryEvent);

    void broadcastQueryEvent(Event queryEvent);

    Map<Integer, Collection<String>> groupShardKeys(Collection<String> keys);

    boolean containsVariable(Identifier id);

    boolean containsKey(String key);

    VariableMeta getMeta(Identifier id);

    List<VariableCacheIterator> getCacheIterators();

    void start();

    void stop();

    void update(List<VariableMeta> metaList);

    boolean isAllEmpty();

    void clear();
}