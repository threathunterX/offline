package com.threathunter.bordercollie.slot.util;

import com.threathunter.bordercollie.slot.compute.graph.VariableGraphManager;
import com.threathunter.bordercollie.slot.compute.graph.query.*;
import com.threathunter.common.Identifier;
import com.threathunter.model.Event;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class VariableQueryUtil {
    public static enum VariableQueryType {
        GLOBAL_TOP,
        GLOBAL_COUNT,
        KEY_TOP,
        KEY_VALUE
    }

    public static VariableQuery broadcastTopQuery(final VariableGraphManager manager, final Identifier id, int topCount) {
        TopKeyVariableQuery query = new TopKeyVariableQuery(manager.getShardCount(), topCount);
        Event queryEvent = new Event("nebula", "__query__", "");
        queryEvent.setTimestamp(SystemClock.getCurrentTimestamp());
        Map<String, Object> properties = new HashMap<>();
        properties.put("topCount", topCount);
        properties.put("query", query);
        properties.put("id", id);
        properties.put("type", VariableQueryType.GLOBAL_TOP);
        queryEvent.setPropertyValues(properties);

        manager.broadcastQueryEvent(queryEvent);
        return query;
    }

    public static VariableQuery broadcastQuery(final VariableGraphManager manager, final Identifier id) {
        GlobalValueVariableQuery query = new GlobalValueVariableQuery(manager.getShardCount());

        Event event = new Event("nebula", "__query__", "");
        Map<String, Object> properties = new HashMap<>();
        event.setTimestamp(SystemClock.getCurrentTimestamp());
        event.setValue(1.0);
        properties.put("query", query);
        properties.put("id", id);
        properties.put("type", VariableQueryType.GLOBAL_COUNT);
        event.setPropertyValues(properties);

        manager.broadcastQueryEvent(event);
        return query;
    }

    public static VariableQuery sendKeyTopQuery(final VariableGraphManager manager, final Identifier id, final String key, int topCount) {
        KeyValueTopVariableQuery query = new KeyValueTopVariableQuery(topCount);
        Event event = new Event("nebula", "__query__", key);

        Map<String, Object> properties = new HashMap<>();
        event.setTimestamp(SystemClock.getCurrentTimestamp());
        event.setValue(1.0);
        properties.put("query", query);
        properties.put("id", id);
        properties.put("topCount", topCount);
        properties.put("type", VariableQueryType.KEY_TOP);
        event.setPropertyValues(properties);

        manager.sendQueryEvent(event);
        return query;
    }

    public static VariableQuery sendKeyTopQuery(final VariableGraphManager manager, final Identifier id, final Collection<String> keys, int topCount) {
        Map<Integer, Collection<String>> shardKeys = manager.groupShardKeys(keys);
        KeyValueTopVariableQuery query = new KeyValueTopVariableQuery(shardKeys.size(), topCount);
        shardKeys.forEach((shard, list) -> {
            Event event = new Event("nebula", "__query__", "__batch__");
            Map<String, Object> properties = new HashMap<>();
            event.setTimestamp(SystemClock.getCurrentTimestamp());
            event.setValue(1.0);
            properties.put("query", query);
            properties.put("keys", list);
            properties.put("topCount", topCount);
            properties.put("id", id);
            properties.put("type", VariableQueryType.KEY_TOP);
            event.setPropertyValues(properties);

            manager.sendQueryEvent(shard, event);
        });

        return query;
    }

    public static VariableQuery sendKeyQuery(final VariableGraphManager manager, final Identifier id, final String key) {
        KeyValueVariableQuery query = new KeyValueVariableQuery();

        Event event = new Event("nebula", "__query__", key);
        Map<String, Object> properties = new HashMap<>();
        event.setTimestamp(SystemClock.getCurrentTimestamp());
        event.setValue(1.0);
        properties.put("query", query);
        properties.put("id", id);
        properties.put("type", VariableQueryType.KEY_VALUE);
        event.setPropertyValues(properties);

        manager.sendQueryEvent(event);
        return query;
    }

    public static VariableQuery sendKeyQuery(final VariableGraphManager manager, final Identifier id, final Collection<String> keys) {
        // keys grouped by shard first
        Map<Integer, Collection<String>> shardKeys = manager.groupShardKeys(keys);
        KeyValueVariableQuery query = new KeyValueVariableQuery(shardKeys.size());
        shardKeys.forEach((shard, list) -> {
            Event event = new Event("nebula", "__query__", "__batch__");
            Map<String, Object> properties = new HashMap<>();
            event.setTimestamp(SystemClock.getCurrentTimestamp());
            event.setValue(1.0);
            properties.put("query", query);
            properties.put("keys", list);
            properties.put("id", id);
            properties.put("type", VariableQueryType.KEY_VALUE);
            event.setPropertyValues(properties);

            manager.sendQueryEvent(shard, event);
        });

        return query;
    }

    public static VariableQuery sendKeyQuery(final VariableGraphManager manager, final Identifier id, final String firstKey, final Collection<String> secondKeys) {
        KeyValueVariableQuery query = new KeyValueVariableQuery();

        Event event = new Event("nebula", "__query__", firstKey);
        Map<String, Object> properties = new HashMap<>();
        event.setTimestamp(SystemClock.getCurrentTimestamp());
        event.setValue(1.0);
        properties.put("query", query);
        properties.put("sub_keys", secondKeys);
        properties.put("id", id);
        properties.put("type", VariableQueryType.KEY_VALUE);
        event.setPropertyValues(properties);

        manager.sendQueryEvent(event);
        return query;
    }
}