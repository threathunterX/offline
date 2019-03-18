package com.threathunter.bordercollie.slot.compute.graph.extension.incident;

import com.threathunter.bordercollie.slot.compute.cache.StorageType;
import com.threathunter.bordercollie.slot.compute.graph.VariableCacheIterator;
import com.threathunter.bordercollie.slot.compute.graph.VariableGraphManager;
import com.threathunter.bordercollie.slot.util.DimensionHelper;
import com.threathunter.bordercollie.slot.util.HashType;
import com.threathunter.common.Identifier;
import com.threathunter.config.CommonDynamicConfig;
import com.threathunter.model.Event;
import com.threathunter.model.VariableMeta;
import com.threathunter.variable.DimensionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * 
 */
public class IncidentVariableGraphManager implements VariableGraphManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(IncidentVariableGraphManager.class);
    private static final Logger logger = LoggerFactory.getLogger("bordercollie");
    private final IncidentComputeWorker[] workers;
    private final String shardKey = DimensionHelper.getDimensionKey(DimensionType.IP);

    private final StorageType storageType;
    private final Map<Identifier, VariableMeta> variableMetaMap;

    private final DimensionType dimensionType;
    private final HashType hashType;

    public IncidentVariableGraphManager(final DimensionType dimension, final List<VariableMeta> metaList, final StorageType type) {
        this.dimensionType = dimension;
        if (dimensionType.equals(DimensionType.IP)) {
            this.hashType = HashType.IP;
        } else {
            this.hashType = HashType.NORMAL;
        }
        this.storageType = type;
        this.workers = new IncidentComputeWorker[
                CommonDynamicConfig.getInstance().getInt("nebula.slot.incident.shard." + dimension.toString(), 1)];
        for (int i = 0; i < this.getWorkers().length; i++) {
            this.getWorkers()[i] = new IncidentComputeWorker(this.storageType, dimensionType, metaList, "" + i);
        }

        this.variableMetaMap = new HashMap<>();
        metaList.forEach(meta -> this.variableMetaMap.put(Identifier.fromKeys(meta.getApp(), meta.getName()), meta));
    }

    public void start() {
        for (int i = 0; i < this.getWorkers().length; i++) {
            this.getWorkers()[i].start();
        }
    }

    public void stop() {
        for (int i = 0; i < this.getWorkers().length; i++) {
            try {
                this.getWorkers()[i].join(1000);
            } catch (Exception e) {
                LOGGER.error("error when stopping incident compute workers", e);
            }
        }
    }

    @Override
    public void update(List<VariableMeta> metaList) {
        return;
    }

    @Override
    public boolean isAllEmpty() {
        for (IncidentComputeWorker worker : this.getWorkers()) {
            if (!worker.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void clear() {
        for (IncidentComputeWorker worker : this.getWorkers()) {
            worker.clear();
        }
    }

    public boolean compute(final Event incidentEvent) {
        String notices = (String) incidentEvent.getPropertyValues().get("notices");
        if (notices == null || notices.isEmpty()) {
            return true;
        }
        logger.warn("IncidentVariable compute step1, id: {} , notices: {} , event : {}" , incidentEvent.getId(),notices,incidentEvent);
        if (this.shardKey == null) {
            if (this.getWorkers()[0].addEvent(incidentEvent)) {
                return true;
            }
            return false;
        }
        int hash = HashType.getHash((String) incidentEvent.getPropertyValues().get(shardKey));
        int shard = hash < 0 ? ((hash * -1) % this.getWorkers().length) : hash % this.getWorkers().length;
        if (this.getWorkers()[shard].addEvent(incidentEvent)) {
            return true;
        }
        return false;
    }

    public void sendQueryEvent(final Event queryEvent) {
        if (this.shardKey == null) {
            this.getWorkers()[0].addQueryEvent(queryEvent);
            return;
        }
        int hash = HashType.getHash(queryEvent.getKey());
        int shard = hash < 0 ? ((hash * -1) % this.getWorkers().length) : hash % this.getWorkers().length;
        this.getWorkers()[shard].addQueryEvent(queryEvent);
    }

    @Override
    public void sendQueryEvent(int shard, final Event queryEvent) {
        this.getWorkers()[shard].addQueryEvent(queryEvent);
    }

    public void broadcastQueryEvent(final Event queryEvent) {
        for (int i = 0; i < this.getWorkers().length; i++) {
            this.getWorkers()[i].addQueryEvent(queryEvent);
        }
    }

    @Override
    public Map<Integer, Collection<String>> groupShardKeys(final Collection<String> keys) {
        Map<Integer, Collection<String>> group = new HashMap<>();
        if (this.shardKey == null) {
            group.put(0, keys);
        } else {
            keys.forEach(k -> {
                int hash = HashType.getHash(k);
                int shard = hash < 0 ? ((hash * -1) % this.getWorkers().length) : hash % this.getWorkers().length;
                group.computeIfAbsent(shard, s -> new ArrayList<>()).add(k);
            });
        }
        return group;
    }

    @Override
    public boolean containsVariable(final Identifier id) {
        return this.variableMetaMap.containsKey(id);
    }

    @Override
    public boolean containsKey(final String key) {
        for (IncidentComputeWorker worker : getWorkers()) {
            if (worker.containsKey(key)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public VariableMeta getMeta(final Identifier id) {
        return this.variableMetaMap.get(id);
    }

    @Override
    public List<VariableCacheIterator> getCacheIterators() {
        List<VariableCacheIterator> list = new ArrayList<>();
        for (IncidentComputeWorker worker : this.getWorkers()) {
            list.addAll(worker.getCacheIterators());
        }
        return list;
    }

    public int getShardCount() {
        return this.getWorkers().length;
    }

    public IncidentComputeWorker[] getWorkers() {
        return workers;
    }
}
