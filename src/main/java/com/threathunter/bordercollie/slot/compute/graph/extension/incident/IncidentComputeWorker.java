package com.threathunter.bordercollie.slot.compute.graph.extension.incident;

import com.threathunter.bordercollie.slot.compute.cache.StorageType;
import com.threathunter.bordercollie.slot.compute.cache.storage.CacheStore;
import com.threathunter.bordercollie.slot.compute.cache.storage.CacheStoreFactory;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapper;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.array.CacheConstants;
import com.threathunter.bordercollie.slot.compute.graph.VariableCacheIterator;
import com.threathunter.bordercollie.slot.compute.graph.node.CacheNode;
import com.threathunter.bordercollie.slot.compute.graph.query.TopQuery;
import com.threathunter.bordercollie.slot.compute.graph.query.VariableQuery;
import com.threathunter.bordercollie.slot.util.SlotMetricsHelper;
import com.threathunter.common.Identifier;
import com.threathunter.config.CommonDynamicConfig;
import com.threathunter.model.Event;
import com.threathunter.model.VariableMeta;
import com.threathunter.variable.DimensionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created by daisy on 17/3/30.
 */
public class IncidentComputeWorker extends Thread {
    private static final Logger LOGGER = LoggerFactory.getLogger(IncidentComputeWorker.class);
    private static final Logger logger = LoggerFactory.getLogger("bordercollie");
    private static final int MAX_COUNT = 100;

    private volatile boolean running = false;
    private volatile boolean clearCache = false;
    private final StorageType storageType;

    private final BlockingDeque<Event> processWaitingQueue;
    private final Map<Identifier, IncidentNode> incidentNodeMap;
    private final List<CacheNode> cacheNodes;
    private final List<CacheWrapper> cacheWrappers;

    private final CacheStore cacheStore;
    private final String workerId;
    private final DimensionType dimensionType;

    private final boolean isOffline = CommonDynamicConfig.getInstance().getBoolean("is_offline", false);
    private final int onlineDrainCount = CommonDynamicConfig.getInstance().getInt("online.slot.compute.drain.count", 100);

    public IncidentComputeWorker(final StorageType type, final DimensionType dimension, final List<VariableMeta> variableMetas) {
        this(type, dimension, variableMetas, "0");
    }

    public IncidentComputeWorker(final StorageType type, final DimensionType dimension, final List<VariableMeta> variableMetas, final String id) {
        super("incidents computer");
        this.processWaitingQueue = new LinkedBlockingDeque<>(CommonDynamicConfig.getInstance().getInt("nebula.slot.sender.capacity", 10000));

        this.storageType = type;
        this.dimensionType = dimension;
        this.incidentNodeMap = IncidentNodeGenerator.getIncidentNode(dimensionType, type);

        this.cacheWrappers = new ArrayList<>();
        this.cacheNodes = new ArrayList<>();
        this.getIncidentNodeMap().values().forEach(node -> {
            if (node instanceof CacheNode) {
                this.cacheNodes.add((CacheNode) node);
                this.cacheWrappers.addAll(((CacheNode) node).getWrappers());
            }
        });
        this.cacheStore = CacheStoreFactory.newCacheStore(this.storageType, this.cacheWrappers);
        this.setDaemon(true);

        this.workerId = id;
    }

    public boolean isEmpty() {
        return this.processWaitingQueue.isEmpty();
    }

    public boolean addEvent(final Event event) {
        if (this.processWaitingQueue.offer(event)) {
            SlotMetricsHelper.getInstance().addMetrics("slot.events.incident.offer.count", 1.0,
                    "name", event.getName(), "shard", this.workerId, "dimension", dimensionType.toString());
            return true;
        } else {
            SlotMetricsHelper.getInstance().addMetrics("slot.events.incident.drop.count", 1.0,
                    "name", event.getName(), "shard", this.workerId, "dimension", dimensionType.toString());
            return false;
        }
    }

    public void addQueryEvent(final Event queryEvent) {
        this.processWaitingQueue.offerFirst(queryEvent);
    }

    @Override
    public void run() {
        if (running) {
            LOGGER.warn("already running");
            return;
        }
        running = true;
        int idle = 0;
        while (running) {
            List<Event> events = new ArrayList<>();
            if (isOffline) {
                this.processWaitingQueue.drainTo(events);
            } else {
                this.processWaitingQueue.drainTo(events, onlineDrainCount);
            }
            if (events.isEmpty()) {
                idle++;
                if (idle >= 3) {
                    try {
                        Thread.sleep(100);
                    } catch (Exception e) {
                        LOGGER.error("error in waiting for incident compute events", e);
                    }
                }
            } else {
                idle = 0;
                events.forEach(event -> {
                    try {
                        /*if (this.clearCache) {
                            this.cacheStore.clearAll();
                            this.cacheNodes.forEach(node -> node.clearTop());
                            this.clearCache = false;
                        }*/

                        this.process(event);
                    } catch (Exception e) {
                        LOGGER.error("process error", e);
                    }
                });
            }
        }
    }

    public boolean containsKey(final String key) {
        return this.cacheStore.getCache(key) != null;
    }

    private void process(final Event event) {
        if (event.getName().equals("__query__")) {
            try {
                processQuery(event);
            } catch (Exception e) {
                LOGGER.error("query error", e);
            }
        } else {
            this.getIncidentNodeMap().values().forEach(node -> {
                try {
                    logger.warn("IncidentVariable compute step2,  id: {} , node: {}",event.getId(),node.getClass());
                    node.compute(event);
                } catch (Exception e) {
                    SlotMetricsHelper.getInstance().addMetrics("slot.node.incident.compute.error.count", 1.0,
                            "name", event.getName(), "dimension", dimensionType.toString(), "node", node.getName());
                }
            });
            SlotMetricsHelper.getInstance().addMetrics("slot.events.incident.compute.count", 1.0,
                    "name", event.getName(), "shard", this.workerId, "dimension", dimensionType.toString());
        }
    }

    private void processQuery(final Event event) {
        Identifier variableId = (Identifier) event.getPropertyValues().get("id");
        if (variableId.getKeys().get(1).equals("ip__visit_incident_score_top100__1h__slot")) {
            variableId = Identifier.fromKeys("nebula", "ip__visit_incident_score__1h__slot");
        }
        if (!this.getIncidentNodeMap().containsKey(variableId)) {
            return;
        }
        IncidentNode node = this.getIncidentNodeMap().get(variableId);
        if (!(node instanceof CacheNode)) {
            return;
        }

        VariableQuery query = (VariableQuery) event.getPropertyValues().get("query");
        CacheNode cacheNode = (CacheNode) node;
        try {
            if (event.getKey().equals("__batch__")) {
                Map<String, Object> result = new HashMap<>();
                ((Collection<String>) event.getPropertyValues().get("keys")).forEach(k -> result.put(k,
                        cacheNode.getData(k)));
                query.addResult(result);
                LOGGER.info("Incident __batch__ query result:{}", result);
            } else {
                if (event.getPropertyValues().containsKey("sub_keys")) {
                    Map<String, Object> subMap = new HashMap<>();
                    ((Collection<String>) event.getPropertyValues().get("sub_keys")).forEach(secondKey ->
                            subMap.put(secondKey, cacheNode.getData(event.getKey(), secondKey)));
                    query.addResult(subMap);
                    LOGGER.info("Incident sub_keys query result:{}", event.getPropertyValues().get("sub_keys"));
                } else {
                    query.addResult(cacheNode.getData(event.getKey()));
                    LOGGER.info("Incident query result:{}", cacheNode.getData(event.getKey()));
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("query failed, input event = " + event, e);
        }
    }

    public List<VariableCacheIterator> getCacheIterators() {
        Map<DimensionType, List<CacheNode>> dimensionedNodes = new HashMap<>();
        this.cacheNodes.stream().forEach(n -> {
            dimensionedNodes.computeIfAbsent(n.getDataDimension(), k -> new ArrayList<>()).add(n);
        });
        List<VariableCacheIterator> iterators = new ArrayList<>(3);
        dimensionedNodes.forEach((dimension, nodes) -> {
            if (dimension.equals(DimensionType.GLOBAL)) {
                iterators.add(new VariableCacheIterator(new Iterator<String>() {
                    boolean get = false;

                    @Override
                    public boolean hasNext() {
                        return !get;
                    }

                    @Override
                    public String next() {
                        get = true;
                        return CacheConstants.GLOBAL_KEY;
                    }
                }, DimensionType.GLOBAL, nodes));
            } else {
                iterators.add(new VariableCacheIterator(cacheStore.getKeyIterator(), dimension, nodes));
            }
        });
        return iterators;
    }

    public void clear() {
        this.clearCache = true;
    }

    public Map<Identifier, IncidentNode> getIncidentNodeMap() {
        return incidentNodeMap;
    }
}
