package com.threathunter.bordercollie.slot.compute.graph;

import com.threathunter.bordercollie.slot.compute.cache.StorageType;
import com.threathunter.bordercollie.slot.util.DimensionHelper;
import com.threathunter.bordercollie.slot.util.HashType;
import com.threathunter.bordercollie.slot.util.SlotMetricsHelper;
import com.threathunter.common.Identifier;
import com.threathunter.config.CommonDynamicConfig;
import com.threathunter.model.Event;
import com.threathunter.model.VariableMeta;
import com.threathunter.variable.DimensionType;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Graph for the variable computation.
 * Every variable comes, it will find the first node by identifier,
 * then the node will do computation and transfer to other nodes.
 *
 * @author daisy
 * @since 1.4
 */
public class DimensionVariableGraphManager implements VariableGraphManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(DimensionVariableGraphManager.class);
    private static final Logger logger = LoggerFactory.getLogger("bordercollie");
    // no share graph compared to former version
    // shard graph, variable node does not require a cache
    private final VariableGraphProcessor[] shardGraphsProcessors;
    private final DimensionType graphDimension;
    private final HashType hashType;
    private final String shardKey;
    private final List<VariableMeta> metas;
    private final StorageType storage;

    private volatile Map<Identifier, VariableMeta> variableMetas;
    private final boolean isOffline = CommonDynamicConfig.getInstance().getBoolean("is_offline", false);
    private final int onlineDrainCount = CommonDynamicConfig.getInstance().getInt("online.slot.compute.drain.count", 100);

    public DimensionVariableGraphManager(final DimensionType dimensionType, final List<VariableMeta> dimensionVariableMetaList, final StorageType storageType) {
        this.graphDimension = dimensionType;
        if (dimensionType.equals(DimensionType.IP)) {
            this.hashType = HashType.IP;
        } else {
            this.hashType = HashType.NORMAL;
        }
        this.metas = dimensionVariableMetaList;
        this.shardKey = DimensionHelper.getDimensionKey(dimensionType);
        this.storage = storageType;
        LOGGER.trace("======\tmeta list content======", dimensionVariableMetaList.size());
        int j = 1;
        for (VariableMeta meta : dimensionVariableMetaList) {
            LOGGER.trace("======\t" + j++ + "\t{}\t=======", meta.getName());
        }
        shardGraphsProcessors = new VariableGraphProcessor[
                CommonDynamicConfig.getInstance().getInt("nebula.slot.graph.dimension.shard." + dimensionType.toString(), 3)];
        for (int i = 0; i < getShardGraphsProcessors().length; i++) {
            getShardGraphsProcessors()[i] = new VariableGraphProcessor(new VariableGraph(dimensionVariableMetaList, this.storage), "" + i);
            LOGGER.trace("manager:{},processor:{}", this.hashCode(), getShardGraphsProcessors()[i].hashCode());
        }
    }

    public void start() {
        for (int i = 0; i < getShardGraphsProcessors().length; i++) {
            getShardGraphsProcessors()[i].startProcessing();
        }
    }

    public void stop() {
        for (int i = 0; i < getShardGraphsProcessors().length; i++) {
            try {
                getShardGraphsProcessors()[i].stopProcessing();
            } catch (Exception e) {
                LOGGER.warn("stopping processor error", e);
            }
        }
    }

    public boolean compute(final Event computeEvent) {
        int hash = HashType.getHash((String) computeEvent.getPropertyValues().get(this.shardKey));
        int shard = hash < 0 ? (hash * -1 % this.getShardGraphsProcessors().length) : hash % this.getShardGraphsProcessors().length;
        logger.warn("DimensionVariable compute step1, id:{} ,  shardKey: {} , hash:{} , event: {} ",computeEvent.getId(),shardKey,hash,computeEvent);
        return this.getShardGraphsProcessors()[shard].addEvent(computeEvent);
    }

    // query from graph
    public void sendQueryEvent(final Event queryEvent) {
        int hash = HashType.getHash(queryEvent.getKey());
        int shard = hash < 0 ? (hash * -1 % this.getShardGraphsProcessors().length) : hash % this.getShardGraphsProcessors().length;
        this.getShardGraphsProcessors()[shard].addQueryEvent(queryEvent);
    }

    @Override
    public void sendQueryEvent(int shard, final Event queryEvent) {
        this.getShardGraphsProcessors()[shard].addQueryEvent(queryEvent);
    }

    public void broadcastQueryEvent(final Event queryEvent) {
        for (int i = 0; i < this.getShardGraphsProcessors().length; i++) {
            this.getShardGraphsProcessors()[i].addQueryEvent(queryEvent);
        }
    }

    @Override
    public Map<Integer, Collection<String>> groupShardKeys(final Collection<String> keys) {
        Map<Integer, Collection<String>> group = new HashMap<>(this.getShardGraphsProcessors().length);
        keys.forEach(k -> {
            int hash = HashType.getHash(k);
            int shard = hash < 0 ? (hash * -1 % this.getShardGraphsProcessors().length) : hash % this.getShardGraphsProcessors().length;
            group.computeIfAbsent(shard, s -> new ArrayList<>()).add(k);
        });
        return group;
    }

    @Override
    public boolean containsVariable(final Identifier id) {
        return false;
    }

    @Override
    public boolean containsKey(String key) {
        for (VariableGraphProcessor processor : getShardGraphsProcessors()) {
            if (processor.getVariableGraph().containsKey(key)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public VariableMeta getMeta(final Identifier id) {
        if (this.variableMetas == null)
            return null;
        return this.variableMetas.get(id);
    }

    public int getShardCount() {
        return this.getShardGraphsProcessors().length;
    }

    public void update(final List<VariableMeta> dimensionVariableMetaList) {
        Map<Identifier, VariableMeta> metaMap = new HashMap<>();
        for (VariableGraphProcessor processor : this.getShardGraphsProcessors()) {
            processor.updateGraph(new VariableGraph(dimensionVariableMetaList, this.storage));
        }
        dimensionVariableMetaList.forEach(meta -> metaMap.put(Identifier.fromKeys(meta.getApp(), meta.getName()),
                meta));
        this.variableMetas = ImmutableMap.copyOf(metaMap);
    }

    @Override
    public boolean isAllEmpty() {
        for (VariableGraphProcessor processor : this.getShardGraphsProcessors()) {
            if (!processor.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void clear() {
        for (VariableGraphProcessor processor : this.getShardGraphsProcessors()) {
//            processor.clear();
        }
    }

    public List<VariableCacheIterator> getCacheIterators() {
        List<VariableCacheIterator> list = new ArrayList<>();
        for (VariableGraphProcessor processor : this.getShardGraphsProcessors()) {
            list.addAll(processor.getCacheIterators());
        }
        return list;
    }

    public VariableGraphProcessor[] getShardGraphsProcessors() {
        return shardGraphsProcessors;
    }

    public HashType getHashType() {
        return hashType;
    }

    public List<VariableMeta> getMetas() {
        return metas;
    }

    public class VariableGraphProcessor extends Thread {
        private final String processorName;
        private volatile VariableGraph variableGraph;

        private final BlockingDeque<Event> processWaitingQueue;

        private volatile boolean running;
        private volatile boolean clearCache = false;

        public VariableGraphProcessor(final VariableGraph graph) {
            this(graph, "ComputeGraph");
        }

        public VariableGraphProcessor(final VariableGraph graph, final String name) {
            super(String.format("%s-%s", graphDimension, name));
            this.setDaemon(true);

            this.processorName = name;
            this.variableGraph = graph;
            LOGGER.trace("Processor:{},Graph:{}", VariableGraphProcessor.this.hashCode(), variableGraph.hashCode());
            this.processWaitingQueue = new LinkedBlockingDeque<>(
                    CommonDynamicConfig.getInstance().getInt("nebula.slot.sender.capacity", 10000));
        }

        public boolean isEmpty() {
            return this.processWaitingQueue.isEmpty();
        }

        public VariableGraph getVariableGraph() {
            return this.variableGraph;
        }

        public void updateGraph(final VariableGraph graph) {
            this.variableGraph = graph;
        }

        final List<VariableCacheIterator> getCacheIterators() {
            return this.variableGraph.getCacheIterators();
        }

        public void metricsEventCount(final String shard, final String eventName) {

        }

        public boolean addEvent(final Event event) {
            if (this.processWaitingQueue.offer(event)) {
                SlotMetricsHelper.getInstance().addMetrics("slot.events.graph.offer.count", 1.0,
                        "name", event.getName(), "shard", "" + processorName, "dimension", graphDimension.toString());
                return true;
            }
            SlotMetricsHelper.getInstance().addMetrics("slot.events.graph.drop.count", 1.0,
                    "name", event.getName(), "shard", "" + processorName, "dimension", graphDimension.toString());
            return false;

        }

        public void addQueryEvent(final Event event) {
            this.processWaitingQueue.offerFirst(event);
        }

        public void startProcessing() {
            if (this.running) {
                return;
            }
            this.running = true;
            this.start();
        }

        public void stopProcessing() throws InterruptedException {
            if (!this.running) {
                return;
            }
            this.running = false;
            this.join(1000);
        }

        @Override
        public void run() {
            int idle = 0;
            while (this.running) {
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
                            LOGGER.error("error in waiting for graph compute events", e);
                        }
                    }
                } else {
                    idle = 0;
                    events.forEach(event -> {
                        try {
                            variableGraph.process(event);
                        } catch (Exception e) {
                            LOGGER.error("process error", e);
                        }
                    });
                }
            }
        }

        public void clear() {
            this.clearCache = true;
        }
    }
}
