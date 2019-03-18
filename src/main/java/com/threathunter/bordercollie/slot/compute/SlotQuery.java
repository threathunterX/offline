package com.threathunter.bordercollie.slot.compute;


import com.threathunter.bordercollie.slot.compute.graph.DimensionVariableGraphManager;
import com.threathunter.bordercollie.slot.compute.graph.VariableGraph;
import com.threathunter.bordercollie.slot.compute.graph.VariableGraphManager;
import com.threathunter.bordercollie.slot.compute.graph.extension.incident.IncidentComputeWorker;
import com.threathunter.bordercollie.slot.compute.graph.extension.incident.IncidentNode;
import com.threathunter.bordercollie.slot.compute.graph.extension.incident.IncidentVariableGraphManager;
import com.threathunter.bordercollie.slot.compute.graph.extension.incident.IncidentVariableMetaRegister;
import com.threathunter.bordercollie.slot.compute.graph.node.CacheNode;
import com.threathunter.bordercollie.slot.compute.graph.node.NodePrimaryData;
import com.threathunter.bordercollie.slot.compute.graph.node.TopVariableNode;
import com.threathunter.bordercollie.slot.compute.graph.node.VariableNode;
import com.threathunter.bordercollie.slot.compute.graph.query.VariableQuery;
import com.threathunter.bordercollie.slot.util.MetaUtil;
import com.threathunter.bordercollie.slot.util.ResultFormatter;
import com.threathunter.bordercollie.slot.util.VariableQueryUtil;
import com.threathunter.common.Identifier;
import com.threathunter.model.VariableMeta;
import com.threathunter.variable.DimensionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * 
 */
public class SlotQuery implements SlotQueryable {
    private static Logger logger = LoggerFactory.getLogger(SlotQuery.class);
    private final SlotComputable engine;

    public SlotQuery(SlotComputable engine) {
        this.engine = engine;
    }

    @Override
    public Object queryPrevious(Identifier identifier, Collection<String> keys) {
        Map<Long, SlotWindow> allSlots = engine.getAllSlots();
        Map<Collection<String>, Map<String, Object>> results = new HashMap<>();
        allSlots.forEach((k, v) -> {
            SlotWindow window = v;
            Identifier id = identifier;
            VariableMeta meta = null;
            VariableGraphManager manager = null;
            Map<DimensionType, DimensionVariableGraphManager> dimensionedGraphManagers = window.getDimensionedGraphManagers();
            Map<DimensionType, IncidentVariableGraphManager> dimensionedIncidentVariableGraphManager = window.getDimensionedIncidentVariableGraphManager();
            List<VariableMeta> metas = window.getMetas();
            if (MetaUtil.getMetas(metas, id) != null) {
                meta = MetaUtil.getMetas(metas, id);
                manager = dimensionedGraphManagers.get(DimensionType.valueOf(meta.getDimension().toUpperCase()));
            } else {
                meta = IncidentVariableMetaRegister.getMeta(id);
                manager = dimensionedIncidentVariableGraphManager.get(DimensionType.valueOf(meta.getDimension().toUpperCase()));
            }
            if (meta == null) {
                logger.error("variable does not exist");
                return;
            }
            if (manager instanceof DimensionVariableGraphManager) {
                DimensionVariableGraphManager dimensionManager = (DimensionVariableGraphManager) manager;
                DimensionVariableGraphManager.VariableGraphProcessor[] shardGraphsProcessors = dimensionManager.getShardGraphsProcessors();
                //TODO YY
                /*int hash = HashType.getMurMurHash(dimensionManager.getHashType(),  DimensionHelper.getDimensionKey(DimensionType.valueOf(meta.getDimension().toUpperCase())));
                int shard = hash < 0 ? (hash * -1 % dimensionManager.getShardGraphsProcessors().length) : hash % shardGraphsProcessors.length;*/
                Map<Integer, Collection<String>> shardKeys = manager.groupShardKeys(keys);
                shardKeys.forEach((shard, sks) -> {
                    DimensionVariableGraphManager.VariableGraphProcessor shardGraphsProcessor = shardGraphsProcessors[shard];
                    VariableGraph variableGraph = shardGraphsProcessor.getVariableGraph();
                    Map<Identifier, VariableNode> variableMap = variableGraph.getVariableMap();
                    VariableNode variableNode = variableMap.get(identifier);
                    if (variableNode instanceof TopVariableNode) {
                        Object data = ((TopVariableNode) variableNode).getData();
                        results.put(keys, ResultFormatter.parse(k, data));
                    } else if (variableNode instanceof CacheNode) {
                        Object data = ((CacheNode) variableNode).getData(sks.toArray(new String[]{}));
                        results.put(keys, ResultFormatter.parse(k, data));
                    } else
                        results.put(keys, ResultFormatter.parse(k, ""));
                });
            } else {
                IncidentVariableGraphManager incidentManager = (IncidentVariableGraphManager) manager;
                IncidentComputeWorker[] workers = incidentManager.getWorkers();
                Map<Integer, Collection<String>> shardKeys = incidentManager.groupShardKeys(keys);
                shardKeys.forEach((shard, sks) -> {
                    IncidentComputeWorker shardGraphsProcessor = workers[shard];
                    Map<Identifier, IncidentNode> incidentNodeMap = shardGraphsProcessor.getIncidentNodeMap();
                    CacheNode variableNode = (CacheNode) incidentNodeMap.get(identifier);
                    if (variableNode instanceof TopVariableNode) {
                        Object data = ((TopVariableNode) variableNode).getData();
                        results.put(keys, ResultFormatter.parse(k, data));
                    } else if (variableNode instanceof CacheNode) {
                        Object data = ((CacheNode) variableNode).getData(sks.toArray(new String[]{}));
                        results.put(keys, ResultFormatter.parse(k, data));
                    } else
                        results.put(keys, ResultFormatter.parse(k, ""));
                });
            }

        });
        logger.info("query previous slot return, results = {}", results);
        return results;
    }

    @Override
    public Object mergePrevious(Identifier identifier, Collection<String> keys) {
        if (keys == null) {
            logger.warn("query merge previous, keys = NULL");
            return null;
        } else {
            logger.info("query merge previous, identifier = {}, keys = {} ", identifier, keys.toArray(new String[]{}));
        }
        Map<Long, SlotWindow> allSlots = engine.getAllSlots();
        Map<Collection<String>, Map<String, Object>> results = new HashMap<>();
        List<CacheNode> toMergeList = new ArrayList<>();
        allSlots.forEach((k, v) -> {
            SlotWindow window = v;
            Identifier id = identifier;
            VariableMeta meta = null;
            VariableGraphManager manager = null;
            Map<DimensionType, DimensionVariableGraphManager> dimensionedGraphManagers = window.getDimensionedGraphManagers();
            Map<DimensionType, IncidentVariableGraphManager> dimensionedIncidentVariableGraphManager = window.getDimensionedIncidentVariableGraphManager();
            List<VariableMeta> metas = window.getMetas();
            if (MetaUtil.getMetas(metas, id) != null) {
                meta = MetaUtil.getMetas(metas, id);
                manager = dimensionedGraphManagers.get(DimensionType.valueOf(meta.getDimension().toUpperCase()));
            } else {
                meta = IncidentVariableMetaRegister.getMeta(id);
                manager = dimensionedIncidentVariableGraphManager.get(DimensionType.valueOf(meta.getDimension().toUpperCase()));
            }
            if (meta == null) {
                logger.error("variable does not exist");
                return;
            }
            if (manager instanceof DimensionVariableGraphManager) {
                DimensionVariableGraphManager dimensionManager = (DimensionVariableGraphManager) manager;
                DimensionVariableGraphManager.VariableGraphProcessor[] shardGraphsProcessors = dimensionManager.getShardGraphsProcessors();
                Map<Integer, Collection<String>> shardKeys = manager.groupShardKeys(keys);
                shardKeys.forEach((shard, sks) -> {
                    DimensionVariableGraphManager.VariableGraphProcessor shardGraphsProcessor = shardGraphsProcessors[shard];
                    VariableGraph variableGraph = shardGraphsProcessor.getVariableGraph();
                    Map<Identifier, VariableNode> variableMap = variableGraph.getVariableMap();
                    VariableNode variableNode = variableMap.get(identifier);
                    if (variableNode instanceof CacheNode) {
                        toMergeList.add((CacheNode) variableNode);
                    }
                });
            } else {
                IncidentVariableGraphManager incidentManager = (IncidentVariableGraphManager) manager;
                IncidentComputeWorker[] workers = incidentManager.getWorkers();
                Map<Integer, Collection<String>> shardKeys = incidentManager.groupShardKeys(keys);
                shardKeys.forEach((shard, sks) -> {
                    IncidentComputeWorker shardGraphsProcessor = workers[shard];
                    Map<Identifier, IncidentNode> incidentNodeMap = shardGraphsProcessor.getIncidentNodeMap();
                    CacheNode variableNode = (CacheNode) incidentNodeMap.get(identifier);
                    toMergeList.add(variableNode);
                });
            }
        });

        NodePrimaryData data = merge(toMergeList, keys.toArray(new String[]{}));
        if (data == null) {
            return null;
        }
        logger.info("merge previous slot primary data = {}", data);
        logger.info("query merge previous slot return, result = {}", data.getResult());
        return data.getResult();
    }

    @Override
    public Object mergePrevious(Identifier identifier, String key) {
        if (key == null) {
            return null;
        }
        List<String> keys = new ArrayList<>();
        keys.add(key);
        return mergePrevious(identifier, keys);
    }

    private NodePrimaryData merge(List<CacheNode> toMergeList, String... keys) {
        NodePrimaryData pre = null;
        for (CacheNode node : toMergeList) {
            pre = node.merge(pre, keys);
        }
        if (pre != null) {
            return pre;
        }
        return null;
    }

    public Object queryCurrent(Identifier identifier, Collection<String> keys) {
        VariableMeta meta;
        VariableGraphManager manager;
        Identifier id = identifier;
        Map<DimensionType, DimensionVariableGraphManager> dimensionedGraphManagers = engine.getCurrentCommonManagers();
        Map<DimensionType, IncidentVariableGraphManager> dimensionedIncidentVariableGraphManager = engine.getCurrentIncidentManagers();
        List<VariableMeta> metas = engine.getMetas();
        if (MetaUtil.getMetas(metas, id) != null) {
            meta = MetaUtil.getMetas(metas, id);
            manager = dimensionedGraphManagers.get(DimensionType.valueOf(meta.getDimension().toUpperCase()));
        } else {
            meta = IncidentVariableMetaRegister.getMeta(id);
            manager = dimensionedIncidentVariableGraphManager.get(DimensionType.valueOf(meta.getDimension().toUpperCase()));
        }
        if (meta == null) {
            logger.error("variable does not exist");
            logger.warn(">>>>>>query current slot return, results = NULL");
            return null;
        }
        VariableQuery query;
        if (meta.getType().equals("top")) {
            if (meta.getGroupKeys() != null && meta.getGroupKeys().size() > 0) {
                if (keys == null || keys.size() <= 0) {
                    return null;
                }
                query = VariableQueryUtil.sendKeyTopQuery(manager, id, keys, 20);
            } else {
                query = VariableQueryUtil.broadcastTopQuery(manager, id, 20);
            }
        } else {
            if (meta.getGroupKeys() != null && meta.getGroupKeys().size() > 0) {
                if (keys == null || keys.size() <= 0) {
                    return null;
                }
                query = VariableQueryUtil.sendKeyQuery(manager, id, keys);
            } else {
                query = VariableQueryUtil.broadcastQuery(manager, id);
            }
        }
        Object obj = query.waitQueryResult(2, TimeUnit.SECONDS);
        if (obj != null) {
            logger.info(">>>>>>query current slot return, results = {}", obj);
        }
        return obj;
    }
}
