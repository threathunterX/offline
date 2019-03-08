package com.threathunter.bordercollie.slot.compute.graph;

import com.threathunter.bordercollie.slot.compute.VariableDataContext;
import com.threathunter.bordercollie.slot.compute.cache.StorageType;
import com.threathunter.bordercollie.slot.compute.cache.storage.CacheStore;
import com.threathunter.bordercollie.slot.compute.cache.storage.CacheStoreFactory;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapper;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.array.CacheConstants;
import com.threathunter.bordercollie.slot.compute.graph.node.*;
import com.threathunter.bordercollie.slot.compute.graph.nodegenerator.VariableNodeGenerator;
import com.threathunter.bordercollie.slot.compute.graph.query.VariableQuery;
import com.threathunter.bordercollie.slot.util.SlotMetricsHelper;
import com.threathunter.common.Identifier;
import com.threathunter.model.Event;
import com.threathunter.model.VariableMeta;
import com.threathunter.variable.DimensionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by daisy on 17/3/6.
 */
public class VariableGraph {
    private static final Logger LOGGER = LoggerFactory.getLogger(VariableGraph.class);
    private static final Logger logger = LoggerFactory.getLogger("bordercollie");
    private final Map<Identifier, VariableNode> variableMap;
    private final List<CacheNode> cacheNodes;
    private final List<CacheWrapper> cacheWrappers;
    private final CacheStore cacheStore;

    public VariableGraph(final List<VariableMeta> metaList, final StorageType storageType) {
        List<VariableNode> variableNodes = new ArrayList<>();
        this.variableMap = new HashMap<>();
        LOGGER.trace("before node generator, metaList size:" + metaList.size());
        metaList.forEach(meta -> variableNodes.addAll(VariableNodeGenerator.generateNode(meta, storageType)));
        LOGGER.trace("after node generator, variable node size:" + variableNodes.size());

        Collections.sort(variableNodes, Comparator.comparingInt(VariableNode::getPriority));

        this.buildGraph(variableNodes);

        this.cacheNodes = new ArrayList<>();
        this.cacheWrappers = new ArrayList<>();
        variableNodes.stream().filter(node -> node instanceof CacheNode).forEach(node -> {
            if (node instanceof TopVariableNode) {
                ((TopVariableNode) node).setParentNode((CacheNode) this.variableMap.get(node.getSrcIdentifiers().get(0)));
            }
            CacheNode cacheNode = (CacheNode) node;
            cacheNodes.add(cacheNode);
            cacheWrappers.addAll(cacheNode.getWrappers());
        });
        this.cacheStore = CacheStoreFactory.newCacheStore(storageType, cacheWrappers);
        LOGGER.trace("variableGraph:{} cacheStore:{}", hashCode(), cacheStore.hashCode());
    }

    public void clearCache() {
        this.cacheStore.clearAll();
    }

    public boolean containsKey(final String key) {
        return this.cacheStore.getCache(key) != null;
    }

    public int getNodesCount() {
        return this.getVariableMap().size();
    }

    public int getCacheNodeCount() {
        return this.cacheNodes.size();
    }

    public List<VariableCacheIterator> getCacheIterators() {
        Map<DimensionType, List<CacheNode>> dimensionedNodes = new HashMap<>();
        this.cacheNodes.stream().forEach(n -> {
            dimensionedNodes.computeIfAbsent(n.getDataDimension(), k -> new ArrayList<>()).add(n);
        });
        List<VariableCacheIterator> iterators = new ArrayList<>(6);
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

    private void buildGraph(final List<VariableNode> sortedNodes) {
        sortedNodes.forEach(node -> {
            List<Identifier> srcIdentifiers = node.getSrcIdentifiers();
            if (srcIdentifiers == null || srcIdentifiers.size() <= 0) {
                if (node instanceof EventVariableNode) {
                    getVariableMap().put(node.getIdentifier(), node);
                } else {
                    throw new RuntimeException("graph update: root node must be event variable node");
                }
            } else {
                getVariableMap().put(node.getIdentifier(), node);
                addVariableNodeToEdge(srcIdentifiers, node);
            }
        });
    }

    public void process(final Event event) {
        if (event.getName().equals("__query__")) {
            try {
                processQuery(event);
            } catch (Exception e) {
                LOGGER.error("query error", e);
            }
            return;
        }
        Identifier identifier = Identifier.fromKeys(event.getApp(), event.getName());
        VariableNode node = this.getVariableMap().get(identifier);
        if (node != null) {
            if (!(node instanceof EventVariableNode)) {
                //LOGGER.error("wangbo 错误太多先屏蔽掉 event variable is not exist: " + node.getMeta().getName());
            }else {
                logger.warn("DimensionVariable compute step2, id:{} ,  identifier: {} , children:{} ",event.getId(),identifier.toString(),node.getToNodes());
                VariableDataContext context = ((EventVariableNode) node).computeEvent(event);

                computeChildren(context, node.getToNodes());
            }

        } else {
//            LOGGER.error("node is not exist: " + node.getMeta().getName());
            LOGGER.error("node is not exist！");
        }
        SlotMetricsHelper.getInstance().addMetrics("slot.events.graph.compute.count", 1.0,
                "name", event.getName());

    }

    private void processQuery(final Event event) {
        LOGGER.info(">>>>>>graph query event {}>>>>>>>", event);
        Identifier variableId = (Identifier) event.getPropertyValues().get("id");
        if (!this.getVariableMap().containsKey(variableId)) {
            LOGGER.error("variable not exist");
            return;
        }
        VariableNode node = this.getVariableMap().get(variableId);
        if (!(node instanceof CacheNode)) {
            LOGGER.error("variable is not cache type");
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
                LOGGER.info("Common __batch__ query result:{}", result);
            } else {
                if (event.getPropertyValues().containsKey("sub_keys")) {
                    Map<String, Object> subMap = new HashMap<>();
                    ((Collection<String>) event.getPropertyValues().get("sub_keys")).forEach(secondKey ->
                            subMap.put(secondKey, cacheNode.getData(event.getKey(), secondKey)));
                    query.addResult(subMap);
                    LOGGER.info("Common sub_keys query result:{}", event.getPropertyValues().get("sub_keys"));
                } else {
                    query.addResult(cacheNode.getData(event.getKey()));
                    LOGGER.info("Common query result:{}", cacheNode.getData(event.getKey()));
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("query failed, input event = " + event, e);
        }

    }

    private void computeChildren(final VariableDataContext context, final List<VariableNode> children) {
        List<VariableNode> continueProcessNodes = new ArrayList<>();

        if (children == null) {
            return;
        }
        children.forEach(node -> {
            try {
                LOGGER.trace("meta name {}", node.getMeta().getName());
                boolean result = node.compute(context);
                if (result && node.getToNodes() != null && node.getToNodes().size() > 0) {
                    continueProcessNodes.add(node);
                }
            } catch (Exception e) {
                LOGGER.error("graph compute error", e);
                SlotMetricsHelper.getInstance().addMetrics("slot.node.graph.compute.error.count", 1.0, "node", node.getMeta().getName());
            }
        });
        continueProcessNodes.forEach(node -> computeChildren(context, node.getToNodes()));

    }

    private void addVariableNodeToEdge(final List<Identifier> parentIdentifiers, final VariableNode node) {
        parentIdentifiers.forEach(parentId -> {
            VariableNode parentNode = getVariableMap().get(parentId);
            if (parentNode == null) {
                throw new RuntimeException(String.format("priority error, node %s should not before node %s",
                        node.getIdentifier().toString(), parentId.toString()));
            }
            if (parentNode.getPriority() >= node.getPriority()) {
                throw new RuntimeException(String.format("priority error: nodes circle warning: child node %s priority is %d," +
                                "while parent node %s priority is %d", node.getIdentifier().toString(), node.getPriority(),
                        parentNode.getIdentifier().toString(), parentNode.getPriority()));
            }
            if (parentNode.getToNodes() == null) {
                parentNode.setToNodes(new ArrayList<>());
            }

            parentNode.getToNodes().add(node);
        });
        if (node instanceof DualvarVariableNode) {
            ((DualvarVariableNode) node).setFirstParentNode((CacheNode) getVariableMap().get(parentIdentifiers.get(0)));
            ((DualvarVariableNode) node).setSecondParentNode((CacheNode) getVariableMap().get(parentIdentifiers.get(1)));
        }
    }

    public Map<Identifier, VariableNode> getVariableMap() {
        return variableMap;
    }

}
