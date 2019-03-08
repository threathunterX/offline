package com.threathunter.bordercollie.slot.compute.graph.nodegenerator;

import com.threathunter.bordercollie.slot.compute.cache.StorageType;
import com.threathunter.bordercollie.slot.compute.graph.VariableGraph;
import com.threathunter.bordercollie.slot.compute.graph.node.VariableNode;
import com.threathunter.model.VariableMeta;
import com.threathunter.variable.exception.NotSupportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author daisy
 * @since 1.4
 */
public abstract class VariableNodeGenerator<V extends VariableMeta> {
    private V meta;
    private StorageType storageType;
    private static final Logger LOGGER = LoggerFactory.getLogger("bordercollie");

    public static List<VariableNode> generateNode(final VariableMeta meta, final StorageType type) {

        Class<? extends VariableNodeGenerator> g = VariableNodeGeneratorRegistry.getGenerator(meta.getClass());
        if (g == null) {
            return null;
        }

        try {
            VariableNodeGenerator gen = g.newInstance();
            gen.setVariable(meta);
            gen.setStorageType(type);
            List<VariableNode> nodes = new ArrayList<>();
            nodes.addAll(gen.genVariableNodes());
            nodes.forEach(node -> {
                LOGGER.warn("generateNode , VariableMeta: {} , node: {} ",meta.getVisibleName(),node);
            });

            return nodes;
        } catch (Exception e) {
            throw new NotSupportException("fail to generate for " + meta.getName(), e);
        }
    }

    protected StorageType getStorageType() {
        return storageType;
    }

    protected void setStorageType(StorageType storageType) {
        this.storageType = storageType;
    }

    protected void setVariable(V meta) {
        this.meta = meta;
    }

    protected V getVariableMeta() {
        return meta;
    }

    public abstract List<VariableNode> genVariableNodes();

}
