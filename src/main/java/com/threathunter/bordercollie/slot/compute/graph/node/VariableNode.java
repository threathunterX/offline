package com.threathunter.bordercollie.slot.compute.graph.node;

import com.threathunter.bordercollie.slot.compute.VariableDataContext;
import com.threathunter.bordercollie.slot.compute.cache.CacheType;
import com.threathunter.common.Identifier;
import com.threathunter.model.VariableMeta;
import com.threathunter.variable.exception.NotSupportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * {@code VariableNode} is the basic of offline variable computation.
 * A variable node is generated according to a variable meta.
 * <p>
 * {@code VariableNode} will compute variables, some will just do filter,
 * some will do aggregation.
 * A variable will be transferred between {@code VariableNode}, every variable
 * has it's value store in {@code value}, with other properties.
 * <p/>
 * {@code VariableNode} has several types:
 * {@link EventVariableNode}:
 * represent a base event variable, the variable is directly from a event.
 * {@link FilterVariableNode}:
 * filter a variable, check if it need to be transferred to other nodes.
 * {@link AggregateVariableNode}:
 * aggregate value of a variable node.
 * {@link SequenceVariableNode}:
 * will compute two continuous variable, get a value, and store the value.
 * {@link DualvarVariableNode}:
 * will compute the ratio value from two variable, and store the value.
 *
 * @author daisy
 * @since 1.4
 */
public abstract class VariableNode {
    private static final Logger LOGGER = LoggerFactory.getLogger("bordercollie");

    private Identifier identifier;
    private List<Identifier> srcIdentifiers;
    private int priority;
    protected VariableMeta meta;

    private List<VariableNode> toNodes;

    private static Map<String, String> reductionCacheTypeMap;
    private static Map<String, String> valuePropertyCacheTypeMap;

    static {
        reductionCacheTypeMap = new HashMap<>();
        reductionCacheTypeMap.put("stringcount", "COUNT");
        reductionCacheTypeMap.put("longcount", "COUNT");
        reductionCacheTypeMap.put("doublecount", "COUNT");
        reductionCacheTypeMap.put("longgroup_count", "GROUP_COUNT");
        reductionCacheTypeMap.put("longgroup_sum", "GROUP_SUM_LONG");
        reductionCacheTypeMap.put("doublegroup_count", "GROUP_COUNT");

        reductionCacheTypeMap.put("count", "COUNT");
        reductionCacheTypeMap.put("stringdistinct_count", "DISTINCT_COUNT");
        reductionCacheTypeMap.put("distinctcount", "DISTINCT_COUNT");
        reductionCacheTypeMap.put("stringlistdistinct_count", "DISTINCT_COUNT");
        reductionCacheTypeMap.put("stringlistcount", "COUNT");
        reductionCacheTypeMap.put("longmin", "MIN_LONG");
        reductionCacheTypeMap.put("longavg", "AVG_LONG");
        // TODO check
        reductionCacheTypeMap.put("longsum", "SUM_LONG");
        reductionCacheTypeMap.put("doublesum", "SUM_DOUBLE");
        reductionCacheTypeMap.put("doublestddev", "STDDEV_DOUBLE");
        reductionCacheTypeMap.put("doublecv", "CV_DOUBLE");
        reductionCacheTypeMap.put("longlast", "LAST_LONG");
        reductionCacheTypeMap.put("longfirst", "FIRST_LONG");
        reductionCacheTypeMap.put("longstddev", "STDDEV_LONG");
        reductionCacheTypeMap.put("longcv", "CV_LONG");
        reductionCacheTypeMap.put("stringlast", "LAST_STRING");
        valuePropertyCacheTypeMap = new HashMap<>();
        valuePropertyCacheTypeMap.put("long", "LAST_LONG");
        valuePropertyCacheTypeMap.put("double", "LAST_DOUBLE");
    }

    public CacheType getCacheType(String reductionType, String valuePropertyType, int keysCount) {
        String raw = null;
        if (reductionType != null) {
            raw = reductionCacheTypeMap.get(reductionType);
        }
        if (valuePropertyType != null) {
            raw = valuePropertyCacheTypeMap.get(valuePropertyType);
        }
        if (raw == null) {
            throw new NotSupportException("reduction type is not supported: " + raw);
        }
        if (keysCount == 0) {
            return CacheType.valueOf("GLOBAL_" + raw);
        } else if (keysCount == 1) {
            return CacheType.valueOf(raw);
        } else if (keysCount == 2) {
            return CacheType.valueOf("SECONDARY_" + raw);
        }

        throw new NotSupportException("reduction type is not supported: " + raw);
    }

    public Identifier getIdentifier() {
        return identifier;
    }

    public void setIdentifier(Identifier identifier) {
        this.identifier = identifier;
    }

    public List<VariableNode> getToNodes() {
        return toNodes;
    }

    public void setToNodes(List<VariableNode> toNodes) {
        this.toNodes = toNodes;
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public VariableMeta getMeta() {
        return meta;
    }

    public void setMeta(VariableMeta meta) {
        this.meta = meta;
    }

    public List<Identifier> getSrcIdentifiers() {
        return srcIdentifiers;
    }

    public void setSrcIdentifiers(List<Identifier> srcIdentifiers) {
        this.srcIdentifiers = srcIdentifiers;
    }

    public abstract boolean compute(VariableDataContext context);
}
