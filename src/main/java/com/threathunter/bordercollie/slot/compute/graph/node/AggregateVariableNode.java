package com.threathunter.bordercollie.slot.compute.graph.node;

import com.threathunter.bordercollie.slot.compute.VariableDataContext;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapper;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.PrimaryData;
import com.threathunter.bordercollie.slot.compute.graph.node.propertyhandler.condition.PropertyConditionHandler;
import com.threathunter.bordercollie.slot.compute.graph.node.propertyhandler.reduction.ReductionHandler;
import com.threathunter.bordercollie.slot.util.LimitMaxPriorityNodeQueue;
import com.threathunter.model.Property;
import com.threathunter.variable.DimensionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by daisy on 17/3/25.
 */
public class AggregateVariableNode<R> extends VariableNode implements CacheNode<R> {
    private List<CacheWrapper> cacheWrappers;

    private List<Property> groupBys;
    private ReductionHandler reductionHandler;
    private PropertyConditionHandler conditionHandler;

    private LimitMaxPriorityNodeQueue topQueue;
    private Map<String, LimitMaxPriorityNodeQueue> keyTopMap;

    Logger logger = LoggerFactory.getLogger(AggregateVariableNode.class);

    public Map<String, LimitMaxPriorityNodeQueue> getKeyTopMap() {
        return keyTopMap;
    }

    public void setKeyTopMap(final Map<String, LimitMaxPriorityNodeQueue> keyTopMap) {
        this.keyTopMap = keyTopMap;
    }

    public LimitMaxPriorityNodeQueue getTopQueue() {
        return topQueue;
    }

    public void setTopQueue(final LimitMaxPriorityNodeQueue topQueue) {
        this.topQueue = topQueue;
    }

    public void setCacheWrappers(final List<CacheWrapper> cacheWrappers) {
        this.cacheWrappers = cacheWrappers;
    }

    public void setGroupBys(final List<Property> groupBys) {
        this.groupBys = groupBys;
    }

    public void setReductionHandler(final ReductionHandler reductionHandler) {
        this.reductionHandler = reductionHandler;
    }

    public ReductionHandler getReductionHandler() {
        return reductionHandler;
    }

    public void setConditionHandler(final PropertyConditionHandler conditionHandler) {
        this.conditionHandler = conditionHandler;
    }

    @Override
    public String getVariableName() {
        return this.getMeta().getName();
    }

    @Override
    public List<CacheWrapper> getWrappers() {
        return cacheWrappers;
    }

    @Override
    public R getData(final String... keys) {
        return (R) this.cacheWrappers.get(0).getData(keys);
    }

    @Override
    public Object getAll(final String key) {
        return this.cacheWrappers.get(0).readAll(key);
    }

    @Override
    public DimensionType getDataDimension() {
        return this.groupBys != null && this.groupBys.size() > 0 ? DimensionType.getDimension(this.meta.getDimension()) : DimensionType.GLOBAL;
    }

    @Override
    public NodePrimaryData merge(NodePrimaryData data, String... keys) {
        if (data == null) {
            data = new NodePrimaryData() {
                @Override
                public Object getResult() {
                    if (this.getWrapperPrimaryData()[0] == null) {
                        return null;
                    }
                    return this.getWrapperPrimaryData()[0].getResult();
                }
            };
            data.setWrapperPrimaryData(new PrimaryData[1]);
        }
        PrimaryData[] array = data.getWrapperPrimaryData();
        PrimaryData merged = this.cacheWrappers.get(0).merge(array[0], keys);
        array[0] = merged;

        return data;
    }

    @Override
    public boolean compute(final VariableDataContext context) {

        if (this.conditionHandler != null && !this.conditionHandler.match(context)) {
            return false;
        }

        Object value = this.reductionHandler.doReduction(context);
        if (value != null) {
            context.addContextValue(this.getIdentifier(), "value", value);
            return true;
        }
        return false;
    }
}
