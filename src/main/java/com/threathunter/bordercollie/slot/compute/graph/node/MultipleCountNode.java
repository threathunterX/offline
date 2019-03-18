package com.threathunter.bordercollie.slot.compute.graph.node;

import com.threathunter.bordercollie.slot.compute.VariableDataContext;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapper;
import com.threathunter.bordercollie.slot.compute.graph.node.propertyhandler.condition.PropertyConditionHandler;
import com.threathunter.bordercollie.slot.compute.graph.node.propertyhandler.reduction.ReductionHandler;
import com.threathunter.bordercollie.slot.util.LogUtil;
import com.threathunter.common.Identifier;
import com.threathunter.model.Property;
import com.threathunter.variable.DimensionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 
 */
public class MultipleCountNode<R> extends VariableNode implements CacheNode<R> {
    private List<CacheWrapper> cacheWrappers;
    Logger logger = LoggerFactory.getLogger(MultipleCountNode.class);
    private List<Property> groupBys;

    private ReductionHandler reductionHandler;
    private PropertyConditionHandler conditionHandler;

    @Override
    public String getVariableName() {
        return getMeta().getName();
    }

    @Override
    public List<CacheWrapper> getWrappers() {
        return cacheWrappers;
    }

    @Override
    public R getData(String... keys) {
        return (R) cacheWrappers.get(0).getData(keys);
    }

    @Override
    public Object getAll(String key) {
        return this.cacheWrappers.get(0).readAll(key);
    }

    @Override
    public DimensionType getDataDimension() {
        return this.meta.getGroupKeys() != null && this.meta.getGroupKeys().size() > 0 ? DimensionType.getDimension(this.meta.getDimension()) : DimensionType.GLOBAL;
    }

    @Override
    public NodePrimaryData merge(NodePrimaryData data, String... keys) {
        return null;
    }

/*    @Override
    public Map<String, R> getTop(int topCount, String key) {
        return null;
    }

    @Override
    public Map<String, Object> getAllTop() {
        return null;
    }

    @Override
    public void clearTop() {

    }*/

    @Override
    public boolean compute(VariableDataContext context) {
        if (this.conditionHandler != null && !this.conditionHandler.match(context)) {
            return false;
        }

        Object value = this.reductionHandler.doReduction(context);
        if (value == null) {
            return false;
        }

//        Object[] values = new String[this.groupBys.size()];
        Identifier id = this.getSrcIdentifiers().get(0);
        List<Object> objs = new ArrayList<>();
        Set<Object> dctObjs = new HashSet<>();
        groupBys.forEach(p -> {
            String name = p.getName();
            Object obj = context.getFromContext(id, name);
            objs.add(context.getFromContext(id, name));
            dctObjs.add(obj);
        });

        //TODO YY HOW to put the count into wrapper
//        CacheWrapper wrapper = this.cacheWrappers.get(0);
 /*       if (value instanceof List) {
            ((List) value).forEach(v -> {
                if (v != null) {
                    wrapper.addData(v, values);
                }
            });
        } else {
            if (value != null) {
                wrapper.addData(value, values);
            }
        }
        R result = (R) wrapper.getData(values);
        if (result == null) {
            return false;
        }

        if (this.topQueue != null) {
            this.topQueue.update((String) context.getFromContext(this.getSrcIdentifiers().get(0), this.groupBys.get(0).getName()));
        }
        if (keyTopMap != null) {
            String firstKey = (String) context.getFromContext(this.getSrcIdentifiers().get(0), this.groupBys.get(0).getName());
            String secondKey = (String) context.getFromContext(this.getSrcIdentifiers().get(0), this.groupBys.get(1).getName());
            this.keyTopMap.computeIfAbsent(firstKey, k -> new LimitMaxPriorityNodeQueue(100, this, firstKey)).update(secondKey);
        }*/
//        context.addContextValue(this.getIdentifier(), "value", result);
        LogUtil.print(context, logger);
        return true;
    }

    public void setGroupBys(List<Property> groupBys) {
        this.groupBys = groupBys;
    }

    public ReductionHandler getReductionHandler() {
        return reductionHandler;
    }

    public void setReductionHandler(ReductionHandler reductionHandler) {
        this.reductionHandler = reductionHandler;
    }

    public PropertyConditionHandler getConditionHandler() {
        return conditionHandler;
    }

    public void setConditionHandler(PropertyConditionHandler conditionHandler) {
        this.conditionHandler = conditionHandler;
    }

    public void setCacheWrappers(List<CacheWrapper> cacheWrappers) {
        this.cacheWrappers = cacheWrappers;
    }

    public List<CacheWrapper> getCacheWrappers() {
        return cacheWrappers;
    }

    public List<Property> getGroupBys() {
        return groupBys;
    }
}
