package com.threathunter.bordercollie.slot.compute.graph.node;

import com.threathunter.bordercollie.slot.compute.VariableDataContext;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapper;
import com.threathunter.bordercollie.slot.compute.graph.node.propertyhandler.condition.PropertyConditionHandler;
import com.threathunter.bordercollie.slot.compute.graph.node.propertyhandler.reduction.ReductionHandler;
import com.threathunter.bordercollie.slot.compute.graph.node.propertyhandler.reduction.TopReductionHandler;
import com.threathunter.bordercollie.slot.util.LogUtil;
import com.threathunter.bordercollie.slot.util.TopType;
import com.threathunter.model.Property;
import com.threathunter.variable.DimensionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 
 */
public class TopVariableNode extends VariableNode implements CacheNode<List> {
    Logger logger = LoggerFactory.getLogger(TopVariableNode.class);
    private List<Property> groupBys;
    private TopReductionHandler reductionHandler;
    private PropertyConditionHandler conditionHandler;
    private TopType toptype;

    // TODO for different level computation
    private CacheNode parentNode;

    public CacheNode getParentNode() {
        return parentNode;
    }

    public void setParentNode(CacheNode parentNode) {
        this.parentNode = parentNode;
    }

    @Override
    public String getVariableName() {
        return getMeta().getName();
    }

    @Override
    public List<CacheWrapper> getWrappers() {
        return getCacheWrappers();
    }

    @Override
    public List<Map<String, Object>> getData(String... keys) {
        if (this.getToptype() == TopType.SINGLE) {
            return this.reductionHandler.getData();
        } else {
            // get data from parent.
            if (this.parentNode == null) {
                return null;
            }
            Map<String, Number> data = (Map<String, Number>) this.parentNode.getData(keys);
            if (data == null || data.isEmpty()) {
                return null;
            }

            List<Map<String, Object>> result = new ArrayList<>(data.size());
            data.forEach((tag, count) -> {
                HashMap<String, Object> map = new HashMap<>(4);
                map.put("key", tag);
                map.put("value", count);
                result.add(map);
            });

            result.sort((d1, d2) -> ((Number) d2.get("value")).intValue() - ((Number) d1.get("value")).intValue());

            return result;
        }

    }

    @Override
    public Object getAll(String key) {
        return getCacheWrappers().get(0).readAll(key);
    }

    @Override
    public DimensionType getDataDimension() {
        return this.getToptype() == TopType.DOUBLE ? DimensionType.getDimension(meta.getDimension()) : DimensionType.GLOBAL;
    }

    @Override
    public NodePrimaryData merge(NodePrimaryData data, String... keys) {
        return null;
    }

    @Override
    public boolean compute(VariableDataContext context) {
        if (this.conditionHandler != null && !this.conditionHandler.match(context)) {
            return false;
        }

        Object value = this.reductionHandler.doReduction(context);
        if (value == null) {
            return false;
        }

        if (groupBys == null)
            return false;
        String[] gkeys = new String[this.groupBys.size()];
        Object[] gvalues = new String[this.groupBys.size()];
        for (int i = 0; i < gkeys.length; i++) {
            String keyName = this.groupBys.get(i).getName();
            Object keyValue = context.getFromContext(this.getSrcIdentifiers().get(0), keyName);
            context.addContextValue(this.getIdentifier(), keyName, keyValue);
            gkeys[i] = keyName;
            gvalues[i] = keyValue;
        }

        if (this.toptype == TopType.SINGLE) {
        }

        LogUtil.print(context, logger);
        return true;
    }

    public List<CacheWrapper> getCacheWrappers() {
        return new ArrayList<>();
    }

    public void setCacheWrappers(List<CacheWrapper> cacheWrappers) {
        throw new RuntimeException("not cache wrapper in top variable node");
    }

    public List<Property> getGroupBys() {
        return groupBys;
    }

    public void setGroupBys(List<Property> groupBys) {
        this.groupBys = groupBys;
    }

    public ReductionHandler getReductionHandler() {
        return reductionHandler;
    }

    public void setReductionHandler(TopReductionHandler reductionHandler) {
        this.reductionHandler = reductionHandler;
    }

    public PropertyConditionHandler getConditionHandler() {
        return conditionHandler;
    }

    public void setConditionHandler(PropertyConditionHandler conditionHandler) {
        this.conditionHandler = conditionHandler;
    }


    public TopType getToptype() {
        return toptype;
    }

    public void setToptype(TopType toptype) {
        this.toptype = toptype;
    }

}
