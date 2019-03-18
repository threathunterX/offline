package com.threathunter.bordercollie.slot.compute.graph.node;

import com.threathunter.bordercollie.slot.compute.VariableDataContext;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapper;
import com.threathunter.bordercollie.slot.compute.graph.node.operator.ValueOperator;
import com.threathunter.bordercollie.slot.util.LogUtil;
import com.threathunter.model.Property;
import com.threathunter.variable.DimensionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 
 */
public class SequenceVariableNode extends VariableNode implements CacheNode<Number> {

    Logger logger = LoggerFactory.getLogger(SequenceVariableNode.class);
    // first wrapper is to store the real result, second wrapper is to store the previous value (a temp value)
    private List<CacheWrapper> cacheWrappers;

    private List<String> groupBys;
    private Property valueProperty;
    private ValueOperator operator;

    public void setCacheWrappers(final List<CacheWrapper> cacheWrappers) {
        this.cacheWrappers = cacheWrappers;
    }

    public void setGroupBys(final List<Property> groupProperty) {
        this.groupBys = new ArrayList<>();
        groupProperty.forEach(property -> this.groupBys.add(property.getName()));
    }

    public void setValueProperty(final Property valueProperty) {
        this.valueProperty = valueProperty;
    }

    public void setOperator(final ValueOperator operator) {
        this.operator = operator;
    }

    @Override
    public String getVariableName() {
        return this.getMeta().getName();
    }

    @Override
    public List<CacheWrapper> getWrappers() {
        return this.cacheWrappers;
    }

    @Override
    public Number getData(final String... keys) {
        return (Number) this.cacheWrappers.get(0).getData(keys);
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

/*
    @Override
    public Map<String, Number> getTop(int topCount, String key) {
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
    public boolean compute(final VariableDataContext context) {
        Number currentValue = (Number) context.getFromContext(this.valueProperty.getIdentifier(), valueProperty.getName());
        if (currentValue == null) {
            return false;
        }
        String[] keys = new String[this.groupBys.size()];
        for (int i = 0; i < keys.length; i++) {
            String keyName = this.groupBys.get(0);
            Object keyValue = context.getFromContext(this.getSrcIdentifiers().get(0), keyName);
            if (keyValue == null) {
                return false;
            }
            context.addContextValue(this.getIdentifier(), keyName, keyValue);
        }
        Number previousValue = (Number) this.cacheWrappers.get(0).getData(keys);
        if (previousValue == null) {
            this.cacheWrappers.get(0).addData(currentValue, keys);
            return false;
        }
        this.cacheWrappers.get(0).addData(currentValue, keys);
        Number result = (Number) this.cacheWrappers.get(0).addData(operator.doOperate(previousValue, currentValue), keys);
        context.addContextValue(this.getIdentifier(), "value", result);
        LogUtil.print(context, logger);
        return true;
    }
}
