package com.threathunter.bordercollie.slot.compute.graph.node;

import com.threathunter.bordercollie.slot.compute.VariableDataContext;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapper;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.PrimaryData;
import com.threathunter.bordercollie.slot.util.LogUtil;
import com.threathunter.common.Identifier;
import com.threathunter.model.Property;
import com.threathunter.variable.DimensionType;
import com.threathunter.variable.meta.DualVariableMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by daisy on 17/3/25.
 */
public class DualvarVariableNode extends VariableNode implements CacheNode<Double> {

    // also just have only one wrapper that store the result
    private List<CacheWrapper> cacheWrappers;

    private CacheNode firstParentNode;
    private CacheNode secondParentNode;

    private List<String> groupBys;
    private Identifier firstVariableId;
    private Identifier secondVariableId;

    private String valuePropertyName;
    private Logger logger = LoggerFactory.getLogger(DualvarVariableNode.class);

    public void setGroupBys(List<Property> groupProperty) {
        this.groupBys = new ArrayList<>();
        groupProperty.forEach(property -> this.groupBys.add(property.getName()));
    }

    public CacheNode getFirstParentNode() {
        return firstParentNode;
    }

    public void setFirstParentNode(CacheNode firstParentNode) {
        this.firstParentNode = firstParentNode;
    }

    public CacheNode getSecondParentNode() {
        return secondParentNode;
    }

    public void setSecondParentNode(CacheNode secondParentNode) {
        this.secondParentNode = secondParentNode;
    }

    public void setCacheWrappers(final List<CacheWrapper> cacheWrappers) {
        this.cacheWrappers = cacheWrappers;
    }

    public void setFirstVariableId(Identifier firstVariableId) {
        this.firstVariableId = firstVariableId;
    }

    public void setSecondVariableId(Identifier secondVariableId) {
        this.secondVariableId = secondVariableId;
    }

    public void setValuePropertyName(final String valuePropertyName) {
        this.valuePropertyName = valuePropertyName;
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
    public Double getData(final String... keys) {
        return (Double) this.cacheWrappers.get(0).getData(keys);
    }

    @Override
    public Object getAll(String key) {
        return this.cacheWrappers.get(0).readAll(key);
    }

    @Override
    public DimensionType getDataDimension() {
        return this.groupBys != null && this.groupBys.size() > 0 ? DimensionType.getDimension(meta.getDimension()) : DimensionType.GLOBAL;
    }

    @Override
    public NodePrimaryData merge(NodePrimaryData data, String... keys) {
        if (data == null) {
            data = new NodePrimaryData() {
                @Override
                public Object getResult() {
                    if (this.getWrapperPrimaryData() == null) {
                        return null;
                    }
                    double first = this.getWrapperPrimaryData()[0] == null ? 0.0 : ((Number) this.getWrapperPrimaryData()[0].getResult()).doubleValue();
                    double second = this.getWrapperPrimaryData()[1] == null ? 0.0 : ((Number) this.getWrapperPrimaryData()[1].getResult()).doubleValue();
                    if (((DualVariableMeta) meta).getOperation().equals("/")) {
                        if (this.getWrapperPrimaryData()[1] == null) {
                            return 0.0;
                        }
                        return first / second;
                    }
                    return first + second;
                }
            };
            data.setWrapperPrimaryData(new PrimaryData[2]);
        }
        CacheWrapper firstCacheWrapper = (CacheWrapper) this.firstParentNode.getWrappers().get(0);
        CacheWrapper secondCacheWrapper = (CacheWrapper) this.secondParentNode.getWrappers().get(0);
        data.getWrapperPrimaryData()[0] = firstCacheWrapper.merge(data.getWrapperPrimaryData()[0], keys);
        data.getWrapperPrimaryData()[1] = secondCacheWrapper.merge(data.getWrapperPrimaryData()[1], keys);

        return data;
    }

    @Override
    public boolean compute(final VariableDataContext context) {
        String[] keys = new String[this.groupBys.size()];
        for (int i = 0; i < keys.length; i++) {
            String keyName = this.groupBys.get(i);
            Object obj = context.getFromContext(secondVariableId, keyName);
            if (obj == null) {
                return false;
            }
            keys[i] = obj.toString();
        }
        Number firstValue = (Number) context.getFromContext(firstVariableId, "value");
        Number secondValue = (Number) context.getFromContext(secondVariableId, "value");
        if (firstValue == null || secondValue == null) {
            return false;
        }
        String operation = ((DualVariableMeta) this.meta).getOperation();
        if (operation.equals("/")) {
            this.cacheWrappers.get(0).addData(firstValue.doubleValue() / secondValue.doubleValue(), keys);
        } else if (operation.equals("+")) {
            this.cacheWrappers.get(0).addData(firstValue.doubleValue() + secondValue.doubleValue(), keys);
        } else {
            throw new RuntimeException("operation is not support in dual: " + operation);
        }
        LogUtil.print(context, logger);
        return true;
    }
}
