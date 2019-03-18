package com.threathunter.bordercollie.slot.compute.graph.node.propertyhandler.reduction;

import com.threathunter.bordercollie.slot.compute.VariableDataContext;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapper;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapperMeta;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.array.CountsArrayCacheWrapper;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.array.DoubleArrayCacheWrapper;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.array.GlobalCountsArrayCacheWrapper;
import com.threathunter.common.Identifier;
import com.threathunter.model.Property;
import com.threathunter.variable.reduction.DoublePropertyReduction;

import java.util.Arrays;
import java.util.List;

/**
 * 
 */
public abstract class DoubleReductionHandler implements ReductionHandler {
    protected List<Property> srcProperties;
    protected Property destProperty;
    protected CacheWrapper wrapper;

    public DoubleReductionHandler(final DoublePropertyReduction r, final CacheWrapperMeta meta, final List<Property> groupKeys) {
        this.srcProperties = r.getSrcProperties();
        this.destProperty = r.getDestProperty();
    }

    public List<CacheWrapper> getCacheWrappers() {
        return Arrays.asList(wrapper);
    }

    public static class DoubleSumReductionHandler extends DoubleReductionHandler {
        private String propertyName;
        private Identifier srcId;
        private String indexKey;

        public DoubleSumReductionHandler(final DoublePropertyReduction r, final CacheWrapperMeta meta, final List<Property> groupKeys) {
            super(r, meta, groupKeys);
            propertyName = srcProperties.get(0).getName();
            srcId = srcProperties.get(0).getIdentifier();
            this.wrapper = new DoubleArrayCacheWrapper.SumDoubleArrayCacheWrapper(meta);
            this.indexKey = groupKeys.get(0).getName();
        }

        @Override
        public String getType() {
            return "doublesum";
        }

        @Override
        public Double doReduction(final VariableDataContext context) {
            Double value = (Double) context.getFromContext(srcId, propertyName);
            String indexKeyValue = (String) context.getFromContext(srcId, indexKey);
            context.addContextValue(destProperty.getIdentifier(), indexKey, indexKeyValue);
            return (Double) this.wrapper.addData(value, indexKeyValue);
        }
    }

    public static class GlobalDoubleCountReductionHandler extends DoubleReductionHandler {
        private Identifier srcId;

        public GlobalDoubleCountReductionHandler(final DoublePropertyReduction r, final CacheWrapperMeta meta, final List<Property> groupKeys) {
            super(r, meta, groupKeys);
            srcId = srcProperties.get(0).getIdentifier();
            this.wrapper = new GlobalCountsArrayCacheWrapper.GlobalCountArrayCacheWrapper(meta);
        }

        @Override
        public String getType() {
            return "globaldoublecount";
        }

        @Override
        public Integer doReduction(final VariableDataContext context) {
            return (Integer) this.wrapper.addData(1);
        }
    }

    public static class DoubleCountReductionHandler extends DoubleReductionHandler {
        private Identifier srcId;

        private String indexKey;

        public DoubleCountReductionHandler(final DoublePropertyReduction r, final CacheWrapperMeta meta, final List<Property> groupKeys) {
            super(r, meta, groupKeys);
            srcId = srcProperties.get(0).getIdentifier();
            this.wrapper = new CountsArrayCacheWrapper.CountArrayCacheWrapper(meta);
            this.indexKey = groupKeys.get(0).getName();
        }

        @Override
        public String getType() {
            return "doublecount";
        }

        @Override
        public Integer doReduction(final VariableDataContext context) {
            String indexKeyValue = (String) context.getFromContext(srcId, indexKey);
            context.addContextValue(destProperty.getIdentifier(), indexKey, indexKeyValue);
            return (Integer) this.wrapper.addData("", indexKeyValue);
        }
    }

    public static class GlobalDoubleGroupCountReductionHandler extends DoubleReductionHandler {
        private Identifier srcId;

        private String groupPropertyName;

        public GlobalDoubleGroupCountReductionHandler(final DoublePropertyReduction r, final CacheWrapperMeta meta, final List<Property> groupKeys) {
            super(r, meta, groupKeys);
            srcId = srcProperties.get(0).getIdentifier();
            this.wrapper = new GlobalCountsArrayCacheWrapper.GlobalGroupCountArrayCacheWrapper(meta);
            this.groupPropertyName = ((DoublePropertyReduction.DoubleGroupCountPropertyReduction) r).getGroupProperty().getName();
        }

        @Override
        public String getType() {
            return "globaldoublegroupcount";
        }

        @Override
        public Integer doReduction(final VariableDataContext context) {
            return (Integer) this.wrapper.addData("", (String) context.getFromContext(srcId, groupPropertyName));
        }
    }

    public static class DoubleGroupCountReductionHandler extends DoubleReductionHandler {
        private Identifier srcId;
        private String groupPropertyName;
        private String indexKey;

        public DoubleGroupCountReductionHandler(DoublePropertyReduction r, CacheWrapperMeta meta, List<Property> groupKeys) {
            super(r, meta, groupKeys);
            this.indexKey = groupKeys.get(0).getName();
            srcId = srcProperties.get(0).getIdentifier();
            this.wrapper = new CountsArrayCacheWrapper.GroupCountArrayCacheWrapper(meta);
            this.groupPropertyName = ((DoublePropertyReduction.DoubleGroupCountPropertyReduction) r).getGroupProperty().getName();
        }

        @Override
        public String getType() {
            return "doublegroupcount";
        }

        @Override
        public Object doReduction(VariableDataContext context) {
            String groupKey = (String) context.getFromContext(srcId, groupPropertyName);
            String indexKey = (String) context.getFromContext(srcId, this.indexKey);
            if (groupKey == null || indexKey == null) {
                return null;
            }
            return this.wrapper.addData("", indexKey, groupKey);
        }
    }

    public static class DoubleStddevReductionHandler extends DoubleReductionHandler {
        private String propertyName;
        private Identifier srcId;
        private String indexKey;

        public DoubleStddevReductionHandler(final DoublePropertyReduction r, final CacheWrapperMeta meta, final List<Property> groupKeys) {
            super(r, meta, groupKeys);
            propertyName = srcProperties.get(0).getName();
            srcId = srcProperties.get(0).getIdentifier();
            this.wrapper = new DoubleArrayCacheWrapper.StddevDoubleArrayCacheWrapper(meta);
            this.indexKey = groupKeys.get(0).getName();
        }

        @Override
        public String getType() {
            return "doublestddev";
        }

        @Override
        public Double doReduction(final VariableDataContext context) {
            Double value = (Double) context.getFromContext(srcId, propertyName);
            String indexKeyValue = (String) context.getFromContext(srcId, indexKey);
            context.addContextValue(destProperty.getIdentifier(), indexKey, indexKeyValue);
            return (Double) this.wrapper.addData(value, indexKeyValue);
        }
    }

    public static class DoubleCVReductionHandler extends DoubleReductionHandler {
        private String propertyName;
        private Identifier srcId;
        private String indexKey;

        public DoubleCVReductionHandler(final DoublePropertyReduction r, final CacheWrapperMeta meta, final List<Property> groupKeys) {
            super(r, meta, groupKeys);
            propertyName = srcProperties.get(0).getName();
            srcId = srcProperties.get(0).getIdentifier();
            this.wrapper = new DoubleArrayCacheWrapper.SumDoubleArrayCacheWrapper(meta);
            this.indexKey = groupKeys.get(0).getName();
        }

        @Override
        public String getType() {
            return "doublecv";
        }

        @Override
        public Double doReduction(final VariableDataContext context) {
            Double value = (Double) context.getFromContext(srcId, propertyName);
            String indexKeyValue = (String) context.getFromContext(srcId, indexKey);
            context.addContextValue(destProperty.getIdentifier(), indexKey, indexKeyValue);
            return (Double) this.wrapper.addData(value, indexKeyValue);
        }
    }
}
