package com.threathunter.bordercollie.slot.compute.graph.node.propertyhandler.reduction;

import com.threathunter.bordercollie.slot.compute.VariableDataContext;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapper;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapperMeta;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.array.CountsArrayCacheWrapper;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.array.GlobalCountsArrayCacheWrapper;
import com.threathunter.common.Identifier;
import com.threathunter.model.Property;
import com.threathunter.variable.reduction.StringPropertyReduction;

import java.util.Arrays;
import java.util.List;

/**
 * 
 */
public abstract class StringReductionHandler implements ReductionHandler {
    protected List<Property> srcProperties;
    protected Property destProperty;
    protected CacheWrapper wrapper;

    public StringReductionHandler(final StringPropertyReduction r, final CacheWrapperMeta meta, final List<Property> groupKeys) {
        this.srcProperties = r.getSrcProperties();
        this.destProperty = r.getDestProperty();
    }

    public List<CacheWrapper> getCacheWrappers() {
        return Arrays.asList(wrapper);
    }

    public static class GlobalStringDistinctCountReductionHandler extends StringReductionHandler {
        private String distinctKey;
        private Identifier srcId;

        public GlobalStringDistinctCountReductionHandler(final StringPropertyReduction r, final CacheWrapperMeta meta, final List<Property> groupKeys) {
            super(r, meta, groupKeys);
            this.distinctKey = srcProperties.get(0).getName();
            this.srcId = srcProperties.get(0).getIdentifier();
            this.wrapper = new GlobalCountsArrayCacheWrapper.GlobalDistinctCountArrayCacheWrapper(meta);
        }

        @Override
        public String getType() {
            return "distinctcount";
        }

        @Override
        public Integer doReduction(final VariableDataContext context) {
            String value =  (String) context.getFromContext(srcId, distinctKey);
            return (Integer) this.wrapper.addData(value);
        }
    }

    public static class StringCountReductionHandler extends StringReductionHandler {
        private String indexKey;
        private Identifier srcId;

        public StringCountReductionHandler(final StringPropertyReduction r, final CacheWrapperMeta meta, final List<Property> groupKeys) {
            super(r, meta, groupKeys);
            this.indexKey = groupKeys.get(0).getName();
            this.wrapper = new CountsArrayCacheWrapper.CountArrayCacheWrapper(meta);
            this.srcId = r.getSrcProperties().get(0).getIdentifier();
        }

        @Override
        public String getType() {
            return "count";
        }

        @Override
        public Integer doReduction(final VariableDataContext context) {
            String indexKeyValue = (String) context.getFromContext(srcId, indexKey);
            context.addContextValue(destProperty.getIdentifier(), indexKey, indexKeyValue);
            return (Integer) this.wrapper.addData(1, indexKeyValue);
        }
    }

    public static class StringDistinctCountReductionHandler extends StringReductionHandler {
        private String distinctKey;
        private Identifier srcId;

        private String indexKey;

        public StringDistinctCountReductionHandler(final StringPropertyReduction r, final CacheWrapperMeta meta, final List<Property> groupKeys) {
            super(r, meta, groupKeys);
            this.distinctKey = srcProperties.get(0).getName();
            this.srcId = srcProperties.get(0).getIdentifier();
            this.indexKey = groupKeys.get(0).getName();
            this.wrapper = new CountsArrayCacheWrapper.DistinctCountArrayCacheWrapper(meta);
        }

        @Override
        public String getType() {
            return "distinctcount";
        }

        @Override
        public Integer doReduction(final VariableDataContext context) {
            String value =  (String) context.getFromContext(srcId, distinctKey);
            String indexKeyValue = (String) context.getFromContext(srcId, indexKey);
            context.addContextValue(destProperty.getIdentifier(), indexKey, indexKeyValue);
            return (Integer) this.wrapper.addData(value, indexKeyValue);
        }
    }

    public static class StringListDistinctCountReductionHandler extends StringReductionHandler {
        private String distinctName;
        private Identifier srcId;

        private String indexKey;

        public StringListDistinctCountReductionHandler(final StringPropertyReduction r, final CacheWrapperMeta meta, final List<Property> groupKeys) {
            super(r, meta, groupKeys);
            distinctName = srcProperties.get(0).getName();
            srcId = srcProperties.get(0).getIdentifier();
            this.indexKey = groupKeys.get(0).getName();
            this.wrapper = new CountsArrayCacheWrapper.DistinctCountArrayCacheWrapper(meta);
        }

        @Override
        public String getType() {
            return "distinctcount";
        }

        @Override
        public Integer doReduction(final VariableDataContext context) {
            Object list = context.getFromContext(srcId, distinctName);
            if (list == null) {
                return null;
            }
            String index = (String) context.getFromContext(srcId, indexKey);
            if (list instanceof List) {
                ((List) list).forEach(data -> wrapper.addData(data, index));
            } else {
                for (String data : ((String) list).split(",")) {
                    wrapper.addData(data, index);
                }
            }
            context.addContextValue(destProperty.getIdentifier(), indexKey, index);

            return (Integer) wrapper.getData(index);
        }
    }
}
