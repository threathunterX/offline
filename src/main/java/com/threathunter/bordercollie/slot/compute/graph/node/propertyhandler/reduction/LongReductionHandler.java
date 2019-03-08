package com.threathunter.bordercollie.slot.compute.graph.node.propertyhandler.reduction;

import com.threathunter.bordercollie.slot.compute.VariableDataContext;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapper;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapperFactory;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapperMeta;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.array.CountsArrayCacheWrapper;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.array.GlobalCountsArrayCacheWrapper;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.array.GlobalLongArrayCacheWrapper;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.array.LongArrayCacheWrapper;
import com.threathunter.common.Identifier;
import com.threathunter.model.Property;
import com.threathunter.variable.reduction.LongPropertyReduction;

import java.util.Arrays;
import java.util.List;

/**
 * Created by daisy on 16/5/20.
 */
public abstract class LongReductionHandler implements ReductionHandler {
    protected List<Property> srcProperties;
    protected Property destProperty;
    protected CacheWrapper wrapper;

    public LongReductionHandler(final LongPropertyReduction r, final CacheWrapperMeta meta, final List<Property> groupKeys) {
        this.srcProperties = r.getSrcProperties();
        this.destProperty = r.getDestProperty();
    }

    public List<CacheWrapper> getCacheWrappers() {
        return Arrays.asList(wrapper);
    }

    public static class LongCountReductionHandler extends LongReductionHandler {
        private Identifier srcId;
        private String indexKey;

        public LongCountReductionHandler(final LongPropertyReduction r, final CacheWrapperMeta meta,
                                         final List<Property> groupKeys) {
            super(r, meta, groupKeys);
            srcId = srcProperties.get(0).getIdentifier();
            this.indexKey = groupKeys.get(0).getName();
            this.wrapper = new CountsArrayCacheWrapper.CountArrayCacheWrapper(meta);
        }

        @Override
        public String getType() {
            return "count";
        }

        @Override
        public Long doReduction(final VariableDataContext context) {
            String indexKeyValue = (String) context.getFromContext(srcId, indexKey);
            context.addContextValue(destProperty.getIdentifier(), indexKey, indexKeyValue);
            return (Long) wrapper.addData(1, indexKeyValue);
        }
    }

    public static class GlobalLongCountReductionHandler extends LongReductionHandler {
        private Identifier srcId;

        public GlobalLongCountReductionHandler(final LongPropertyReduction r, final CacheWrapperMeta meta,
                                         final List<Property> groupKeys) {
            super(r, meta, groupKeys);
            srcId = srcProperties.get(0).getIdentifier();
            this.wrapper = new GlobalCountsArrayCacheWrapper.GlobalCountArrayCacheWrapper(meta);
        }

        @Override
        public String getType() {
            return "count";
        }

        @Override
        public Long doReduction(final VariableDataContext context) {
            return (Long) wrapper.addData(1);
        }
    }

    public static class LongSumReductionHandler extends LongReductionHandler {
        private String propertyName;
        private Identifier srcId;
        private String indexKey;

        public LongSumReductionHandler(final LongPropertyReduction r, final CacheWrapperMeta meta,
                                       final List<Property> groupKeys) {
            super(r, meta, groupKeys);
            propertyName = srcProperties.get(0).getName();
            srcId = srcProperties.get(0).getIdentifier();
            indexKey = groupKeys.get(0).getName();
            this.wrapper = new LongArrayCacheWrapper.SumLongArrayCacheWrapper(meta);
        }

        @Override
        public String getType() {
            return "longsum";
        }

        @Override
        public Long doReduction(final VariableDataContext context) {
            Long value = (Long) context.getFromContext(srcId, propertyName);
            String indexKeyValue = (String) context.getFromContext(srcId, indexKey);
            context.addContextValue(destProperty.getIdentifier(), indexKey, indexKeyValue);
            return (Long) wrapper.addData(value, indexKeyValue);
        }
    }
    public static class GlobalLongSumReductionHandler extends LongReductionHandler {
        private String propertyName;
        private Identifier srcId;

        public GlobalLongSumReductionHandler(final LongPropertyReduction r, final CacheWrapperMeta meta,
                                       final List<Property> groupKeys) {
            super(r, meta, groupKeys);
            propertyName = srcProperties.get(0).getName();
            srcId = srcProperties.get(0).getIdentifier();
            this.wrapper = new GlobalLongArrayCacheWrapper.GlobalSumLongArrayCacheWrapper(meta);
        }

        @Override
        public String getType() {
            return "longsum";
        }

        @Override
        public Long doReduction(final VariableDataContext context) {
            Long value = (Long) context.getFromContext(srcId, propertyName);
            return (Long) wrapper.addData(value);
        }
    }
    public static class GlobalLongGroupSumReductionHandler extends LongReductionHandler {
        private String propertyName;
        private Identifier srcId;
        private String groupPropertyName;

        public GlobalLongGroupSumReductionHandler(final LongPropertyReduction r, final CacheWrapperMeta meta,
                                                  final List<Property> groupKeys) {
            super(r, meta, groupKeys);
            propertyName = srcProperties.get(0).getName();
            srcId = srcProperties.get(0).getIdentifier();
            this.groupPropertyName = ((LongPropertyReduction.LongGroupSumPropertyReduction) r).getGroupProperty().getName();
            this.wrapper = new GlobalLongArrayCacheWrapper.GlobalGroupSumLongArrayCacheWrapper(meta);
        }

        @Override
        public String getType() {
            return "longgroup_sum";
        }

        @Override
        public Long doReduction(final VariableDataContext context) {
            Long value = (Long) context.getFromContext(srcId, propertyName);
            return (Long) wrapper.addData(value, (String) context.getFromContext(srcId, groupPropertyName));
        }
    }

    public static class GlobalLongGroupCountReductionHandler extends LongReductionHandler {
        private String propertyName;
        private Identifier srcId;
        private String groupPropertyName;

        public GlobalLongGroupCountReductionHandler(final LongPropertyReduction r, final CacheWrapperMeta meta,
                                                    final List<Property> groupKeys) {
            super(r, meta, groupKeys);
            propertyName = srcProperties.get(0).getName();
            srcId = srcProperties.get(0).getIdentifier();
            this.groupPropertyName = ((LongPropertyReduction.LongGroupCountPropertyReduction) r).getGroupProperty().getName();
            this.wrapper = new GlobalCountsArrayCacheWrapper.GlobalGroupCountArrayCacheWrapper(meta);
        }

        @Override
        public String getType() {
            return "longgroup_count";
        }

        @Override
        public Long doReduction(final VariableDataContext context) {
            return (Long) this.wrapper.addData(1, (String) context.getFromContext(srcId, groupPropertyName));
        }
    }

    public static class LongMinReductionHandler extends LongReductionHandler {
        private String propertyName;
        private Identifier srcId;
        private String indexKey;

        public LongMinReductionHandler(final LongPropertyReduction r, final CacheWrapperMeta meta,
                                       final List<Property> groupKeys) {
            super(r, meta, groupKeys);
            propertyName = srcProperties.get(0).getName();
            srcId = srcProperties.get(0).getIdentifier();
            this.indexKey = groupKeys.get(0).getName();
            this.wrapper = new LongArrayCacheWrapper.MinLongArrayCacheWrapper(meta);
        }

        @Override
        public String getType() {
            return "longmin";
        }

        @Override
        public Long doReduction(final VariableDataContext context) {
            Long value = (Long) context.getFromContext(srcId, propertyName);
            String indexKeyValue = (String) context.getFromContext(srcId, indexKey);
            context.addContextValue(destProperty.getIdentifier(), indexKey, indexKeyValue);
            return (Long) this.wrapper.addData(value, indexKeyValue);
        }
    }

    public static class LongFirstReductionHandler extends LongReductionHandler {
        private String propertyName;
        private Identifier srcId;

        private String indexKey;

        public LongFirstReductionHandler(final LongPropertyReduction r, final CacheWrapperMeta meta,
                                         final List<Property> groupKeys) {
            super(r, meta, groupKeys);
            propertyName = srcProperties.get(0).getName();
            srcId = srcProperties.get(0).getIdentifier();
            this.indexKey = groupKeys.get(0).getName();
            this.wrapper = new LongArrayCacheWrapper.FirstLongArrayCacheWrapper(meta);
        }

        @Override
        public String getType() {
            return "longfirst";
        }

        @Override
        public Long doReduction(final VariableDataContext context) {
            Long value = (Long) context.getFromContext(srcId, propertyName);
            String indexKeyValue = (String) context.getFromContext(srcId, indexKey);
            context.addContextValue(destProperty.getIdentifier(), indexKey, indexKeyValue);
            return (Long) this.wrapper.addData(value, indexKeyValue);
        }
    }

    public static class LongLastReductionHandler extends LongReductionHandler {
        private String propertyName;
        private Identifier srcId;

        private String indexKey;

        public LongLastReductionHandler(final LongPropertyReduction r, final CacheWrapperMeta meta,
                                        final List<Property> groupKeys) {
            super(r, meta, groupKeys);
            propertyName = srcProperties.get(0).getName();
            srcId = srcProperties.get(0).getIdentifier();
            this.indexKey = groupKeys.get(0).getName();
            this.wrapper = new LongArrayCacheWrapper.LastLongArrayCacheWrapper(meta);
        }

        @Override
        public String getType() {
            return "longlast";
        }

        @Override
        public Long doReduction(final VariableDataContext context) {
            Long value = (Long) context.getFromContext(srcId, propertyName);
            String indexKeyValue = (String) context.getFromContext(srcId, indexKey);
            context.addContextValue(destProperty.getIdentifier(), indexKey, indexKeyValue);
            return (Long) this.wrapper.addData(value, indexKeyValue);
        }
    }

    public static class LongAvgReductionHandler extends LongReductionHandler {
        private String propertyName;
        private Identifier srcId;
        private String indexKey;

        public LongAvgReductionHandler(final LongPropertyReduction r, final CacheWrapperMeta meta,
                                       final List<Property> groupKeys) {
            super(r, meta, groupKeys);
            propertyName = srcProperties.get(0).getName();
            srcId = srcProperties.get(0).getIdentifier();
            this.indexKey = groupKeys.get(0).getName();
            this.wrapper = new LongArrayCacheWrapper.AvgLongArrayCacheWrapper(meta);
        }

        @Override
        public String getType() {
            return "longavg";
        }

        @Override
        public Long doReduction(final VariableDataContext context) {
            Long value = (Long) context.getFromContext(srcId, propertyName);
            String indexKeyValue = (String) context.getFromContext(srcId, indexKey);
            context.addContextValue(destProperty.getIdentifier(), indexKey, indexKeyValue);
            return (Long) this.wrapper.addData(value, indexKeyValue);
        }
    }

    public static class LongStddevReductionHandler extends LongReductionHandler {
        private String propertyName;
        private Identifier srcId;

        private String indexKey;

        public LongStddevReductionHandler(final LongPropertyReduction r, final CacheWrapperMeta meta,
                                          final List<Property> groupKeys) {
            super(r, meta, groupKeys);
            propertyName = srcProperties.get(0).getName();
            srcId = srcProperties.get(0).getIdentifier();
            this.indexKey = groupKeys.get(0).getName();
            this.wrapper = new LongArrayCacheWrapper.StddevLongArrayCacheWrapper(meta);
        }

        @Override
        public String getType() {
            return "longstddev";
        }

        @Override
        public Long doReduction(final VariableDataContext context) {
            Long value = (Long) context.getFromContext(srcId, propertyName);
            String indexKeyValue = (String) context.getFromContext(srcId, indexKey);
            context.addContextValue(destProperty.getIdentifier(), indexKey, indexKeyValue);
            return (Long) this.wrapper.addData(value, indexKeyValue);
        }
    }

    public static class LongCVReductionHandler extends LongReductionHandler {
        private String propertyName;
        private Identifier srcId;

        private String indexKey;

        public LongCVReductionHandler(final LongPropertyReduction r, final CacheWrapperMeta meta,
                                          final List<Property> groupKeys) {
            super(r, meta, groupKeys);
            propertyName = srcProperties.get(0).getName();
            srcId = srcProperties.get(0).getIdentifier();
            this.indexKey = groupKeys.get(0).getName();
            this.wrapper = new LongArrayCacheWrapper.CVLongArrayCacheWrapper(meta);
        }

        @Override
        public String getType() {
            return "longcv";
        }

        @Override
        public Long doReduction(final VariableDataContext context) {
            Long value = (Long) context.getFromContext(srcId, propertyName);
            String indexKeyValue = (String) context.getFromContext(srcId, indexKey);
            context.addContextValue(destProperty.getIdentifier(), indexKey, indexKeyValue);
            return (Long) this.wrapper.addData(value, indexKeyValue);
        }
    }
}
