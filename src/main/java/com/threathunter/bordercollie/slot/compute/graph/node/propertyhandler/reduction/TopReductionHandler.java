package com.threathunter.bordercollie.slot.compute.graph.node.propertyhandler.reduction;

import com.threathunter.bordercollie.slot.compute.VariableDataContext;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapper;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapperMeta;
import com.threathunter.bordercollie.slot.util.LimitMaxPriorityQueue;
import com.threathunter.common.Identifier;
import com.threathunter.model.Property;
import com.threathunter.variable.reduction.TopPropertyReduction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * 
 */
public abstract class TopReductionHandler implements ReductionHandler {
    protected final String topKey;
    protected List<Property> srcProperties;
    protected Property destProperty;

    public abstract List<Map<String, Object>> getData();

    public List<CacheWrapper> getCacheWrappers() {
        return new ArrayList<>();
    }

    public TopReductionHandler(final TopPropertyReduction r, final CacheWrapperMeta meta,
                               final List<Property> groupKeys) {
        this.srcProperties = r.getSrcProperties();
        this.destProperty = r.getDestProperty();
        this.topKey = r.getTopKey();
    }

    public static abstract class TopCommonReductionHandler extends TopReductionHandler {
        private String propertyName;
        private Identifier srcId;

        public TopCommonReductionHandler(TopPropertyReduction r, final CacheWrapperMeta meta,
                                         final List<Property> groupKeys) {
            super(r, meta, groupKeys);
            propertyName = srcProperties.get(0).getName();
            srcId = srcProperties.get(0).getIdentifier();
        }

        public String getPropertyName() {
            return propertyName;
        }

        public void setPropertyName(String propertyName) {
            this.propertyName = propertyName;
        }

        public Identifier getSrcId() {
            return srcId;
        }

        public void setSrcId(Identifier srcId) {
            this.srcId = srcId;
        }
    }

    public static class DoubleTopNReductionHandler extends TopCommonReductionHandler {
        private final LimitMaxPriorityQueue topQueue;

        @Override
        public List<Map<String, Object>> getData() {
            return this.topQueue.getCopy();
        }

        public DoubleTopNReductionHandler(TopPropertyReduction r, final CacheWrapperMeta meta,
                                          final List<Property> groupKeys) {
            super(r, meta, groupKeys);
            this.topQueue = new LimitMaxPriorityQueue(100);
        }

        @Override
        public String getType() {
            // TODO doubledoubletop is not right, refer to TopPropertyReduction in threathunter_common
            return "doubledoubletop";
        }

        /**
         * Will compute every time, differ from the keytop.
         *
         * But do not allowed children
         *
         * @param context
         * @return
         */
        @Override
        public Object doReduction(VariableDataContext context) {
            Number value = (Number) context.getFromContext(getSrcId(), getPropertyName());
            this.topQueue.update((String) context.getFromContext(getSrcId(), this.topKey), value);
            return null;
        }
    }

    public static class LongTopNReductionHandler extends TopCommonReductionHandler {
        private final LimitMaxPriorityQueue topQueue;

        @Override
        public List<Map<String, Object>> getData() {
            return this.topQueue.getCopy();
        }

        public LongTopNReductionHandler(TopPropertyReduction r, final CacheWrapperMeta meta,
                                        final List<Property> groupKeys) {
            super(r, meta, groupKeys);
            this.topQueue = new LimitMaxPriorityQueue(100);
        }

        @Override
        public String getType() {
            // TODO doubledoubletop is not right, refer to TopPropertyReduction in threathunter_common
            return "longlongtop";
        }

        @Override
        public Object doReduction(VariableDataContext context) {
            Number value = (Number) context.getFromContext(getSrcId(), getPropertyName());
            this.topQueue.update((String) context.getFromContext(getSrcId(), this.topKey), value);
            return null;
        }
    }

    public static class DoubleKeyTopNReductionHandler extends
            TopCommonReductionHandler {

        @Override
        public List<Map<String, Object>> getData() {
            return null;
        }

        public DoubleKeyTopNReductionHandler(TopPropertyReduction r, final CacheWrapperMeta meta,
                                             final List<Property> groupKeys) {
            super(r, meta, groupKeys);
        }

        @Override
        public String getType() {
            return "doublemaptop";
        }

        @Override
        public Object doReduction(VariableDataContext context) {
//            return context.getFromContext(getSrcId(), getPropertyName());
            // TODO no children allowed.
            return null;
        }
    }

    public static class LongKeyTopNReductionHandler extends
            TopCommonReductionHandler {

        @Override
        public List<Map<String, Object>> getData() {
            return null;
        }

        public LongKeyTopNReductionHandler(TopPropertyReduction r, final CacheWrapperMeta meta,
                                           final List<Property> groupKeys) {
            super(r, meta, groupKeys);
        }

        @Override
        public String getType() {
            return "longmaptop";
        }

        @Override
        public Object doReduction(VariableDataContext context) {
            // TODO no children allowed.
//            return context.getFromContext(getSrcId(), getPropertyName());
            return null;
        }
    }
}
