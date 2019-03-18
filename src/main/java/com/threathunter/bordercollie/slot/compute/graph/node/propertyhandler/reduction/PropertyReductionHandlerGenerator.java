package com.threathunter.bordercollie.slot.compute.graph.node.propertyhandler.reduction;

import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapperMeta;
import com.threathunter.bordercollie.slot.util.ClassBasedRegistry;
import com.threathunter.model.Property;
import com.threathunter.model.PropertyReduction;
import com.threathunter.variable.exception.NotSupportException;
import com.threathunter.variable.reduction.*;

import java.util.List;

/**
 * 
 */
public abstract class PropertyReductionHandlerGenerator<R extends PropertyReduction> {
    private static final ClassBasedRegistry<PropertyReduction, PropertyReductionHandlerGenerator> registry =
            new ClassBasedRegistry<>(PropertyReduction.class);

    static {
        registerReduction(LongPropertyReduction.class, LongReductionHandlerGenerator.class);
        registerReduction(DoublePropertyReduction.class, DoubleReductionHandlerGenerator.class);
        registerReduction(StringPropertyReduction.class, StringReductionHandlerGenerator.class);
        registerReduction(TopPropertyReduction.class, TopPropertyReductionHandlerGenerator.class);
    }

    public static void registerReduction(Class<? extends PropertyReduction> c, Class<? extends PropertyReductionHandlerGenerator> g) {
        registry.register(c, g);
    }

    public static ReductionHandler generateReductionHandler(CacheWrapperMeta meta, List<Property> groupKeys, PropertyReduction r) {
        Class<? extends PropertyReductionHandlerGenerator> handlerClass = registry.get(r.getClass());
        if (handlerClass == null) {
            return null;
        }

        try {
            PropertyReductionHandlerGenerator handlerGenerator = handlerClass.newInstance();
            return handlerGenerator.generateHandler(meta, groupKeys, r);
        } catch (Exception e) {
            throw new RuntimeException("error in property mapping handler generation.", e);
        }
    }

    public abstract ReductionHandler generateHandler(CacheWrapperMeta meta, List<Property> groupKeys, R r);

    public static class LongReductionHandlerGenerator extends PropertyReductionHandlerGenerator<LongPropertyReduction> {

        @Override
        public ReductionHandler generateHandler(CacheWrapperMeta meta, List<Property> groupKeys, LongPropertyReduction longPropertyReduction) {
            String type = longPropertyReduction.getType();
            if (type.equals("longgroup_count")) {
                if (meta.getIndexCount() == 0) {
                    return new LongReductionHandler.GlobalLongGroupCountReductionHandler(longPropertyReduction, meta, groupKeys);
                }
            }
            if (type.equals("longcount")) {
                if (meta.getIndexCount() == 0) {
                    return new LongReductionHandler.GlobalLongCountReductionHandler(longPropertyReduction, meta, groupKeys);
                }
                return new LongReductionHandler.LongCountReductionHandler(longPropertyReduction, meta, groupKeys);
            }
            if (type.equals("longsum")) {
                if (meta.getIndexCount() == 0) {
                    return new LongReductionHandler.GlobalLongSumReductionHandler(longPropertyReduction, meta, groupKeys);
                }
                return new LongReductionHandler.LongSumReductionHandler(longPropertyReduction, meta, groupKeys);
            }
            if (type.equals("longgroup_sum")) {
                if (meta.getIndexCount() == 0) {
                    return new LongReductionHandler.GlobalLongGroupSumReductionHandler(longPropertyReduction, meta, groupKeys);
                }
            }
            if (type.equals("longmin")) {
                if (meta.getIndexCount() == 1) {
                    return new LongReductionHandler.LongMinReductionHandler(longPropertyReduction, meta, groupKeys);
                }
            }
            if (type.equals("longlast")) {
                if (meta.getIndexCount() == 1) {
                    return new LongReductionHandler.LongLastReductionHandler(longPropertyReduction, meta, groupKeys);
                }
            }
            if (type.equals("longfirst")) {
                return new LongReductionHandler.LongFirstReductionHandler(longPropertyReduction, meta, groupKeys);
            }
            if (type.equals("longavg")) {
                return new LongReductionHandler.LongAvgReductionHandler(longPropertyReduction, meta, groupKeys);
            }
            if (type.equals("longstddev")) {
                return new LongReductionHandler.LongStddevReductionHandler(longPropertyReduction, meta, groupKeys);
            }
            if (type.equals("longcv")) {
                return new LongReductionHandler.LongCVReductionHandler(longPropertyReduction, meta, groupKeys);
            }
            throw new NotSupportException("reduction type is not support: " + longPropertyReduction.getType());
        }
    }

    public static class DoubleReductionHandlerGenerator extends PropertyReductionHandlerGenerator<DoublePropertyReduction> {

        @Override
        public ReductionHandler generateHandler(CacheWrapperMeta meta, List<Property> groupKeys, DoublePropertyReduction r) {
            if (r.getType().equals("doublesum")) {
                return new DoubleReductionHandler.DoubleSumReductionHandler(r, meta, groupKeys);
            }
            if (r.getType().equals("doublegroup_count")) {
                if (meta.getIndexCount() == 0) {
                    return new DoubleReductionHandler.GlobalDoubleGroupCountReductionHandler(r, meta, groupKeys);
                }
                return new DoubleReductionHandler.DoubleGroupCountReductionHandler(r, meta, groupKeys);
            }
            if (r.getType().equals("doublecount")) {
                if (meta.getIndexCount() == 0) {
                    return new DoubleReductionHandler.GlobalDoubleCountReductionHandler(r, meta, groupKeys);
                }
                return new DoubleReductionHandler.DoubleCountReductionHandler(r, meta, groupKeys);
            }
            if (r.getType().equals("doublestddev")) {
                return new DoubleReductionHandler.DoubleStddevReductionHandler(r, meta, groupKeys);
            }
            if (r.getType().equals("doublecv")) {
                return new DoubleReductionHandler.DoubleCVReductionHandler(r, meta, groupKeys);
            }
            return null;
        }
    }

    public static class StringReductionHandlerGenerator extends PropertyReductionHandlerGenerator<StringPropertyReduction> {

        @Override
        public ReductionHandler generateHandler(CacheWrapperMeta meta, List<Property> groupKeys, StringPropertyReduction r) {
            if (r.getType().equals("stringcount")) {
                return new StringReductionHandler.StringCountReductionHandler(r, meta, groupKeys);
            }
            if (r.getType().equals("stringdistinct_count")) {
                if (meta.getIndexCount() == 0) {
                    return new StringReductionHandler.GlobalStringDistinctCountReductionHandler(r, meta, groupKeys);
                }
                return new StringReductionHandler.StringDistinctCountReductionHandler(r, meta, groupKeys);
            }
            if (r.getType().equals("stringlistdistinct_count")) {
                return new StringReductionHandler.StringListDistinctCountReductionHandler(r, meta, groupKeys);
            }

            throw new NotSupportException("reduction type is not support: " + r.getType());

        }
    }

    public static class TopPropertyReductionHandlerGenerator extends PropertyReductionHandlerGenerator<TopPropertyReduction> {
        @Override
        public ReductionHandler generateHandler(CacheWrapperMeta meta, List<Property> groupKeys, TopPropertyReduction r) {
            if (r.getType().equals("doubledoubletop")) {
                return new TopReductionHandler.DoubleTopNReductionHandler(r, meta, groupKeys);
            }
            if (r.getType().equals("longlongtop")) {
                return new TopReductionHandler.LongTopNReductionHandler(r, meta, groupKeys);
            }
            if (r.getType().equals("doublemaptop")) {
                return new TopReductionHandler.DoubleKeyTopNReductionHandler(r, meta, groupKeys);
            }
            if (r.getType().equals("longmaptop")) {
                return new TopReductionHandler.LongKeyTopNReductionHandler(r, meta, groupKeys);
            }
            throw new NotSupportException("reduction type is not support: " + r.getType());
        }
    }
}
