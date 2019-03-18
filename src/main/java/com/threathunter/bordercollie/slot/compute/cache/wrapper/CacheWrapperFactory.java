package com.threathunter.bordercollie.slot.compute.cache.wrapper;


import com.threathunter.bordercollie.slot.compute.cache.CacheType;
import com.threathunter.bordercollie.slot.compute.cache.StorageType;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.array.*;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.builtin.*;

import java.util.HashMap;
import java.util.Map;

/**
 * 
 */
public class CacheWrapperFactory {
    private static final Map<CacheType, CacheWrapperCreator> ARRAY_REGISTRY = new
            HashMap<>();

    static {
        ARRAY_REGISTRY.put(CacheType.GLOBAL_COUNT, meta -> new GlobalCountsArrayCacheWrapper.GlobalCountArrayCacheWrapper(meta));
        ARRAY_REGISTRY.put(CacheType.GLOBAL_GROUP_COUNT, meta -> new GlobalCountsArrayCacheWrapper.GlobalGroupCountArrayCacheWrapper(meta));
        ARRAY_REGISTRY.put(CacheType.GLOBAL_DISTINCT_COUNT, meta -> new GlobalCountsArrayCacheWrapper.GlobalDistinctCountArrayCacheWrapper(meta));
        ARRAY_REGISTRY.put(CacheType.GLOBAL_SUM_LONG, meta -> new GlobalLongArrayCacheWrapper.GlobalSumLongArrayCacheWrapper(meta));
        ARRAY_REGISTRY.put(CacheType.GLOBAL_GROUP_SUM_LONG, meta -> new GlobalLongArrayCacheWrapper.GlobalGroupSumLongArrayCacheWrapper(meta));
        ARRAY_REGISTRY.put(CacheType.GLOBAL_SUM_DOUBLE, meta -> new GlobalDoubleArrayCacheWrapper.GlobalSumDoubleArrayCacheWrapper(meta));
        ARRAY_REGISTRY.put(CacheType.COUNT, meta -> new CountsArrayCacheWrapper.CountArrayCacheWrapper(meta));
        ARRAY_REGISTRY.put(CacheType.GROUP_COUNT, meta -> new CountsArrayCacheWrapper.GroupCountArrayCacheWrapper(meta));
        ARRAY_REGISTRY.put(CacheType.DISTINCT_COUNT, meta -> new CountsArrayCacheWrapper.DistinctCountArrayCacheWrapper(meta));
        ARRAY_REGISTRY.put(CacheType.SUM_DOUBLE, meta -> new DoubleArrayCacheWrapper.SumDoubleArrayCacheWrapper(meta));
        ARRAY_REGISTRY.put(CacheType.AVG_DOUBLE, meta -> new DoubleArrayCacheWrapper.AvgDoubleArrayCacheWrapper(meta));
        ARRAY_REGISTRY.put(CacheType.FIRST_DOUBLE, meta -> new DoubleArrayCacheWrapper.FirstDoubleArrayCacheWrapper(meta));
        ARRAY_REGISTRY.put(CacheType.LAST_DOUBLE, meta -> new DoubleArrayCacheWrapper.LastDoubleArrayCacheWrapper(meta));
        ARRAY_REGISTRY.put(CacheType.MAX_DOUBLE, meta -> new DoubleArrayCacheWrapper.MaxDoubleArrayCacheWrapper(meta));
        ARRAY_REGISTRY.put(CacheType.MIN_DOUBLE, meta -> new DoubleArrayCacheWrapper.MinDoubleArrayCacheWrapper(meta));
        ARRAY_REGISTRY.put(CacheType.STDDEV_DOUBLE, meta -> new DoubleArrayCacheWrapper.StddevDoubleArrayCacheWrapper(meta));
        ARRAY_REGISTRY.put(CacheType.CV_DOUBLE, meta -> new DoubleArrayCacheWrapper.CVDoubleArrayCacheWrapper(meta));
        ARRAY_REGISTRY.put(CacheType.MIN_LONG, meta -> new LongArrayCacheWrapper.MinLongArrayCacheWrapper(meta));
        ARRAY_REGISTRY.put(CacheType.MAX_LONG, meta -> new LongArrayCacheWrapper.MaxLongArrayCacheWrapper(meta));
        ARRAY_REGISTRY.put(CacheType.FIRST_LONG, meta -> new LongArrayCacheWrapper.FirstLongArrayCacheWrapper(meta));
        ARRAY_REGISTRY.put(CacheType.LAST_LONG, meta -> new LongArrayCacheWrapper.LastLongArrayCacheWrapper(meta));
        ARRAY_REGISTRY.put(CacheType.SUM_LONG, meta -> new LongArrayCacheWrapper.SumLongArrayCacheWrapper(meta));
        ARRAY_REGISTRY.put(CacheType.AVG_LONG, meta -> new LongArrayCacheWrapper.AvgLongArrayCacheWrapper(meta));
        ARRAY_REGISTRY.put(CacheType.STDDEV_LONG, meta -> new LongArrayCacheWrapper.StddevLongArrayCacheWrapper(meta));
        ARRAY_REGISTRY.put(CacheType.CV_LONG, meta -> new LongArrayCacheWrapper.CVLongArrayCacheWrapper(meta));
    }

    public static CacheWrapper createCacheWrapper(final CacheWrapperMeta meta) {
        return ARRAY_REGISTRY.get(meta.getCacheType()).createCacheWrapper(meta);
    }

    public interface CacheWrapperCreator {
        CacheWrapper createCacheWrapper(CacheWrapperMeta meta);
    }
}
