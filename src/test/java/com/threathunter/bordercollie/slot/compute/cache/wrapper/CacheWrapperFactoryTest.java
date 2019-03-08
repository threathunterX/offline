package com.threathunter.bordercollie.slot.compute.cache.wrapper;

import com.threathunter.bordercollie.slot.compute.cache.CacheType;
import com.threathunter.bordercollie.slot.compute.cache.StorageType;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.array.*;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.builtin.*;
import com.threathunter.bordercollie.slot.util.HashType;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by toyld on 3/31/17.
 */
public class CacheWrapperFactoryTest {

    @Test
    public void testCreateCacheWrapper() throws Exception {
        CacheWrapperMeta meta = new CacheWrapperMeta();

        meta.setStorageType(StorageType.BUILDIN);
        meta.setCacheType(CacheType.COUNT);
        CacheWrapper wrapper = CacheWrapperFactory.createCacheWrapper(meta);
        Assert.assertTrue(wrapper instanceof CountsBuiltinCacheWrapper.CountBuiltinCacheWrapper);

        meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.COUNT);
        meta.setStorageType(StorageType.BYTES_ARRAY);
        wrapper = CacheWrapperFactory.createCacheWrapper(meta);
        Assert.assertTrue(wrapper instanceof CountsArrayCacheWrapper.CountArrayCacheWrapper);

        meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.DISTINCT_COUNT);
        meta.setStorageType(StorageType.BUILDIN);
        wrapper = CacheWrapperFactory.createCacheWrapper(meta);
        Assert.assertTrue(wrapper instanceof CountsBuiltinCacheWrapper.DistinctCountBuiltinCacheWrapper);

        meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.DISTINCT_COUNT);
        meta.setStorageType(StorageType.BYTES_ARRAY);
        meta.setValueHashType(HashType.IP);
        wrapper = CacheWrapperFactory.createCacheWrapper(meta);
        Assert.assertTrue(wrapper instanceof CountsArrayCacheWrapper.DistinctCountArrayCacheWrapper);

        meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.GLOBAL_COUNT);
        meta.setStorageType(StorageType.BUILDIN);
        wrapper = CacheWrapperFactory.createCacheWrapper(meta);
        Assert.assertTrue(wrapper instanceof GlobalCountsBuiltinCacheWrapper.GlobalCountBuiltinCacheWrapper);

        meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.GLOBAL_COUNT);
        meta.setStorageType(StorageType.BYTES_ARRAY);
        wrapper = CacheWrapperFactory.createCacheWrapper(meta);
        Assert.assertTrue(wrapper instanceof GlobalCountsArrayCacheWrapper.GlobalCountArrayCacheWrapper);

        meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.GLOBAL_DISTINCT_COUNT);
        meta.setStorageType(StorageType.BUILDIN);
        wrapper = CacheWrapperFactory.createCacheWrapper(meta);
        Assert.assertTrue(wrapper instanceof GlobalCountsBuiltinCacheWrapper.GlobalDistinctCountBuiltinCacheWrapper);

        meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.GLOBAL_DISTINCT_COUNT);
        meta.setStorageType(StorageType.BYTES_ARRAY);
        meta.setValueHashType(HashType.IP);
        wrapper = CacheWrapperFactory.createCacheWrapper(meta);
        Assert.assertTrue(wrapper instanceof GlobalCountsArrayCacheWrapper.GlobalDistinctCountArrayCacheWrapper);

        meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.FIRST_DOUBLE);
        meta.setStorageType(StorageType.BUILDIN);
        wrapper = CacheWrapperFactory.createCacheWrapper(meta);
        Assert.assertTrue(wrapper instanceof DoubleBuiltinCacheWrapper.FirstDoubleBuiltinCacheWrapper);

        meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.FIRST_DOUBLE);
        meta.setStorageType(StorageType.BYTES_ARRAY);
        wrapper = CacheWrapperFactory.createCacheWrapper(meta);
        Assert.assertTrue(wrapper instanceof DoubleArrayCacheWrapper.FirstDoubleArrayCacheWrapper);

        meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.LAST_DOUBLE);
        meta.setStorageType(StorageType.BUILDIN);
        wrapper = CacheWrapperFactory.createCacheWrapper(meta);
        Assert.assertTrue(wrapper instanceof DoubleBuiltinCacheWrapper.LastDoubleBuiltinCacheWrapper);

        meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.LAST_DOUBLE);
        meta.setStorageType(StorageType.BYTES_ARRAY);
        wrapper = CacheWrapperFactory.createCacheWrapper(meta);
        Assert.assertTrue(wrapper instanceof DoubleArrayCacheWrapper.LastDoubleArrayCacheWrapper);

        meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.AVG_DOUBLE);
        meta.setStorageType(StorageType.BUILDIN);
        wrapper = CacheWrapperFactory.createCacheWrapper(meta);
        Assert.assertTrue(wrapper instanceof DoubleBuiltinCacheWrapper.AvgDoubleBuiltinCacheWrapper);

        meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.AVG_DOUBLE);
        meta.setStorageType(StorageType.BYTES_ARRAY);
        wrapper = CacheWrapperFactory.createCacheWrapper(meta);
        Assert.assertTrue(wrapper instanceof DoubleArrayCacheWrapper.AvgDoubleArrayCacheWrapper);

        meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.MIN_DOUBLE);
        meta.setStorageType(StorageType.BUILDIN);
        wrapper = CacheWrapperFactory.createCacheWrapper(meta);
        Assert.assertTrue(wrapper instanceof DoubleBuiltinCacheWrapper.MinDoubleBuiltinCacheWrapper);

        meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.MIN_DOUBLE);
        meta.setStorageType(StorageType.BYTES_ARRAY);
        wrapper = CacheWrapperFactory.createCacheWrapper(meta);
        Assert.assertTrue(wrapper instanceof DoubleArrayCacheWrapper.MinDoubleArrayCacheWrapper);

        meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.FIRST_LONG);
        meta.setStorageType(StorageType.BUILDIN);
        wrapper = CacheWrapperFactory.createCacheWrapper(meta);
        Assert.assertTrue(wrapper instanceof LongBuiltinCacheWrapper.FirstLongBuiltinCacheWrapper);

        meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.FIRST_LONG);
        meta.setStorageType(StorageType.BYTES_ARRAY);
        wrapper = CacheWrapperFactory.createCacheWrapper(meta);
        Assert.assertTrue(wrapper instanceof LongArrayCacheWrapper.FirstLongArrayCacheWrapper);

        meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.LAST_LONG);
        meta.setStorageType(StorageType.BUILDIN);
        wrapper = CacheWrapperFactory.createCacheWrapper(meta);
        Assert.assertTrue(wrapper instanceof LongBuiltinCacheWrapper.LastLongBuiltinCacheWrapper);

        meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.LAST_LONG);
        meta.setStorageType(StorageType.BYTES_ARRAY);
        wrapper = CacheWrapperFactory.createCacheWrapper(meta);
        Assert.assertTrue(wrapper instanceof LongArrayCacheWrapper.LastLongArrayCacheWrapper);

        meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.MAX_LONG);
        meta.setStorageType(StorageType.BUILDIN);
        wrapper = CacheWrapperFactory.createCacheWrapper(meta);
        Assert.assertTrue(wrapper instanceof LongBuiltinCacheWrapper.MaxLongBuiltinCacheWrapper);

        meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.MAX_LONG);
        meta.setStorageType(StorageType.BYTES_ARRAY);
        wrapper = CacheWrapperFactory.createCacheWrapper(meta);
        Assert.assertTrue(wrapper instanceof LongArrayCacheWrapper.MaxLongArrayCacheWrapper);

        meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.MIN_LONG);
        meta.setStorageType(StorageType.BUILDIN);
        wrapper = CacheWrapperFactory.createCacheWrapper(meta);
        Assert.assertTrue(wrapper instanceof LongBuiltinCacheWrapper.MinLongBuiltinCacheWrapper);

        meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.MIN_LONG);
        meta.setStorageType(StorageType.BYTES_ARRAY);
        wrapper = CacheWrapperFactory.createCacheWrapper(meta);
        Assert.assertTrue(wrapper instanceof LongArrayCacheWrapper.MinLongArrayCacheWrapper);

        meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.SECONDARY_COUNT);
        meta.setStorageType(StorageType.BUILDIN);
        wrapper = CacheWrapperFactory.createCacheWrapper(meta);
        Assert.assertTrue(wrapper instanceof SecondaryCountsBuiltinCacheWrapper.SecondaryCountBuiltinCacheWrapper);

        meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.SECONDARY_COUNT);
        meta.setStorageType(StorageType.BYTES_ARRAY);
        wrapper = CacheWrapperFactory.createCacheWrapper(meta);
        Assert.assertTrue(wrapper instanceof SecondaryCountsArrayCacheWrapper.SecondaryCountArrayCacheWrapper);

        meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.SECONDARY_DISTINCT_COUNT);
        meta.setStorageType(StorageType.BUILDIN);
        wrapper = CacheWrapperFactory.createCacheWrapper(meta);
        Assert.assertTrue(wrapper instanceof SecondaryCountsBuiltinCacheWrapper.SecondaryDistinctCountBuiltinCacheWrapper);

        meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.SECONDARY_DISTINCT_COUNT);
        meta.setStorageType(StorageType.BYTES_ARRAY);
        meta.setValueHashType(HashType.IP);
        wrapper = CacheWrapperFactory.createCacheWrapper(meta);
        Assert.assertTrue(wrapper instanceof SecondaryCountsArrayCacheWrapper.SecondaryDistinctCountArrayCacheWrapper);

        meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.SECONDARY_FIRST_DOUBLE);
        meta.setStorageType(StorageType.BUILDIN);
        wrapper = CacheWrapperFactory.createCacheWrapper(meta);
        Assert.assertTrue(wrapper instanceof SecondaryDoubleBuiltinCacheWrapper.SecondaryFirstDoubleBuiltinCacheWrapper);

        meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.SECONDARY_FIRST_DOUBLE);
        meta.setStorageType(StorageType.BYTES_ARRAY);
        wrapper = CacheWrapperFactory.createCacheWrapper(meta);
        Assert.assertTrue(wrapper instanceof SecondaryDoubleArrayCacheWrapper.SecondaryFirstDoubleArrayCacheWrapper);

        meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.SECONDARY_LAST_DOUBLE);
        meta.setStorageType(StorageType.BUILDIN);
        wrapper = CacheWrapperFactory.createCacheWrapper(meta);
        Assert.assertTrue(wrapper instanceof SecondaryDoubleBuiltinCacheWrapper.SecondaryLastDoubleBuiltinCacheWrapper);

        meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.SECONDARY_LAST_DOUBLE);
        meta.setStorageType(StorageType.BYTES_ARRAY);
        wrapper = CacheWrapperFactory.createCacheWrapper(meta);
        Assert.assertTrue(wrapper instanceof SecondaryDoubleArrayCacheWrapper.SecondaryLastDoubleArrayCacheWrapper);

        meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.SECONDARY_AVG_DOUBLE);
        meta.setStorageType(StorageType.BUILDIN);
        wrapper = CacheWrapperFactory.createCacheWrapper(meta);
        Assert.assertTrue(wrapper instanceof SecondaryDoubleBuiltinCacheWrapper.SecondaryAvgDoubleBuiltinCacheWrapper);

        meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.SECONDARY_AVG_DOUBLE);
        meta.setStorageType(StorageType.BYTES_ARRAY);
        wrapper = CacheWrapperFactory.createCacheWrapper(meta);
        Assert.assertTrue(wrapper instanceof SecondaryDoubleArrayCacheWrapper.SecondaryAvgDoubleArrayCacheWrapper);

        meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.SECONDARY_MAX_DOUBLE);
        meta.setStorageType(StorageType.BUILDIN);
        wrapper = CacheWrapperFactory.createCacheWrapper(meta);
        Assert.assertTrue(wrapper instanceof SecondaryDoubleBuiltinCacheWrapper.SecondaryMaxDoubleBuiltinCacheWrapper);

        meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.SECONDARY_MAX_DOUBLE);
        meta.setStorageType(StorageType.BYTES_ARRAY);
        wrapper = CacheWrapperFactory.createCacheWrapper(meta);
        Assert.assertTrue(wrapper instanceof SecondaryDoubleArrayCacheWrapper.SecondaryMaxDoubleArrayCacheWrapper);

        meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.SECONDARY_MIN_DOUBLE);
        meta.setStorageType(StorageType.BUILDIN);
        wrapper = CacheWrapperFactory.createCacheWrapper(meta);
        Assert.assertTrue(wrapper instanceof SecondaryDoubleBuiltinCacheWrapper.SecondaryMinDoubleBuiltinCacheWrapper);

        meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.SECONDARY_MIN_DOUBLE);
        meta.setStorageType(StorageType.BYTES_ARRAY);
        wrapper = CacheWrapperFactory.createCacheWrapper(meta);
        Assert.assertTrue(wrapper instanceof SecondaryDoubleArrayCacheWrapper.SecondaryMinDoubleArrayCacheWrapper);

        meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.SECONDARY_FIRST_LONG);
        meta.setStorageType(StorageType.BUILDIN);
        wrapper = CacheWrapperFactory.createCacheWrapper(meta);
        Assert.assertTrue(wrapper instanceof SecondaryLongBuiltinCacheWrapper.SecondaryFirstLongBuiltinCacheWrapper);

        meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.SECONDARY_FIRST_LONG);
        meta.setStorageType(StorageType.BYTES_ARRAY);
        wrapper = CacheWrapperFactory.createCacheWrapper(meta);
        Assert.assertTrue(wrapper instanceof SecondaryLongArrayCacheWrapper.SecondaryFirstLongArrayCacheWrapper);

        meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.SECONDARY_LAST_LONG);
        meta.setStorageType(StorageType.BUILDIN);
        wrapper = CacheWrapperFactory.createCacheWrapper(meta);
        Assert.assertTrue(wrapper instanceof SecondaryLongBuiltinCacheWrapper.SecondaryLastLongBuiltinCacheWrapper);

        meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.SECONDARY_LAST_LONG);
        meta.setStorageType(StorageType.BYTES_ARRAY);
        wrapper = CacheWrapperFactory.createCacheWrapper(meta);
        Assert.assertTrue(wrapper instanceof SecondaryLongArrayCacheWrapper.SecondaryLastLongArrayCacheWrapper);

        meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.SECONDARY_MAX_LONG);
        meta.setStorageType(StorageType.BUILDIN);
        wrapper = CacheWrapperFactory.createCacheWrapper(meta);
        Assert.assertTrue(wrapper instanceof SecondaryLongBuiltinCacheWrapper.SecondaryMaxLongBuiltinCacheWrapper);

        meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.SECONDARY_MAX_LONG);
        meta.setStorageType(StorageType.BYTES_ARRAY);
        wrapper = CacheWrapperFactory.createCacheWrapper(meta);
        Assert.assertTrue(wrapper instanceof SecondaryLongArrayCacheWrapper.SecondaryMaxLongArrayCacheWrapper);

        meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.SECONDARY_MIN_LONG);
        meta.setStorageType(StorageType.BUILDIN);
        wrapper = CacheWrapperFactory.createCacheWrapper(meta);
        Assert.assertTrue(wrapper instanceof SecondaryLongBuiltinCacheWrapper.SecondaryMinLongBuiltinCacheWrapper);

        meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.SECONDARY_MIN_LONG);
        meta.setStorageType(StorageType.BYTES_ARRAY);
        wrapper = CacheWrapperFactory.createCacheWrapper(meta);
        Assert.assertTrue(wrapper instanceof SecondaryLongArrayCacheWrapper.SecondaryMinLongArrayCacheWrapper);

    }
}