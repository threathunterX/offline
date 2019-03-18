package com.threathunter.bordercollie.slot.compute.cache.wrapper.builtin;

import com.threathunter.bordercollie.slot.compute.cache.CacheType;
import com.threathunter.bordercollie.slot.compute.cache.StorageType;
import com.threathunter.bordercollie.slot.compute.cache.storage.BuiltinCacheStore;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapperMeta;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * 
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(BuiltinCacheStore.class)
public class LongBuiltinCacheWrapperTest {
    private BuiltinCacheStore cacheStore = PowerMockito.mock(BuiltinCacheStore.class);

    @Before
    public void setUp() {
        Object[] store = new Object[5];
        PowerMockito.doReturn(store).when(cacheStore).getCache(Mockito.anyString());
    }

    @Test
    public void testSumLongCacheWrapper() {
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.SUM_LONG);
        meta.setStorageType(StorageType.BUILDIN);

        LongBuiltinCacheWrapper.SumLongBuiltinCacheWrapper wrapper = new LongBuiltinCacheWrapper.SumLongBuiltinCacheWrapper(meta);
        wrapper.updateStoreInfo(cacheStore, 0);

        Assert.assertNull(wrapper.getData("null_key"));
        Assert.assertNull(wrapper.getData(null));

        Assert.assertEquals(-100L, wrapper.addData(-100L, "0_key").longValue());
        Assert.assertEquals(-100l, wrapper.getData("0_key").longValue());
        Assert.assertEquals(0L, wrapper.addData(100l, "0_key").longValue());
    }

    @Test
    public void testFirstLongCacheWrapper() {
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.FIRST_LONG);
        meta.setStorageType(StorageType.BUILDIN);

        LongBuiltinCacheWrapper.FirstLongBuiltinCacheWrapper wrapper = new LongBuiltinCacheWrapper.FirstLongBuiltinCacheWrapper(meta);
        wrapper.updateStoreInfo(cacheStore, 0);

        Assert.assertNull(wrapper.getData("null_key"));
        Assert.assertNull(wrapper.getData(null));

        Assert.assertEquals(0L, wrapper.addData(0L, "0_key"), 0L);
        // return null if already add the first value
        Assert.assertNull(wrapper.addData(1L, "0_key"));
        Assert.assertEquals(0L, wrapper.getData("0_key"), 0L);
    }

    @Test
    public void testLastLongCacheWrapper() {
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.LAST_LONG);
        meta.setStorageType(StorageType.BUILDIN);

        LongBuiltinCacheWrapper.LastLongBuiltinCacheWrapper wrapper = new LongBuiltinCacheWrapper.LastLongBuiltinCacheWrapper(meta);
        wrapper.updateStoreInfo(cacheStore, 0);

        Assert.assertNull(wrapper.getData("null_key"));
        Assert.assertNull(wrapper.getData(null));

        Assert.assertEquals(0L, wrapper.addData(0L, "0_key").longValue(), 0L);


        Assert.assertEquals(1L, wrapper.addData(1L, "0_key").longValue(), 1L);
    }

    @Test
    public void testAvgLongCacheWrapper() {
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setStorageType(StorageType.BUILDIN);
        meta.setCacheType(CacheType.AVG_DOUBLE);

        LongBuiltinCacheWrapper.AvgLongBuiltinCacheWrapper wrapper = new LongBuiltinCacheWrapper.AvgLongBuiltinCacheWrapper(meta);
        wrapper.updateStoreInfo(cacheStore, 0);

        Assert.assertNull(wrapper.getData("null_key"));
        Assert.assertNull(wrapper.getData(null));

        Assert.assertEquals(3, wrapper.addData(3l, "0_key").intValue());


        Assert.assertEquals(4, wrapper.addData(5l, "0_key").intValue());
    }

    @Test
    public void testMaxLongCacheWrapper() {
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.MAX_LONG);
        meta.setStorageType(StorageType.BUILDIN);

        LongBuiltinCacheWrapper.MaxLongBuiltinCacheWrapper wrapper = new LongBuiltinCacheWrapper.MaxLongBuiltinCacheWrapper(meta);
        wrapper.updateStoreInfo(cacheStore, 0);

        Assert.assertNull(wrapper.getData("null_key"));
        Assert.assertNull(wrapper.getData(null));

        Assert.assertEquals(0L, wrapper.addData(0L, "0_key").longValue(), 0L);


        Assert.assertEquals(2L, wrapper.addData(2L, "0_key").longValue(), 2L);
        Assert.assertNull(wrapper.addData(1L, "0_key"));
    }

    @Test
    public void testMinLongCacheWrapper() {
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.MIN_LONG);
        meta.setStorageType(StorageType.BUILDIN);

        LongBuiltinCacheWrapper.MinLongBuiltinCacheWrapper wrapper = new LongBuiltinCacheWrapper.MinLongBuiltinCacheWrapper(meta);
        wrapper.updateStoreInfo(cacheStore, 0);

        Assert.assertNull(wrapper.getData("null_key"));
        Assert.assertNull(wrapper.getData(null));

        Assert.assertEquals(0L, wrapper.addData(0L, "0_key").longValue(), 0L);

        Assert.assertNull(wrapper.addData(1L, "0_key"));
        Assert.assertNull(wrapper.addData(2L, "0_key"));
    }

}