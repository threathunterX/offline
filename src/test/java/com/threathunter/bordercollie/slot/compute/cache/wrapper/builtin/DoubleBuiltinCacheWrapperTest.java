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
public class DoubleBuiltinCacheWrapperTest {
    private BuiltinCacheStore cacheStore = PowerMockito.mock(BuiltinCacheStore.class);

    @Before
    public void setUp() {
        Object[] store = new Object[5];
        PowerMockito.doReturn(store).when(cacheStore).getCache(Mockito.anyString());
    }

    @Test
    public void testSumDoubleCacheWrapper() {
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setStorageType(StorageType.BUILDIN);
        meta.setCacheType(CacheType.SUM_DOUBLE);

        DoubleBuiltinCacheWrapper.SumDoubleBuiltinCacheWrapper wrapper = new DoubleBuiltinCacheWrapper.SumDoubleBuiltinCacheWrapper(meta);
        wrapper.updateStoreInfo(cacheStore, 0);

        Assert.assertNull(wrapper.getData("null_key"));
        Assert.assertNull(wrapper.getData(null));

        Assert.assertEquals(-10.0, wrapper.addData(-10.0, "0_key").doubleValue(), 0.0);
        Assert.assertEquals(-10.0, wrapper.getData("0_key").doubleValue(), 0.0);
        Assert.assertEquals(0.0, wrapper.addData(10.0, "0_key").doubleValue(), 0.0);
    }

    @Test
    public void testFirstDoubleCacheWrapper() {
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setStorageType(StorageType.BUILDIN);
        meta.setCacheType(CacheType.FIRST_DOUBLE);

        DoubleBuiltinCacheWrapper.FirstDoubleBuiltinCacheWrapper wrapper = new DoubleBuiltinCacheWrapper.FirstDoubleBuiltinCacheWrapper(meta);
        wrapper.updateStoreInfo(cacheStore, 0);

        Assert.assertNull(wrapper.getData("null_key"));
        Assert.assertNull(wrapper.getData(null));

        Assert.assertEquals(0.0, wrapper.addData(0.0, "0_key").doubleValue(), 0.0);
        // return null if already add the first value
        Assert.assertNull(wrapper.addData(1.0, "0_key"));
        Assert.assertEquals(0.0, wrapper.getData("0_key").doubleValue(), 0.0);
    }

    @Test
    public void testLastDoubleCacheWrapper() {
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setStorageType(StorageType.BUILDIN);
        meta.setCacheType(CacheType.LAST_DOUBLE);

        DoubleBuiltinCacheWrapper.LastDoubleBuiltinCacheWrapper wrapper = new DoubleBuiltinCacheWrapper.LastDoubleBuiltinCacheWrapper(meta);
        wrapper.updateStoreInfo(cacheStore, 0);

        Assert.assertNull(wrapper.getData("null_key"));
        Assert.assertNull(wrapper.getData(null));

        Assert.assertEquals(0.0, wrapper.addData(0.0, "0_key").doubleValue(), 0.0);


        Assert.assertEquals(1.0, wrapper.addData(1.0, "0_key").doubleValue(), 0.0);
    }

    @Test
    public void testAvgDoubleCacheWrapper() {
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setStorageType(StorageType.BUILDIN);
        meta.setCacheType(CacheType.AVG_DOUBLE);

        DoubleBuiltinCacheWrapper.AvgDoubleBuiltinCacheWrapper wrapper = new DoubleBuiltinCacheWrapper.AvgDoubleBuiltinCacheWrapper(meta);
        wrapper.updateStoreInfo(cacheStore, 0);

        Assert.assertNull(wrapper.getData("null_key"));
        Assert.assertNull(wrapper.getData(null));

        Assert.assertEquals(3.0, wrapper.addData(3.0, "0_key").doubleValue(), 0.0);
        Assert.assertEquals(4.0, wrapper.addData(5.0, "0_key").doubleValue(), 0.0);
    }

    @Test
    public void testMaxDoubleCacheWrapper() {
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setStorageType(StorageType.BUILDIN);
        meta.setCacheType(CacheType.MAX_DOUBLE);

        DoubleBuiltinCacheWrapper.MaxDoubleBuiltinCacheWrapper wrapper = new DoubleBuiltinCacheWrapper.MaxDoubleBuiltinCacheWrapper(meta);
        wrapper.updateStoreInfo(cacheStore, 0);

        Assert.assertNull(wrapper.getData("null_key"));
        Assert.assertNull(wrapper.getData(null));

        Assert.assertEquals(0.0, wrapper.addData(0.0, "0_key").doubleValue(), 0.0);


        Assert.assertEquals(2.0, wrapper.addData(2.0, "0_key").doubleValue(), 0.0);
        Assert.assertNull(wrapper.addData(1.0, "0_key"));
    }

    @Test
    public void testMinDoubleCacheWrapper() {
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setStorageType(StorageType.BUILDIN);
        meta.setCacheType(CacheType.MIN_DOUBLE);

        DoubleBuiltinCacheWrapper.MinDoubleBuiltinCacheWrapper wrapper = new DoubleBuiltinCacheWrapper.MinDoubleBuiltinCacheWrapper(meta);
        wrapper.updateStoreInfo(cacheStore, 0);

        Assert.assertNull(wrapper.getData("null_key"));
        Assert.assertNull(wrapper.getData(null));

        Assert.assertEquals(0.0, wrapper.addData(0.0, "0_key").doubleValue(), 0.0);

        Assert.assertNull(wrapper.addData(1.0, "0_key"));
        Assert.assertNull(wrapper.addData(2.0, "0_key"));
    }
}
