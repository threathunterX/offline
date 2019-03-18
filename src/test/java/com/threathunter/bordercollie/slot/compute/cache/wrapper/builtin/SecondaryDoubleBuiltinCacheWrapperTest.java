package com.threathunter.bordercollie.slot.compute.cache.wrapper.builtin;

import com.threathunter.bordercollie.slot.compute.cache.CacheType;
import com.threathunter.bordercollie.slot.compute.cache.StorageType;
import com.threathunter.bordercollie.slot.compute.cache.storage.BuiltinCacheStore;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapperMeta;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Map;

/**
 * 
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(BuiltinCacheStore.class)
public class SecondaryDoubleBuiltinCacheWrapperTest {
    private BuiltinCacheStore cacheStore = PowerMockito.mock(BuiltinCacheStore.class);

    private String dataKey = "key";
    private String nullKey = "key_null";

    @Before
    public void setUp() {
        Object[] cacheData = new Object[5];
        Object[] cacheNull = new Object[5];
        PowerMockito.doReturn(cacheData).when(cacheStore).getCache(dataKey);
        PowerMockito.doReturn(cacheNull).when(cacheStore).getCache(nullKey);
    }

    @Test
    public void testSum() {
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setStorageType(StorageType.BUILDIN);
        meta.setCacheType(CacheType.SECONDARY_SUM_DOUBLE);

        SecondaryDoubleBuiltinCacheWrapper.SecondarySumDoubleBuiltinCacheWrapper wrapper =
                new SecondaryDoubleBuiltinCacheWrapper.SecondarySumDoubleBuiltinCacheWrapper(meta);

        wrapper.updateStoreInfo(cacheStore, 0);
        Assert.assertEquals(1.0, wrapper.addData(1.0, dataKey, "key1").doubleValue(), 0.0);
        Assert.assertEquals(3.0, wrapper.addData(2.0, dataKey, "key1").doubleValue(), 0.0);

        Assert.assertEquals(3.0, wrapper.getData(dataKey, "key1").doubleValue(), 0.0);

        wrapper.addData(2.0, dataKey, "key2");

        Assert.assertEquals(2, ((Map) wrapper.readAll(dataKey)).size());
        Assert.assertNull(wrapper.readAll(nullKey));
    }

    @Test
    public void testFirst() {
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setStorageType(StorageType.BUILDIN);
        meta.setCacheType(CacheType.SECONDARY_FIRST_DOUBLE);

        SecondaryDoubleBuiltinCacheWrapper.SecondaryFirstDoubleBuiltinCacheWrapper wrapper =
                new SecondaryDoubleBuiltinCacheWrapper.SecondaryFirstDoubleBuiltinCacheWrapper(meta);

        wrapper.updateStoreInfo(cacheStore, 0);
        Assert.assertEquals(1.0, wrapper.addData(1.0, dataKey, "key1").doubleValue(), 0.0);
        Assert.assertNull(wrapper.addData(2.0, dataKey, "key1"));

        Assert.assertEquals(1.0, wrapper.getData(dataKey, "key1").doubleValue(), 0.0);

        wrapper.addData(2.0, dataKey, "key2");

        Assert.assertEquals(2, ((Map) wrapper.readAll(dataKey)).size());
        Assert.assertNull(wrapper.readAll(nullKey));
    }

    @Test
    public void testLast() {
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setStorageType(StorageType.BUILDIN);
        meta.setCacheType(CacheType.SECONDARY_LAST_DOUBLE);

        SecondaryDoubleBuiltinCacheWrapper.SecondaryLastDoubleBuiltinCacheWrapper wrapper =
                new SecondaryDoubleBuiltinCacheWrapper.SecondaryLastDoubleBuiltinCacheWrapper(meta);

        wrapper.updateStoreInfo(cacheStore, 0);
        Assert.assertEquals(1.0, wrapper.addData(1.0, dataKey, "key1").doubleValue(), 0.0);
        Assert.assertEquals(2.0, wrapper.addData(2.0, dataKey, "key1").doubleValue(), 0.0);

        Assert.assertEquals(2.0, wrapper.getData(dataKey, "key1").doubleValue(), 0.0);

        wrapper.addData(2.0, dataKey, "key2");

        Assert.assertEquals(2, ((Map) wrapper.readAll(dataKey)).size());
        Assert.assertNull(wrapper.readAll(nullKey));
    }

    @Test
    public void testAvg() {
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setStorageType(StorageType.BUILDIN);
        meta.setCacheType(CacheType.SECONDARY_AVG_DOUBLE);

        SecondaryDoubleBuiltinCacheWrapper.SecondaryAvgDoubleBuiltinCacheWrapper wrapper =
                new SecondaryDoubleBuiltinCacheWrapper.SecondaryAvgDoubleBuiltinCacheWrapper(meta);

        wrapper.updateStoreInfo(cacheStore, 1);
        Assert.assertEquals(1.0, wrapper.addData(1.0, dataKey, "key1").doubleValue(), 0.0);
        Assert.assertEquals(0.5, wrapper.addData(0.0, dataKey, "key1").doubleValue(), 0.0);
        Assert.assertEquals(0.5, wrapper.getData(dataKey, "key1").doubleValue(), 0.0);

        wrapper.addData(2.0, dataKey, "key2");

        Assert.assertEquals(2, ((Map) wrapper.readAll(dataKey)).size());
        Assert.assertNull(wrapper.readAll(nullKey));
    }

    @Test
    public void testMax() {
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setStorageType(StorageType.BUILDIN);
        meta.setCacheType(CacheType.SECONDARY_MAX_DOUBLE);

        SecondaryDoubleBuiltinCacheWrapper.SecondaryMaxDoubleBuiltinCacheWrapper wrapper =
                new SecondaryDoubleBuiltinCacheWrapper.SecondaryMaxDoubleBuiltinCacheWrapper(meta);

        wrapper.updateStoreInfo(cacheStore, 0);
        Assert.assertEquals(2.0, wrapper.addData(2.0, dataKey, "key1").doubleValue(), 0.0);
        Assert.assertEquals(3.0, wrapper.addData(3.0, dataKey, "key1").doubleValue(), 0.0);
        Assert.assertNull(wrapper.addData(1.0, dataKey, "key1"));

        Assert.assertEquals(3.0, wrapper.getData(dataKey, "key1").doubleValue(), 0.0);

        Assert.assertEquals(1, ((Map) wrapper.readAll(dataKey)).size());
        Assert.assertNull(wrapper.readAll(nullKey));
    }

    @Test
    public void testMin() {
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setStorageType(StorageType.BUILDIN);
        meta.setCacheType(CacheType.SECONDARY_MIN_DOUBLE);

        SecondaryDoubleBuiltinCacheWrapper.SecondaryMinDoubleBuiltinCacheWrapper wrapper =
                new SecondaryDoubleBuiltinCacheWrapper.SecondaryMinDoubleBuiltinCacheWrapper(meta);

        wrapper.updateStoreInfo(cacheStore, 0);
        Assert.assertEquals(3.0, wrapper.addData(3.0, dataKey, "key1").doubleValue(), 0.0);
        Assert.assertEquals(1.0, wrapper.addData(1.0, dataKey, "key1").doubleValue(), 0.0);
        Assert.assertNull(wrapper.addData(2.0, dataKey, "key1"));

        Assert.assertEquals(1.0, wrapper.getData(dataKey, "key1").doubleValue(), 0.0);

        Assert.assertEquals(1, ((Map) wrapper.readAll(dataKey)).size());
        Assert.assertNull(wrapper.readAll(nullKey));
    }
}
