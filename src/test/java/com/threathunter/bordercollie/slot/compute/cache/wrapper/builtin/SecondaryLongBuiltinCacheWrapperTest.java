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
public class SecondaryLongBuiltinCacheWrapperTest {
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
        meta.setCacheType(CacheType.SECONDARY_SUM_LONG);

        SecondaryLongBuiltinCacheWrapper.SecondarySumLongBuiltinCacheWrapper wrapper =
                new SecondaryLongBuiltinCacheWrapper.SecondarySumLongBuiltinCacheWrapper(meta);

        wrapper.updateStoreInfo(cacheStore, 0);
        Assert.assertEquals(1L, wrapper.addData(1L, dataKey, "key1").longValue());
        Assert.assertEquals(3L, wrapper.addData(2L, dataKey, "key1").longValue());

        Assert.assertEquals(3L, wrapper.getData(dataKey, "key1").longValue());

        wrapper.addData(2L, dataKey, "key2");

        Assert.assertEquals(2, ((Map) wrapper.readAll(dataKey)).size());
        Assert.assertNull(wrapper.readAll(nullKey));
    }

    @Test
    public void testFirst() {
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setStorageType(StorageType.BUILDIN);
        meta.setCacheType(CacheType.SECONDARY_FIRST_LONG);

        SecondaryLongBuiltinCacheWrapper.SecondaryFirstLongBuiltinCacheWrapper wrapper =
                new SecondaryLongBuiltinCacheWrapper.SecondaryFirstLongBuiltinCacheWrapper(meta);

        wrapper.updateStoreInfo(cacheStore, 0);
        Assert.assertEquals(1L, wrapper.addData(1L, dataKey, "key1"), 0.0);
        Assert.assertNull(wrapper.addData(2L, dataKey, "key1"));

        Assert.assertEquals(1L, wrapper.getData(dataKey, "key1"), 0.0);

        wrapper.addData(2L, dataKey, "key2");

        Assert.assertEquals(2, ((Map) wrapper.readAll(dataKey)).size());
        Assert.assertNull(wrapper.readAll(nullKey));
    }

    @Test
    public void testLast() {
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setStorageType(StorageType.BUILDIN);
        meta.setCacheType(CacheType.SECONDARY_LAST_LONG);

        SecondaryLongBuiltinCacheWrapper.SecondaryLastLongBuiltinCacheWrapper wrapper =
                new SecondaryLongBuiltinCacheWrapper.SecondaryLastLongBuiltinCacheWrapper(meta);

        wrapper.updateStoreInfo(cacheStore, 0);
        Assert.assertEquals(1L, wrapper.addData(1L, dataKey, "key1"), 0.0);
        Assert.assertEquals(2L, wrapper.addData(2L, dataKey, "key1"), 0.0);

        Assert.assertEquals(2L, wrapper.getData(dataKey, "key1"), 0.0);

        wrapper.addData(2L, dataKey, "key2");

        Assert.assertEquals(2, ((Map) wrapper.readAll(dataKey)).size());
        Assert.assertNull(wrapper.readAll(nullKey));
    }

    @Test
    public void testMax() {
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setStorageType(StorageType.BUILDIN);
        meta.setCacheType(CacheType.SECONDARY_MAX_LONG);

        SecondaryLongBuiltinCacheWrapper.SecondaryMaxLongBuiltinCacheWrapper wrapper =
                new SecondaryLongBuiltinCacheWrapper.SecondaryMaxLongBuiltinCacheWrapper(meta);

        wrapper.updateStoreInfo(cacheStore, 0);
        Assert.assertEquals(2L, wrapper.addData(2L, dataKey, "key1"), 0.0);
        Assert.assertEquals(3L, wrapper.addData(3L, dataKey, "key1"), 0.0);
        Assert.assertNull(wrapper.addData(1L, dataKey, "key1"));

        Assert.assertEquals(3L, wrapper.getData(dataKey, "key1"), 0.0);

        Assert.assertEquals(1, ((Map) wrapper.readAll(dataKey)).size());
        Assert.assertNull(wrapper.readAll(nullKey));
    }

    @Test
    public void testMin() {
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setStorageType(StorageType.BUILDIN);
        meta.setCacheType(CacheType.SECONDARY_MIN_LONG);

        SecondaryLongBuiltinCacheWrapper.SecondaryMinLongBuiltinCacheWrapper wrapper =
                new SecondaryLongBuiltinCacheWrapper.SecondaryMinLongBuiltinCacheWrapper(meta);

        wrapper.updateStoreInfo(cacheStore, 0);
        Assert.assertEquals(3L, wrapper.addData(3L, dataKey, "key1"), 0.0);
        Assert.assertEquals(1L, wrapper.addData(1L, dataKey, "key1"), 0.0);
        Assert.assertNull(wrapper.addData(2L, dataKey, "key1"));

        Assert.assertEquals(1L, wrapper.getData(dataKey, "key1"), 0.0);

        Assert.assertEquals(1, ((Map) wrapper.readAll(dataKey)).size());
        Assert.assertNull(wrapper.readAll(nullKey));
    }
}