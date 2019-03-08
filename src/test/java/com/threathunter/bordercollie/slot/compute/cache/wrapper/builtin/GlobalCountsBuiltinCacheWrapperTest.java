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
 * Created by daisy on 17/3/16.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(BuiltinCacheStore.class)
public class GlobalCountsBuiltinCacheWrapperTest {
    private BuiltinCacheStore cacheStore = PowerMockito.mock(BuiltinCacheStore.class);

    @Before
    public void setUpMock() {
        Object[] store = new Object[5];
        PowerMockito.doReturn(store).when(cacheStore).getCache(Mockito.anyString());
    }

    @Test
    public void testGlobalCountCacheWrapper() {
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.GLOBAL_COUNT);
        meta.setStorageType(StorageType.BUILDIN);

        GlobalCountsBuiltinCacheWrapper.GlobalCountBuiltinCacheWrapper wrapper =
                new GlobalCountsBuiltinCacheWrapper.GlobalCountBuiltinCacheWrapper(meta);
        wrapper.updateStoreInfo(cacheStore, 0);

        Assert.assertEquals(0, wrapper.getData("key").intValue());

        Assert.assertEquals(1, wrapper.addData(null, "key").intValue());
        Assert.assertEquals(2, wrapper.addData(null, "key").intValue());

        Assert.assertEquals(2, wrapper.getData("key").intValue());

        wrapper.updateStoreInfo(cacheStore, 2);
        Assert.assertEquals(0, wrapper.getData("key").intValue());

        wrapper.updateStoreInfo(cacheStore, 0);
        Assert.assertEquals(2, wrapper.getData("key").intValue());
    }

    @Test
    public void testGlobalDistinctCountCacheWrapper() {
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.GLOBAL_DISTINCT_COUNT);
        meta.setStorageType(StorageType.BUILDIN);

        GlobalCountsBuiltinCacheWrapper.GlobalDistinctCountBuiltinCacheWrapper wrapper =
                new GlobalCountsBuiltinCacheWrapper.GlobalDistinctCountBuiltinCacheWrapper(meta);
        wrapper.updateStoreInfo(cacheStore, 1);

        Assert.assertEquals(0, wrapper.getData("key").intValue());

        Assert.assertEquals(1, wrapper.addData("1", "key").intValue());

        Assert.assertEquals(1, wrapper.addData("1", "key").intValue());
        Assert.assertEquals(2, wrapper.addData("2", "key").intValue());

        Assert.assertEquals(2, wrapper.getData("key").intValue());
    }
}
