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
 * Created by toyld on 3/22/17.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(BuiltinCacheStore.class)
public class SecondaryCountsBuiltinCacheWrapperTest {
    private BuiltinCacheStore cacheStore = PowerMockito.mock(BuiltinCacheStore.class);

    @Before
    public void setUpMock() {
        Object[] store = new Object[5];
        PowerMockito.doReturn(store).when(cacheStore).getCache(Mockito.anyString());
    }

    @Test
    public void testSecondaryCountBuiltinCacheWrapper() {
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setStorageType(StorageType.BUILDIN);
        meta.setCacheType(CacheType.SECONDARY_COUNT);

        SecondaryCountsBuiltinCacheWrapper.SecondaryCountBuiltinCacheWrapper wrapper = new SecondaryCountsBuiltinCacheWrapper.SecondaryCountBuiltinCacheWrapper(meta);
        wrapper.updateStoreInfo(cacheStore, 0);

        Assert.assertEquals(0, wrapper.getData("0key", "1key").intValue());

        Assert.assertEquals(1, wrapper.addData("1", "0key", "1key").intValue());
        Assert.assertEquals(2, wrapper.addData("2", "0key", "1key").intValue());

        Assert.assertEquals(2, wrapper.getData("0key", "1key").intValue());

    }

    @Test
    public void SecondaryDistinctCountBuiltinCacheWrapper() {
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setStorageType(StorageType.BUILDIN);
        meta.setCacheType(CacheType.SECONDARY_DISTINCT_COUNT);

        SecondaryCountsBuiltinCacheWrapper.SecondaryDistinctCountBuiltinCacheWrapper wrapper = new SecondaryCountsBuiltinCacheWrapper.SecondaryDistinctCountBuiltinCacheWrapper(meta);
        wrapper.updateStoreInfo(cacheStore, 1);

        Assert.assertEquals(0, wrapper.getData("0key", "1key").intValue());

        Assert.assertEquals(1, wrapper.addData("1", "0key", "1key").intValue());

        Assert.assertEquals(1, wrapper.addData("1", "0key", "1key").intValue());
        Assert.assertEquals(2, wrapper.addData("2", "0key", "1key").intValue());

        Assert.assertEquals(2, wrapper.getData("0key", "1key").intValue());

    }

}