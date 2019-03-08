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
 * Created by daisy on 17/5/14.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(BuiltinCacheStore.class)
public class GlobalLongBuiltinCacheWrapperTest {
    private BuiltinCacheStore cacheStore = PowerMockito.mock(BuiltinCacheStore.class);

    @Before
    public void setUpMock() {
        Object[] store = new Object[5];
        PowerMockito.doReturn(store).when(cacheStore).getCache(Mockito.anyString());
    }

    @Test
    public void testSum() {
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setStorageType(StorageType.BUILDIN);
        meta.setCacheType(CacheType.SUM_DOUBLE);

        GlobalLongBuiltinCacheWrapper.GlobalSumLongBuiltinCacheWrapper wrapper = new GlobalLongBuiltinCacheWrapper.GlobalSumLongBuiltinCacheWrapper(meta);
        wrapper.updateStoreInfo(cacheStore, 0);

        Assert.assertEquals(0l, wrapper.getData("").longValue());

        Assert.assertEquals(-10l, wrapper.addData(-10l, "").longValue());
        Assert.assertEquals(-10l, wrapper.getData(null).longValue());
        Assert.assertEquals(0l, wrapper.addData(10l, "").longValue());
    }
}
