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
public class GlobalDoubleBuiltinCacheWrapperTest {
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

        GlobalDoubleBuiltinCacheWrapper.GlobalSumDoubleBuiltinCacheWrapper wrapper = new GlobalDoubleBuiltinCacheWrapper.GlobalSumDoubleBuiltinCacheWrapper(meta);
        wrapper.updateStoreInfo(cacheStore, 0);

        Assert.assertEquals(0.0, wrapper.getData("").doubleValue(), 0.0);

        Assert.assertEquals(-10.0, wrapper.addData(-10.0, "").doubleValue(), 0.0);
        Assert.assertEquals(-10.0, wrapper.getData(null).doubleValue(), 0.0);
        Assert.assertEquals(0.0, wrapper.addData(10.0, "").doubleValue(), 0.0);
    }
}
