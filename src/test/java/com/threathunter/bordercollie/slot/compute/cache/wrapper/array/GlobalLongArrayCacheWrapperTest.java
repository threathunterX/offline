package com.threathunter.bordercollie.slot.compute.cache.wrapper.array;

import com.threathunter.bordercollie.slot.compute.cache.CacheType;
import com.threathunter.bordercollie.slot.compute.cache.StorageType;
import com.threathunter.bordercollie.slot.compute.cache.storage.ByteArrayCacheStore;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapperMeta;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.HashMap;
import java.util.Map;

/**
 * 
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(ByteArrayCacheStore.class)
public class GlobalLongArrayCacheWrapperTest {
    private Map<String, byte[]> data = new HashMap<>();
    private final ByteArrayCacheStore cacheStore = PowerMockito.mock(ByteArrayCacheStore.class);

    private static final String GLOBAL_KEY = "__GLOBAL__";

    @Before
    public void setUp() {
        PowerMockito.doAnswer((Answer<byte[]>) invocationOnMock -> data.get(GLOBAL_KEY)).when(cacheStore).getCache(GLOBAL_KEY);

        PowerMockito.doAnswer((Answer<byte[]>) invocationOnMock -> {
            data.put(GLOBAL_KEY, new byte[84]);
            return data.get(GLOBAL_KEY);
        }).when(cacheStore).allocate(GLOBAL_KEY);
    }

    @Test
    public void testSum() {
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setStorageType(StorageType.BYTES_ARRAY);
        meta.setCacheType(CacheType.GLOBAL_SUM_DOUBLE);
        GlobalLongArrayCacheWrapper.GlobalSumLongArrayCacheWrapper wrapper = new GlobalLongArrayCacheWrapper.GlobalSumLongArrayCacheWrapper(meta);
        wrapper.updateStoreInfo(cacheStore, 1);

        Assert.assertNull(wrapper.getData());

        Assert.assertEquals(-100l, wrapper.addData(-100l, null).longValue());
        Assert.assertEquals(0l, wrapper.addData(100l, GLOBAL_KEY).longValue());
    }
}
