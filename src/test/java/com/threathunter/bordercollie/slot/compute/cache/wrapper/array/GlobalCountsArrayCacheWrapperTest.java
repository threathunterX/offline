package com.threathunter.bordercollie.slot.compute.cache.wrapper.array;

import com.threathunter.bordercollie.slot.compute.cache.CacheType;
import com.threathunter.bordercollie.slot.compute.cache.StorageType;
import com.threathunter.bordercollie.slot.compute.cache.storage.ByteArrayCacheStore;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapperMeta;
import com.threathunter.bordercollie.slot.util.HashType;
import net.agkn.hll.HLL;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
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
public class GlobalCountsArrayCacheWrapperTest {
    private Map<String, byte[]> data = new HashMap<>();
    private Map<String, HLL> hllMap = new HashMap<>();
    private final ByteArrayCacheStore cacheStore = PowerMockito.mock(ByteArrayCacheStore.class);

    private static final String GLOBAL_KEY = "__GLOBAL__";

    @Before
    public void setUp() {
        // Do not use this, because null is the value to return when compiling.
//        PowerMockito.doReturn(data.get(GLOBAL_KEY)).when(cacheStore).getCache(GLOBAL_KEY);
        PowerMockito.doAnswer((Answer<byte[]>) invocationOnMock -> data.get(GLOBAL_KEY)).when(cacheStore).getCache(GLOBAL_KEY);

        PowerMockito.doAnswer((Answer<byte[]>) invocationOnMock -> {
            data.put(GLOBAL_KEY, new byte[168]);
            return data.get(GLOBAL_KEY);
        }).when(cacheStore).allocate(GLOBAL_KEY);

        PowerMockito.doAnswer((Answer<HLL>) invocationOnMock -> hllMap.get(invocationOnMock.getArguments()[0])).when(cacheStore).getHLL(Mockito.anyString());

        PowerMockito.doAnswer((Answer<HLL>) invocationOnMock -> {
            hllMap.put((String) invocationOnMock.getArguments()[0], new HLL(13, 5));
            return hllMap.get(invocationOnMock.getArguments()[0]);
        }).when(cacheStore).allocateHLL(Mockito.anyString());
    }

    @Test
    public void testGlobalCountWrapper() {
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.GLOBAL_COUNT);
        meta.setStorageType(StorageType.BYTES_ARRAY);
        meta.setValueHashType(HashType.NORMAL);
        GlobalCountsArrayCacheWrapper.GlobalCountArrayCacheWrapper wrapper = new GlobalCountsArrayCacheWrapper.GlobalCountArrayCacheWrapper(meta);
        wrapper.updateStoreInfo(cacheStore, 0);

        Assert.assertEquals(1, wrapper.addData("1", GLOBAL_KEY).intValue());
        Assert.assertEquals(2, wrapper.addData("1", GLOBAL_KEY).intValue());
        Assert.assertEquals(2, wrapper.getData(GLOBAL_KEY).intValue());
    }

    @Test
    public void testGlobalDistinctCountWrapper() {
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setCacheType(CacheType.GLOBAL_DISTINCT_COUNT);
        meta.setStorageType(StorageType.BYTES_ARRAY);
        meta.setValueHashType(HashType.NORMAL);

        GlobalCountsArrayCacheWrapper.GlobalDistinctCountArrayCacheWrapper wrapper = new GlobalCountsArrayCacheWrapper.GlobalDistinctCountArrayCacheWrapper(meta);
        wrapper.updateStoreInfo(cacheStore, 0);

        Assert.assertNull(wrapper.getData(GLOBAL_KEY));
        Assert.assertEquals(1, wrapper.addData("1", GLOBAL_KEY).intValue());
        Assert.assertEquals(1, wrapper.addData("1", GLOBAL_KEY).intValue());
        Assert.assertEquals(2, wrapper.addData("0", GLOBAL_KEY).intValue());
        Assert.assertEquals(2, wrapper.getData(GLOBAL_KEY).intValue());
        Assert.assertEquals(3, wrapper.addData("-987", GLOBAL_KEY).intValue());

        for (int i = 0; i < 30; i++) {
            wrapper.addData("" + i, GLOBAL_KEY);
        }
        Assert.assertEquals(31, wrapper.getData(GLOBAL_KEY).intValue());
    }
}
