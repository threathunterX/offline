package com.threathunter.bordercollie.slot.compute.cache.wrapper.array;

import com.threathunter.bordercollie.slot.compute.cache.CacheType;
import com.threathunter.bordercollie.slot.compute.cache.StorageType;
import com.threathunter.bordercollie.slot.compute.cache.storage.ByteArrayCacheStore;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapperMeta;
import com.threathunter.bordercollie.slot.util.HashType;
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
public class CountsArrayCacheWrapperTest {
    private Map<String, byte[]> data = new HashMap<>();
    private ByteArrayCacheStore cacheStore = PowerMockito.mock(ByteArrayCacheStore.class);
    private static final String TEST_KEY = "TEST";

    @Before
    public void setUp() {
        PowerMockito.doAnswer((Answer<byte[]>) invocationOnMock -> data.get(TEST_KEY)).when(cacheStore).getCache(TEST_KEY);
        PowerMockito.doAnswer((Answer<byte[]>) invocationOnMock -> {
            data.put(TEST_KEY, new byte[200]);
            return data.get(TEST_KEY);
        }).when(cacheStore).allocate(TEST_KEY);
    }

    @Test
    public void testCountArrayCacheWrapper() {
        CountsArrayCacheWrapper.CountArrayCacheWrapper wrapper = new CountsArrayCacheWrapper.CountArrayCacheWrapper(getCountCacheMeta());
        wrapper.updateStoreInfo(cacheStore, 0);

        Assert.assertNull(wrapper.getData(TEST_KEY));

        Assert.assertEquals(1, wrapper.addData("value1", TEST_KEY).intValue());
        Assert.assertEquals(2, wrapper.addData("value1", TEST_KEY).intValue());

        Assert.assertEquals(2, wrapper.getData(TEST_KEY).intValue());

//        wrapper.updateStoreInfo(cacheStore, 2);
//        Assert.assertEquals(0, wrapper.getData(TEST_KEY).intValue());
//
//        wrapper.updateStoreInfo(cacheStore, 0);
//        Assert.assertEquals(2, wrapper.getData(TEST_KEY).intValue());
    }

    @Test
    public void testDistinctCountArrayCacheWrapper() {
        CountsArrayCacheWrapper.DistinctCountArrayCacheWrapper wrapper = new CountsArrayCacheWrapper.DistinctCountArrayCacheWrapper(getDistinctCountCacheMeta());
        wrapper.updateStoreInfo(cacheStore, 4);

        Assert.assertNull(wrapper.getData(TEST_KEY));

        Assert.assertEquals(1, wrapper.addData("1", TEST_KEY).intValue());

        Assert.assertEquals(1, wrapper.addData("1", TEST_KEY).intValue());
        Assert.assertEquals(2, wrapper.addData("2", TEST_KEY).intValue());

        Assert.assertEquals(2, wrapper.getData(TEST_KEY).intValue());
    }

    private CacheWrapperMeta getCountCacheMeta() {
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setStorageType(StorageType.BYTES_ARRAY);
        meta.setCacheType(CacheType.COUNT);
        meta.setIndexCount(1);

        return meta;
    }

    private CacheWrapperMeta getDistinctCountCacheMeta() {
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setStorageType(StorageType.BYTES_ARRAY);
        meta.setCacheType(CacheType.DISTINCT_COUNT);
        meta.setIndexCount(1);
        meta.setValueHashType(HashType.NORMAL);

        return meta;
    }
}
