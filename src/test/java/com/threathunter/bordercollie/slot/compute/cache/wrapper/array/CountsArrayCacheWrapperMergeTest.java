package com.threathunter.bordercollie.slot.compute.cache.wrapper.array;

import com.threathunter.bordercollie.slot.compute.cache.CacheType;
import com.threathunter.bordercollie.slot.compute.cache.StorageType;
import com.threathunter.bordercollie.slot.compute.cache.storage.ByteArrayCacheStore;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapperMeta;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.PrimaryData;
import com.threathunter.bordercollie.slot.util.HashType;
import net.agkn.hll.HLL;
import org.junit.Assert;
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
 * Created by daisy on 17-12-11
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(ByteArrayCacheStore.class)
public class CountsArrayCacheWrapperMergeTest {
    private Map<String, byte[]> data = new HashMap<>();
    private Map<String, HLL> hll = new HashMap<>();
    private ByteArrayCacheStore cacheStore = PowerMockito.mock(ByteArrayCacheStore.class);
    private static final String TEST_KEY = "TEST";
    private static final String TEST_KEY_1 = "TEST_1";

    @Test
    public void testCountCacheWrapperMerge() {
        initialCacheSize(4, TEST_KEY);

        CountsArrayCacheWrapper.CountArrayCacheWrapper wrapper = new CountsArrayCacheWrapper.CountArrayCacheWrapper(getCacheWrapperMeta(CacheType.COUNT));
        wrapper.updateStoreInfo(cacheStore, 0);

        PrimaryData data = wrapper.merge(null, TEST_KEY);
        Assert.assertNull(data);

        wrapper.addData(1, TEST_KEY);
        PrimaryData first = wrapper.merge(data, TEST_KEY);
        Assert.assertEquals(1, first.getResult());

        wrapper.addData(1, TEST_KEY);
        PrimaryData second = wrapper.merge(first, TEST_KEY);
        Assert.assertEquals(3, second.getResult());
    }

    @Test
    public void testDistinctCountCacheWrapperFullMerge() {
        int size = 4 + LimitHashArraySet.totalBytesSizeForMaxCount(20);
        initialCacheSize(size, TEST_KEY);

        CountsArrayCacheWrapper.DistinctCountArrayCacheWrapper wrapper = new CountsArrayCacheWrapper.DistinctCountArrayCacheWrapper(getCacheWrapperMeta(CacheType.DISTINCT_COUNT));
        wrapper.updateStoreInfo(cacheStore, 0);
        PrimaryData merge = wrapper.merge(null, TEST_KEY);
        Assert.assertNull(merge);

        for (int i = 0; i < 50; i++) {
            wrapper.addData(i + "", TEST_KEY);
        }
        Assert.assertEquals(50, wrapper.getData(TEST_KEY).intValue(), 2);
        for (int i = 0; i < 50; i++) {
            wrapper.addData(i + "", TEST_KEY);
        }
        Assert.assertEquals(50, wrapper.getData(TEST_KEY).intValue(), 2);

        PrimaryData first = wrapper.merge(merge, TEST_KEY);
        Assert.assertEquals(50, ((Number) first.getResult()).intValue(), 2);

        for (int i = 50; i < 100; i++) {
            wrapper.addData(i + "", TEST_KEY);
        }
        Assert.assertEquals(100, wrapper.getData(TEST_KEY).intValue(), 5);

        PrimaryData second = wrapper.merge(first, TEST_KEY);
        Assert.assertEquals(100, ((Number) second.getResult()).intValue(), 5);
    }

    @Test
    public void testDistinctCountCacheWrapperNotFullMerge() {
        int size = 4 + LimitHashArraySet.totalBytesSizeForMaxCount(20);
        initialCacheSize(size, TEST_KEY);
        initialCacheSize(size, TEST_KEY_1);

        CountsArrayCacheWrapper.DistinctCountArrayCacheWrapper wrapper = new CountsArrayCacheWrapper.DistinctCountArrayCacheWrapper(getCacheWrapperMeta(CacheType.DISTINCT_COUNT));
        wrapper.updateStoreInfo(cacheStore, 0);
        PrimaryData merge = wrapper.merge(null, TEST_KEY);
        Assert.assertNull(merge);

        for (int i = 0; i < 10; i++) {
            wrapper.addData(i + "", TEST_KEY);
        }
        for (int i = 10; i < 20; i++) {
            wrapper.addData(i + "", TEST_KEY_1);
        }

        PrimaryData first = wrapper.merge(merge, TEST_KEY);
        Assert.assertEquals(10, wrapper.getData(TEST_KEY).intValue());
        Assert.assertEquals(10, wrapper.getData(TEST_KEY_1).intValue());
        Assert.assertEquals(10, ((Number) first.getResult()).intValue());

        for (int i = 20; i < 50; i++) {
            wrapper.addData(i + "", TEST_KEY_1);
        }
        Assert.assertEquals(40, wrapper.getData(TEST_KEY_1).intValue(), 2);
        PrimaryData second = wrapper.merge(first, TEST_KEY_1);
        Assert.assertEquals(50, ((Number) second.getResult()).intValue(), 1);
    }

    private void initialCacheSize(int size, String testKey) {
        PowerMockito.doAnswer((Answer<byte[]>) invocationOnMock -> data.get(testKey)).when(cacheStore).getCache(testKey);
        PowerMockito.doAnswer((Answer<HLL>) invocationOnMock -> hll.get(testKey)).when(cacheStore).getHLL(Mockito.contains(testKey));

        PowerMockito.doAnswer((Answer<byte[]>) invocationOnMock -> {
            data.put(testKey, new byte[size]);
            return data.get(testKey);
        }).when(cacheStore).allocate(testKey);

        PowerMockito.doAnswer((Answer<HLL>) invocationOnMock -> {
            hll.put(testKey, HLLUtil.createHLL());
            return hll.get(testKey);
        }).when(cacheStore).allocateHLL(Mockito.contains(testKey));
    }

    private CacheWrapperMeta getCacheWrapperMeta(CacheType type) {
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setStorageType(StorageType.BYTES_ARRAY);
        meta.setCacheType(type);
        meta.setIndexCount(1);
        meta.setValueHashType(HashType.NORMAL);

        return meta;
    }
}
