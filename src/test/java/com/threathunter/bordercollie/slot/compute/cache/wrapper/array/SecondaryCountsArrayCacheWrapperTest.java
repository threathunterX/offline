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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * 
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(ByteArrayCacheStore.class)
public class SecondaryCountsArrayCacheWrapperTest {
    private static final String KEY_1 = "key1";
    private static final String KEY_2 = "key2";

    private static final String SUB_KEY_1 = "subkey1";
    private static final String SUB_KEY_2 = "subkey2";
    private static final String SUB_KEY_3 = "subkey3";

    private ByteArrayCacheStore cacheStore = PowerMockito.mock(ByteArrayCacheStore.class);
    private Map<String, byte[]> data = new HashMap<>();
    private Map<String, HLL> hllMap = new HashMap<>();

    @Before
    public void setUp() {
        PowerMockito.doAnswer((Answer<byte[]>) mockArg -> data.get(KEY_1)).when(cacheStore).getCache(KEY_1);
        PowerMockito.doAnswer((Answer<byte[]>) mockArg -> data.get(KEY_2)).when(cacheStore).getCache(KEY_2);

        PowerMockito.doAnswer((Answer<byte[]>) mockArg -> {
            data.put(KEY_1, new byte[2000]);
            return data.get(KEY_1);
        }).when(cacheStore).allocate(KEY_1);
        PowerMockito.doAnswer((Answer<byte[]>) mockArg -> {
            data.put(KEY_2, new byte[2000]);
            return data.get(KEY_2);
        }).when(cacheStore).allocate(KEY_2);

        PowerMockito.doAnswer((Answer<HLL>) mockArg -> hllMap.get(mockArg.getArguments()[0])).when(cacheStore).getHLL(Mockito.anyString());
        PowerMockito.doAnswer((Answer<HLL>) invocationOnMock -> {
            hllMap.put((String) invocationOnMock.getArguments()[0], new HLL(13, 5));
            return hllMap.get(invocationOnMock.getArguments()[0]);
        }).when(cacheStore).allocateHLL(Mockito.anyString());
    }

    @Test
    public void testSecondaryCountArrayCache() {
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setStorageType(StorageType.BYTES_ARRAY);
        meta.setCacheType(CacheType.SECONDARY_COUNT);
        meta.setSecondaryKeyHashType(HashType.NORMAL);

        SecondaryCountsArrayCacheWrapper.SecondaryCountArrayCacheWrapper wrapper = new SecondaryCountsArrayCacheWrapper.SecondaryCountArrayCacheWrapper(meta);
        wrapper.updateStoreInfo(cacheStore, 21);
        System.out.println(wrapper.getCacheSize());

        Assert.assertEquals(1, wrapper.addData(null, KEY_1, SUB_KEY_1).intValue());
        Assert.assertEquals(2, wrapper.addData(null, KEY_1, SUB_KEY_1).intValue());
        Assert.assertNull(wrapper.getData(KEY_1, SUB_KEY_2));
        Assert.assertNull(wrapper.getData(KEY_2, SUB_KEY_2));

        List<String> inserted = new LinkedList<>();
        for (int i = 0; i < 30; i++) {
            String key = String.format("%s_%d", SUB_KEY_2, i);
            if (wrapper.addData("" + i, KEY_1, key) != null) {
                inserted.add(key);
            }
        }
        System.out.println(inserted);
        Assert.assertEquals(2, wrapper.getData(KEY_1, SUB_KEY_1).intValue());
        for (int i = 0; i < 30; i++) {
            String key = String.format("%s_%d", SUB_KEY_2, i);
            if (inserted.contains(key)) {
                Assert.assertEquals(1, wrapper.getData(KEY_1, key).intValue());
            } else {
                Assert.assertNull(wrapper.getData(KEY_1, SUB_KEY_2 + i));
            }
        }

        Assert.assertEquals(18, inserted.size());
    }

    @Test
    public void testSecondaryDistinctCountArrayCache() {
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setStorageType(StorageType.BYTES_ARRAY);
        meta.setCacheType(CacheType.SECONDARY_DISTINCT_COUNT);
        meta.setSecondaryKeyHashType(HashType.NORMAL);
        meta.setValueHashType(HashType.NORMAL);

        SecondaryCountsArrayCacheWrapper.SecondaryDistinctCountArrayCacheWrapper wrapper =
                new SecondaryCountsArrayCacheWrapper.SecondaryDistinctCountArrayCacheWrapper(meta);
        wrapper.updateStoreInfo(cacheStore, 0);

        Assert.assertEquals(1, wrapper.addData("geo1", KEY_1, SUB_KEY_1).intValue());
        Assert.assertEquals(1, wrapper.addData("geo1", KEY_1, SUB_KEY_1).intValue());
        Assert.assertEquals(2, wrapper.addData("geo2", KEY_1, SUB_KEY_1).intValue());
        Assert.assertEquals(2, wrapper.getData(KEY_1, SUB_KEY_1).intValue());
        Assert.assertNull(wrapper.getData(KEY_1, SUB_KEY_2));
        Assert.assertNull(wrapper.getData(KEY_2, SUB_KEY_2));

        for (int i = 0; i < 30; i++) {
            String geo = String.format("%s_%d", "geo", i);
            wrapper.addData(geo, KEY_2, SUB_KEY_3);
        }
        Assert.assertEquals(30, wrapper.getData(KEY_2, SUB_KEY_3).intValue());
        Assert.assertEquals(2, wrapper.getData(KEY_1, SUB_KEY_1).intValue());
        Assert.assertNull(wrapper.getData(KEY_2, SUB_KEY_2));
    }
}
