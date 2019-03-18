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
public class SecondaryLongArrayCacheWrapperTest {
    private static final String KEY_1 = "key1";
    private static final String KEY_2 = "key2";

    private static final String SUB_KEY_1 = "subkey1";
    private static final String SUB_KEY_2 = "subkey2";

    private ByteArrayCacheStore cacheStore = PowerMockito.mock(ByteArrayCacheStore.class);
    private Map<String, byte[]> data = new HashMap<>();

    @Before
    public void setUp() {
        PowerMockito.doAnswer((Answer<byte[]>) mockArg -> data.get(KEY_1)).when(cacheStore).getCache(KEY_1);
        PowerMockito.doAnswer((Answer<byte[]>) mockArg -> data.get(KEY_2)).when(cacheStore).getCache(KEY_2);

        PowerMockito.doAnswer((Answer<byte[]>) mockArg -> {
            data.put(KEY_1, new byte[1768]);
            return data.get(KEY_1);
        }).when(cacheStore).allocate(KEY_1);
        PowerMockito.doAnswer((Answer<byte[]>) mockArg -> {
            data.put(KEY_2, new byte[1768]);
            return data.get(KEY_2);
        }).when(cacheStore).allocate(KEY_2);
    }

    @Test
    public void testSecondarySumLong() {
        SecondaryLongArrayCacheWrapper.SecondarySumLongArrayCacheWrapper wrapper = new SecondaryLongArrayCacheWrapper.SecondarySumLongArrayCacheWrapper(getCacheWrapperMeta(CacheType.SECONDARY_SUM_LONG));
        wrapper.updateStoreInfo(cacheStore, 0);
        Assert.assertEquals(120, wrapper.addData(120l, KEY_1, SUB_KEY_1).longValue());
        Assert.assertEquals(30l, wrapper.addData(-90l, KEY_1, SUB_KEY_1).longValue());
        Assert.assertEquals(30l, wrapper.getData(KEY_1, SUB_KEY_1).longValue());
        Assert.assertNull(wrapper.getData(KEY_1, SUB_KEY_2));
        Assert.assertNull(wrapper.getData(KEY_2, SUB_KEY_2));

        Assert.assertEquals(-987, wrapper.addData(-987l, KEY_1, SUB_KEY_2).longValue());

        for (int i = 0; i < 30; i++) {
            long value = i % 2 == 0 ? i : i * -1;
            String subKey = String.format("%s_%d", SUB_KEY_2, i);
            Long first = wrapper.addData(value, KEY_1, subKey);
            Long second = wrapper.addData(value * -1, KEY_1, subKey);
            if (i <= 17) {
                Assert.assertEquals(value, first.longValue());
                Assert.assertEquals(0l, second.longValue());
            } else {
                Assert.assertNull(first);
                Assert.assertNull(second);
            }
        }
    }

    @Test
    public void testSecondaryFirstLong() {
        SecondaryLongArrayCacheWrapper.SecondaryFirstLongArrayCacheWrapper wrapper = new SecondaryLongArrayCacheWrapper.SecondaryFirstLongArrayCacheWrapper(getCacheWrapperMeta(CacheType.SECONDARY_FIRST_LONG));
        wrapper.updateStoreInfo(cacheStore, 0);
        Assert.assertEquals(120, wrapper.addData(120l, KEY_1, SUB_KEY_1).longValue());
        Assert.assertNull(wrapper.addData(-90l, KEY_1, SUB_KEY_1));
        Assert.assertEquals(120, wrapper.getData(KEY_1, SUB_KEY_1).longValue());
        Assert.assertNull(wrapper.getData(KEY_1, SUB_KEY_2));
        Assert.assertNull(wrapper.getData(KEY_2, SUB_KEY_2));

        Assert.assertEquals(-987, wrapper.addData(-987l, KEY_1, SUB_KEY_2).longValue());

        for (int i = 0; i < 30; i++) {
            long value = i % 2 == 0 ? i : i * -1;
            String subKey = String.format("%s_%d", SUB_KEY_2, i);
            Long first = wrapper.addData(value, KEY_1, subKey);
            Long second = wrapper.addData(value * -1, KEY_1, subKey);
            if (i <= 17) {
                Assert.assertEquals(value, first.longValue());
                Assert.assertNull(second);
            } else {
                Assert.assertNull(first);
                Assert.assertNull(second);
            }
        }
    }

    @Test
    public void testSecondaryLastLong() {
        SecondaryLongArrayCacheWrapper.SecondaryLastLongArrayCacheWrapper wrapper = new SecondaryLongArrayCacheWrapper.SecondaryLastLongArrayCacheWrapper(getCacheWrapperMeta(CacheType.SECONDARY_LAST_LONG));
        wrapper.updateStoreInfo(cacheStore, 9);
        Assert.assertEquals(1900, wrapper.addData(1900l, KEY_1, SUB_KEY_1).longValue());
        Assert.assertEquals(-78787, wrapper.addData(-78787l, KEY_1, SUB_KEY_1).longValue());
        Assert.assertEquals(0, wrapper.addData(0l, KEY_1, SUB_KEY_1).longValue());
        Assert.assertNull(wrapper.getData(KEY_1, SUB_KEY_2));
        Assert.assertNull(wrapper.getData(KEY_2, SUB_KEY_2));

        for (int i = 0; i < 30; i++) {
            long value = i % 2 == 0 ? i : i * -1;
            String subKey = String.format("%s_%d", SUB_KEY_2, i);
            Long first = wrapper.addData(value, KEY_2, subKey);
            Long second = wrapper.addData(value * -1, KEY_2, subKey);
            if (i < 20) {
                Assert.assertEquals(value, first.longValue());
                Assert.assertEquals(value * -1, second.longValue());
            } else {
                Assert.assertNull(first);
                Assert.assertNull(second);
            }
        }
    }

    @Test
    public void testSecondaryMaxLong() {
        SecondaryLongArrayCacheWrapper.SecondaryMaxLongArrayCacheWrapper wrapper = new SecondaryLongArrayCacheWrapper.SecondaryMaxLongArrayCacheWrapper(getCacheWrapperMeta(CacheType.SECONDARY_MAX_LONG));
        wrapper.updateStoreInfo(cacheStore, 4);

        Assert.assertEquals(90, wrapper.addData(90l, KEY_1, SUB_KEY_1).longValue());
        Assert.assertEquals(90, wrapper.addData(-2l, KEY_1, SUB_KEY_1).longValue());
        Assert.assertEquals(90, wrapper.addData(0l, KEY_1, SUB_KEY_1).longValue());
        Assert.assertEquals(10000, wrapper.addData(10000l, KEY_1, SUB_KEY_1).longValue());
        Assert.assertNull(wrapper.getData(KEY_1, SUB_KEY_2));
        Assert.assertNull(wrapper.getData(KEY_2, SUB_KEY_2));

        for (int i = 0; i < 30; i++) {
            long value = i % 2 == 0 ? i : i * -1;
            String subKey = String.format("%s_%d", SUB_KEY_2, i);
            Long result = wrapper.addData(value, KEY_2, subKey);
            if (i < 20) {
                Assert.assertEquals(value, result.longValue());
            } else {
                Assert.assertNull(result);
            }
        }
    }

    @Test
    public void testSecondaryMinLong() {
        SecondaryLongArrayCacheWrapper.SecondaryMinLongArrayCacheWrapper wrapper = new SecondaryLongArrayCacheWrapper.SecondaryMinLongArrayCacheWrapper(getCacheWrapperMeta(CacheType.SECONDARY_MIN_LONG));
        wrapper.updateStoreInfo(cacheStore, 0);

        Assert.assertEquals(190, wrapper.addData(190l, KEY_1, SUB_KEY_1).longValue());
        Assert.assertEquals(0, wrapper.addData(0l, KEY_1, SUB_KEY_1).longValue());
        Assert.assertEquals(0, wrapper.addData(10l, KEY_1, SUB_KEY_1).longValue());
        Assert.assertEquals(-10, wrapper.addData(-10l, KEY_1, SUB_KEY_1).longValue());
        Assert.assertEquals(-10, wrapper.getData(KEY_1, SUB_KEY_1).longValue());
        Assert.assertNull(wrapper.getData(KEY_1, SUB_KEY_2));
        Assert.assertNull(wrapper.getData(KEY_2, SUB_KEY_2));

        for (int i = 0; i < 30; i++) {
            long value = i % 2 == 0 ? i : i * -1;
            String subKey = String.format("%s_%d", SUB_KEY_2, i);
            Long result = wrapper.addData(value, KEY_2, subKey);
            if (i < 20) {
                Assert.assertEquals(value, result.longValue());
            } else {
                Assert.assertNull(result);
            }
        }
    }

    private CacheWrapperMeta getCacheWrapperMeta(CacheType type) {
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setSecondaryKeyHashType(HashType.NORMAL);
        meta.setIndexCount(2);
        meta.setStorageType(StorageType.BYTES_ARRAY);
        meta.setValueHashType(HashType.NORMAL);
        meta.setCacheType(type);

        return meta;
    }
}
