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
public class SecondaryDoubleArrayCacheWrapperTest {
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
    public void testSecondarySumDouble() {
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setStorageType(StorageType.BYTES_ARRAY);
        meta.setCacheType(CacheType.SECONDARY_SUM_DOUBLE);
        meta.setSecondaryKeyHashType(HashType.NORMAL);
        meta.setValueHashType(HashType.NORMAL);

        SecondaryDoubleArrayCacheWrapper.SecondarySumDoubleArrayCacheWrapper wrapper = new SecondaryDoubleArrayCacheWrapper.SecondarySumDoubleArrayCacheWrapper(meta);
        wrapper.updateStoreInfo(cacheStore, 5);
        Assert.assertEquals(-2.333, wrapper.addData(-2.333, KEY_1, SUB_KEY_1).doubleValue(), 0.0);
        Assert.assertEquals(-2.333, wrapper.getData(KEY_1, SUB_KEY_1).doubleValue(), 0.0);
        Assert.assertEquals(0.0, wrapper.addData(2.333, KEY_1, SUB_KEY_1).doubleValue(), 0.0);
        Assert.assertNull(wrapper.getData(KEY_1, SUB_KEY_2));

        for (int i = 0; i < 30; i++) {
            double value = i % 2 == 0 ? i : i * -1;
            String subKey = String.format("%s_%d", SUB_KEY_2, i);
            Double first = wrapper.addData(value, KEY_2, subKey);
            Double second = wrapper.addData(value * -1, KEY_2, subKey);
            if (i < 20) {
                Assert.assertEquals(value, first.doubleValue(), 0.0);
                Assert.assertEquals(0, second.doubleValue(), 0.0);
            } else {
                Assert.assertNull(first);
                Assert.assertNull(second);
            }
        }
    }

    @Test
    public void testSecondaryFirstDouble() {
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setStorageType(StorageType.BYTES_ARRAY);
        meta.setCacheType(CacheType.SECONDARY_FIRST_DOUBLE);
        meta.setSecondaryKeyHashType(HashType.NORMAL);
        meta.setValueHashType(HashType.NORMAL);

        SecondaryDoubleArrayCacheWrapper.SecondaryFirstDoubleArrayCacheWrapper wrapper = new SecondaryDoubleArrayCacheWrapper.SecondaryFirstDoubleArrayCacheWrapper(meta);
        wrapper.updateStoreInfo(cacheStore, 0);
        Assert.assertEquals(2.333, wrapper.addData(2.333, KEY_1, SUB_KEY_1).doubleValue(), 0.0);
        Assert.assertNull(wrapper.addData(99.8, KEY_1, SUB_KEY_1));
        Assert.assertEquals(2.333, wrapper.getData(KEY_1, SUB_KEY_1).doubleValue(), 0.0);
        Assert.assertNull(wrapper.getData(KEY_1, SUB_KEY_2));
        Assert.assertNull(wrapper.getData(KEY_2, SUB_KEY_2));

        Assert.assertEquals(0.0, wrapper.addData(0.0, KEY_1, SUB_KEY_2).doubleValue(), 0.0);

        for (int i = 0; i < 30; i++) {
            double value = i % 2 == 0 ? i : i * -1;
            String subKey = String.format("%s_%d", SUB_KEY_2, i);
            Double first = wrapper.addData(value, KEY_1, subKey);
            Double second = wrapper.addData(value * -1, KEY_1, subKey);
            if (i <= 17) {
                Assert.assertEquals(value, first.doubleValue(), 0.0);
                Assert.assertNull(second);
            } else {
                Assert.assertNull(first);
                Assert.assertNull(second);
            }
        }
    }

    @Test
    public void testSecondaryLastDouble() {
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setStorageType(StorageType.BYTES_ARRAY);
        meta.setCacheType(CacheType.SECONDARY_LAST_DOUBLE);
        meta.setValueHashType(HashType.NORMAL);
        meta.setSecondaryKeyHashType(HashType.NORMAL);

        SecondaryDoubleArrayCacheWrapper.SecondaryLastDoubleArrayCacheWrapper wrapper = new SecondaryDoubleArrayCacheWrapper.SecondaryLastDoubleArrayCacheWrapper(meta);
        wrapper.updateStoreInfo(cacheStore, 9);
        Assert.assertEquals(2.333, wrapper.addData(2.333, KEY_1, SUB_KEY_1).doubleValue(), 0.0);
        Assert.assertEquals(0.0, wrapper.addData(0.0, KEY_1, SUB_KEY_1).doubleValue(), 0.0);
        Assert.assertEquals(-1.99, wrapper.addData(-1.99, KEY_1, SUB_KEY_1).doubleValue(), 0.0);
        Assert.assertNull(wrapper.getData(KEY_1, SUB_KEY_2));
        Assert.assertNull(wrapper.getData(KEY_2, SUB_KEY_2));

        for (int i = 0; i < 30; i++) {
            double value = i % 2 == 0 ? i : i * -1;
            String subKey = String.format("%s_%d", SUB_KEY_2, i);
            Double first = wrapper.addData(value, KEY_2, subKey);
            Double second = wrapper.addData(value * -1, KEY_2, subKey);
            if (i < 20) {
                Assert.assertEquals(value, first.doubleValue(), 0.0);
                Assert.assertEquals(value * -1, second.doubleValue(), 0.0);
            } else {
                Assert.assertNull(first);
                Assert.assertNull(second);
            }
        }
    }

    @Test
    public void testSecondaryMaxDouble() {
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setStorageType(StorageType.BYTES_ARRAY);
        meta.setCacheType(CacheType.SECONDARY_MAX_DOUBLE);
        meta.setValueHashType(HashType.NORMAL);
        meta.setSecondaryKeyHashType(HashType.NORMAL);

        SecondaryDoubleArrayCacheWrapper.SecondaryMaxDoubleArrayCacheWrapper wrapper = new SecondaryDoubleArrayCacheWrapper.SecondaryMaxDoubleArrayCacheWrapper(meta);
        wrapper.updateStoreInfo(cacheStore, 4);

        Assert.assertEquals(2.333, wrapper.addData(2.333, KEY_1, SUB_KEY_1).doubleValue(), 0.0);
        Assert.assertEquals(2.333, wrapper.addData(0.0, KEY_1, SUB_KEY_1).doubleValue(), 0.0);
        Assert.assertEquals(2.333, wrapper.addData(-1.99, KEY_1, SUB_KEY_1).doubleValue(), 0.0);
        Assert.assertEquals(10, wrapper.addData(10.0, KEY_1, SUB_KEY_1).doubleValue(), 0.0);
        Assert.assertNull(wrapper.getData(KEY_1, SUB_KEY_2));
        Assert.assertNull(wrapper.getData(KEY_2, SUB_KEY_2));

        for (int i = 0; i < 30; i++) {
            double value = i % 2 == 0 ? i : i * -1;
            String subKey = String.format("%s_%d", SUB_KEY_2, i);
            Double result = wrapper.addData(value, KEY_2, subKey);
            if (i < 20) {
                Assert.assertEquals(value, result.doubleValue(), 0.0);
            } else {
                Assert.assertNull(result);
            }
        }
    }

    @Test
    public void testSecondaryMinDouble() {
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setStorageType(StorageType.BYTES_ARRAY);
        meta.setCacheType(CacheType.SECONDARY_MIN_DOUBLE);
        meta.setValueHashType(HashType.NORMAL);
        meta.setSecondaryKeyHashType(HashType.NORMAL);

        SecondaryDoubleArrayCacheWrapper.SecondaryMinDoubleArrayCacheWrapper wrapper = new SecondaryDoubleArrayCacheWrapper.SecondaryMinDoubleArrayCacheWrapper(meta);
        wrapper.updateStoreInfo(cacheStore, 0);

        Assert.assertEquals(2.333, wrapper.addData(2.333, KEY_1, SUB_KEY_1).doubleValue(), 0.0);
        Assert.assertEquals(0.0, wrapper.addData(0.0, KEY_1, SUB_KEY_1).doubleValue(), 0.0);
        Assert.assertEquals(-1.99, wrapper.addData(-1.99, KEY_1, SUB_KEY_1).doubleValue(), 0.0);
        Assert.assertEquals(-1.99, wrapper.addData(10.0, KEY_1, SUB_KEY_1).doubleValue(), 0.0);
        Assert.assertEquals(-1.99, wrapper.getData(KEY_1, SUB_KEY_1).doubleValue(), 0.0);
        Assert.assertNull(wrapper.getData(KEY_1, SUB_KEY_2));
        Assert.assertNull(wrapper.getData(KEY_2, SUB_KEY_2));

        for (int i = 0; i < 30; i++) {
            double value = i % 2 == 0 ? i : i * -1;
            String subKey = String.format("%s_%d", SUB_KEY_2, i);
            Double result = wrapper.addData(value, KEY_2, subKey);
            if (i < 20) {
                Assert.assertEquals(value, result.doubleValue(), 0.0);
            } else {
                Assert.assertNull(result);
            }
        }
    }

    @Test
    public void testSecondaryAvgDouble() {
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setStorageType(StorageType.BYTES_ARRAY);
        meta.setCacheType(CacheType.SECONDARY_AVG_DOUBLE);
        meta.setValueHashType(HashType.NORMAL);
        meta.setSecondaryKeyHashType(HashType.NORMAL);

        SecondaryDoubleArrayCacheWrapper.SecondaryAvgDoubleArrayCacheWrapper wrapper = new SecondaryDoubleArrayCacheWrapper.SecondaryAvgDoubleArrayCacheWrapper(meta);
        wrapper.updateStoreInfo(cacheStore, 0);

        Assert.assertEquals(1.0, wrapper.addData(1.0, KEY_1, SUB_KEY_1).doubleValue(), 0.0);
        Assert.assertEquals(0.5, wrapper.addData(0.0, KEY_1, SUB_KEY_1).doubleValue(), 0.0);
        Assert.assertEquals(-2, wrapper.addData(-7.0, KEY_1, SUB_KEY_1).doubleValue(), 0.0);
        Assert.assertEquals(-2, wrapper.getData(KEY_1, SUB_KEY_1).doubleValue(), 0.0);
        Assert.assertNull(wrapper.getData(KEY_1, SUB_KEY_2));
        Assert.assertNull(wrapper.getData(KEY_2, SUB_KEY_2));

        for (int i = 0; i < 30; i++) {
            double value = i % 2 == 0 ? i : i * -1;
            String subKey = String.format("%s_%d", SUB_KEY_1, i);
            Double result = wrapper.addData(value, KEY_1, subKey);
            if (i < 19) {
                Assert.assertEquals(value, result.doubleValue(), 0.0);
            } else {
                Assert.assertNull(result);
            }
        }
        Assert.assertEquals(-2, wrapper.getData(KEY_1, SUB_KEY_1).doubleValue(), 0.0);
    }
}
