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

@RunWith(PowerMockRunner.class)
@PrepareForTest(ByteArrayCacheStore.class)
public class DoubleArrayCacheWrapperTest {
    private Map<String, byte[]> data = new HashMap<>();
    private ByteArrayCacheStore cacheStore = PowerMockito.mock(ByteArrayCacheStore.class);
    private static final String TEST_KEY = "TEST";

    @Before
    public void setUp() {
        PowerMockito.doAnswer((Answer<byte[]>) invocationOnMock -> data.get(TEST_KEY)).when(cacheStore).getCache(TEST_KEY);

        PowerMockito.doAnswer((Answer<byte[]>) invocationOnMock -> {
            data.put(TEST_KEY, new byte[84]);
            return data.get(TEST_KEY);
        }).when(cacheStore).allocate(TEST_KEY);
    }

    @Test
    public void testSumDoubleCacheWrapper() {
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setStorageType(StorageType.BYTES_ARRAY);
        meta.setCacheType(CacheType.SUM_DOUBLE);

        DoubleArrayCacheWrapper.SumDoubleArrayCacheWrapper wrapper = new DoubleArrayCacheWrapper.SumDoubleArrayCacheWrapper(meta);
        wrapper.updateStoreInfo(cacheStore, 0);

        Assert.assertNull(wrapper.getData(TEST_KEY));

        Assert.assertEquals(-1.0, wrapper.addData(-1.0, TEST_KEY).doubleValue(), 0.0);
        Assert.assertEquals(0.0, wrapper.addData(1.0, TEST_KEY).doubleValue(), 0.0);
    }

    @Test
    public void testFirstDoubleCacheWrapper() {
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setStorageType(StorageType.BYTES_ARRAY);
        meta.setCacheType(CacheType.FIRST_DOUBLE);
        DoubleArrayCacheWrapper.FirstDoubleArrayCacheWrapper wrapper = new DoubleArrayCacheWrapper.FirstDoubleArrayCacheWrapper(meta);
        wrapper.updateStoreInfo(cacheStore, 0);

        Assert.assertNull(wrapper.getData(TEST_KEY));

        Assert.assertEquals(0.0, wrapper.addData(0.0, TEST_KEY).doubleValue(), 0.0);
        // return null if already add the first value
        Assert.assertNull(wrapper.addData(1.0, TEST_KEY));
        Assert.assertEquals(0.0, wrapper.getData(TEST_KEY).doubleValue(), 0.0);
    }

    @Test
    public void testLastDoubleCacheWrapper() {
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setStorageType(StorageType.BYTES_ARRAY);
        meta.setCacheType(CacheType.LAST_DOUBLE);

        DoubleArrayCacheWrapper.LastDoubleArrayCacheWrapper wrapper = new DoubleArrayCacheWrapper.LastDoubleArrayCacheWrapper(meta);
        wrapper.updateStoreInfo(cacheStore, 0);

        Assert.assertNull(wrapper.getData(TEST_KEY));

        Assert.assertEquals(0.0, wrapper.addData(0.0, TEST_KEY).doubleValue(), 0.0);


        Assert.assertEquals(1.0, wrapper.addData(1.0, TEST_KEY).doubleValue(), 0.0);
        Assert.assertEquals(90.0, wrapper.addData(90.0, TEST_KEY).doubleValue(), 0.0);
        Assert.assertEquals(-890, wrapper.addData(-890.0, TEST_KEY).doubleValue(), 0.0);
    }

    @Test
    public void testAvgDoubleCacheWrapper() {
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setStorageType(StorageType.BYTES_ARRAY);
        meta.setCacheType(CacheType.AVG_DOUBLE);

        DoubleArrayCacheWrapper.AvgDoubleArrayCacheWrapper wrapper = new DoubleArrayCacheWrapper.AvgDoubleArrayCacheWrapper(meta);
        wrapper.updateStoreInfo(cacheStore, 0);

        Assert.assertNull(wrapper.getData(TEST_KEY));

        Assert.assertEquals(3.0, wrapper.addData(3.0, TEST_KEY).doubleValue(), 0.0);
        // first time add 0?

        Assert.assertEquals(4.0, wrapper.addData(5.0, TEST_KEY).doubleValue(), 0.0);
        Assert.assertEquals(4.0, wrapper.getData(TEST_KEY), 0.0);
    }

    @Test
    public void testMaxDoubleCacheWrapper() {
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setStorageType(StorageType.BYTES_ARRAY);
        meta.setCacheType(CacheType.MAX_DOUBLE);

        DoubleArrayCacheWrapper.MaxDoubleArrayCacheWrapper wrapper = new DoubleArrayCacheWrapper.MaxDoubleArrayCacheWrapper(meta);
        wrapper.updateStoreInfo(cacheStore, 0);

        Assert.assertNull(wrapper.getData(TEST_KEY));

        Assert.assertEquals(0.0, wrapper.addData(0.0, TEST_KEY).doubleValue(), 0.0);

        Assert.assertEquals(2.0, wrapper.addData(2.0, TEST_KEY).doubleValue(), 0.0);
        Assert.assertNull(wrapper.addData(-1.0, TEST_KEY));
        Assert.assertEquals(9.8, wrapper.addData(9.8, TEST_KEY).doubleValue(), 0.0);
        Assert.assertEquals(9.8, wrapper.getData(TEST_KEY), 0.0);
    }

    @Test
    public void testMinDoubleCacheWrapper() {
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setStorageType(StorageType.BYTES_ARRAY);
        meta.setCacheType(CacheType.MIN_DOUBLE);

        DoubleArrayCacheWrapper.MinDoubleArrayCacheWrapper wrapper = new DoubleArrayCacheWrapper.MinDoubleArrayCacheWrapper(meta);
        wrapper.updateStoreInfo(cacheStore, 0);

        Assert.assertNull(wrapper.getData(TEST_KEY));

        Assert.assertEquals(0.0, wrapper.addData(0.0, TEST_KEY), 0.0);

        Assert.assertEquals(-1.0, wrapper.addData(-1.0, TEST_KEY), 0.0);
        Assert.assertEquals(-9.0, wrapper.addData(-9.0, TEST_KEY), 0.0);
        Assert.assertNull(wrapper.addData(2.0, TEST_KEY));
        Assert.assertEquals(-9.0, wrapper.getData(TEST_KEY).doubleValue(), 0.0);
    }

    @Test
    public void testStddevDoubleCacheWrapper() {
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setStorageType(StorageType.BYTES_ARRAY);
        meta.setCacheType(CacheType.STDDEV_DOUBLE);

        DoubleArrayCacheWrapper.StddevDoubleArrayCacheWrapper wrapper = new DoubleArrayCacheWrapper.StddevDoubleArrayCacheWrapper(meta);
        wrapper.updateStoreInfo(cacheStore, 0);
        Assert.assertNull(wrapper.getData(TEST_KEY));
        Assert.assertEquals(0.0, wrapper.addData(1.0, TEST_KEY), 0.0);
        Assert.assertEquals(0.0, wrapper.addData(1.0, TEST_KEY), 0.0);
        Assert.assertEquals(0.3333, wrapper.addData(2.0, TEST_KEY), 0.0001);
        Assert.assertEquals(3.58333, wrapper.addData(5.0, TEST_KEY), 0.00001);

        Assert.assertEquals(3.58333, wrapper.getData(TEST_KEY), 0.00001);
    }

    @Test
    public void testCVDoubleCacheWrapper() {
        CacheWrapperMeta meta = new CacheWrapperMeta();
        meta.setStorageType(StorageType.BYTES_ARRAY);
        meta.setCacheType(CacheType.CV_DOUBLE);

        DoubleArrayCacheWrapper.CVDoubleArrayCacheWrapper wrapper = new DoubleArrayCacheWrapper.CVDoubleArrayCacheWrapper(meta);
        wrapper.updateStoreInfo(cacheStore, 0);
        Assert.assertNull(wrapper.getData(TEST_KEY));
        Assert.assertEquals(0.0, wrapper.addData(1.0, TEST_KEY), 0.0);
        Assert.assertEquals(0.0, wrapper.addData(1.0, TEST_KEY), 0.0);
        Assert.assertEquals(0.43301, wrapper.addData(2.0, TEST_KEY), 0.00001);
        Assert.assertEquals(0.84132, wrapper.addData(5.0, TEST_KEY), 0.00001);

        Assert.assertEquals(0.84132, wrapper.getData(TEST_KEY), 0.00001);
    }
}

