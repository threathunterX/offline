package com.threathunter.bordercollie.slot.compute.cache.storage;

import com.threathunter.bordercollie.slot.compute.cache.StorageType;
import com.threathunter.variable.exception.NotSupportException;
import org.junit.Test;

import java.util.ArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

/**
 * @author Yuan Yi <yi.yuan@threathunter.cn>
 * @since: 1.5
 */
public class CacheStoreFactoryTest {

    @Test
    public void testNewCacheStore_BuildIn() {
        CacheStore cacheStore = CacheStoreFactory.newCacheStore(StorageType.BUILDIN, new ArrayList<>());
        assertThat(cacheStore).isNotNull();
    }

    @Test
    public void testNewCacheStore_Byte_Array() {
        CacheStore cacheStore = CacheStoreFactory.newCacheStore(StorageType.BYTES_ARRAY, new ArrayList<>());
        assertThat(cacheStore).isNotNull();
    }

    @Test(expected = NotSupportException.class)
    public void testNewCacheStore_Other() {
        CacheStore cacheStore = CacheStoreFactory.newCacheStore(null, new ArrayList<>());
        fail();
    }


    @Test(expected = NotSupportException.class)
    public void testNewCacheStore_BuildIn_NullWrapper() {
        CacheStore cacheStore = CacheStoreFactory.newCacheStore(StorageType.BUILDIN, null);
        fail();
    }
}
