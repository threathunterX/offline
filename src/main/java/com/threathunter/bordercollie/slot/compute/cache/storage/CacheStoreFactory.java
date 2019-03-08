package com.threathunter.bordercollie.slot.compute.cache.storage;

import com.threathunter.bordercollie.slot.compute.cache.StorageType;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapper;
import com.threathunter.variable.exception.NotSupportException;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * Created by daisy on 17/3/20.
 */
@Slf4j
public class CacheStoreFactory {
    public static CacheStore newCacheStore(final StorageType storageType, final List<CacheWrapper> cacheWrappers) {
        if (cacheWrappers != null) {
            if (StorageType.BUILDIN.equals(storageType)) {
                return new BuiltinCacheStore(cacheWrappers);
            } else if (StorageType.BYTES_ARRAY.equals(storageType)) {
                return new ByteArrayCacheStore(cacheWrappers);
            }
            throw new NotSupportException("storage type is not supported: " + storageType);
        } else {
            throw new NotSupportException("cacheWrappers  are NULL.");
        }

    }
}
