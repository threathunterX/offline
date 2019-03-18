package com.threathunter.bordercollie.slot.compute.cache.wrapper;

import com.threathunter.bordercollie.slot.compute.cache.CacheType;
import com.threathunter.bordercollie.slot.compute.cache.StorageType;
import com.threathunter.bordercollie.slot.util.HashType;

/**
 * 
 */
public class CacheWrapperMeta {
    private StorageType storageType;
    private int indexCount;
    private CacheType cacheType;

    private HashType secondaryKeyHashType;
    private HashType valueHashType;

    public StorageType getStorageType() {
        return storageType;
    }

    public void setStorageType(StorageType storageType) {
        this.storageType = storageType;
    }

    public int getIndexCount() {
        return indexCount;
    }

    public void setIndexCount(int indexCount) {
        this.indexCount = indexCount;
    }

    public CacheType getCacheType() {
        return cacheType;
    }

    public void setCacheType(CacheType cacheType) {
        this.cacheType = cacheType;
    }

    public HashType getSecondaryKeyHashType() {
        return secondaryKeyHashType;
    }

    public void setSecondaryKeyHashType(HashType secondaryKeyHashType) {
        this.secondaryKeyHashType = secondaryKeyHashType;
    }

    public HashType getValueHashType() {
        return valueHashType;
    }

    public void setValueHashType(HashType valueHashType) {
        this.valueHashType = valueHashType;
    }
}
