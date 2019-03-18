package com.threathunter.bordercollie.slot.compute.cache.wrapper.array;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * For distinct count wrapper, because the actual storage is in other place,
 * this set instance only contains some meta for searching and writing.
 * <p>
 * 
 */
public class LimitHashArraySet {
    private final HashFunction function = Hashing.murmur3_32();
    private final int capacity;
    private final int threshold;
    private final int maxProbeLength;
    private final int totalBytesSize;
    private final int unitSize;

    private final int statsOffset;
    private final int countReadOffset;

    static final int MAXIMUM_CAPACITY = 1 << 30;
    static final int DEFAULT_CAPACITY = 1 << 7; // 128
    static final float DEFAULT_LOAD_FACTOR = 0.75F;
    static final int DEFAULT_MAX_PROBE_LENGTH = 3;

    int probCount = 0;

    public int getTotalProb() {
        return probCount;
    }

    public int getCapacity() {
        return this.capacity;
    }

    public LimitHashArraySet(int maxCount, int unitSize, float loadFactor, int maxProbeLength) {
        if (maxCount > MAXIMUM_CAPACITY) {
            maxCount = MAXIMUM_CAPACITY;
        }
        if (maxCount <= 0) {
            maxCount = DEFAULT_CAPACITY;
        }
        this.threshold = maxCount;

        int cap = (int) ((float) maxCount / loadFactor) + 1;
        this.capacity = tableSizeFor(cap);

        this.unitSize = unitSize;

        this.totalBytesSize = this.capacity * (unitSize + 1) + 4; // 4 for storing data hash, 1 for storing states, last 4 bytes is for record added count
        this.statsOffset = this.capacity * unitSize; // 4 for storing data hash, 1 for storing states
        this.countReadOffset = this.capacity * (unitSize + 1);
        this.maxProbeLength = maxProbeLength;
    }

    public LimitHashArraySet(int maxCount, int unitSize, int maxProbeLength) {
        this(maxCount, unitSize, DEFAULT_LOAD_FACTOR, maxProbeLength);
    }

    /**
     * Default unit size is 4, for compared hash(int 32).
     *
     * @param maxCount
     */
    public LimitHashArraySet(int maxCount) {
        this(maxCount, 4, DEFAULT_LOAD_FACTOR, DEFAULT_MAX_PROBE_LENGTH);
    }

    public static int totalBytesSizeForMaxCount(int maxCount, float loadFactor) {
        int cap = (int) ((float) maxCount / loadFactor) + 1;
        return tableSizeFor(cap) * 5 + 4;
    }

    public static int totalBytesSizeForMaxCount(int maxCount) {
        return totalBytesSizeForMaxCount(maxCount, DEFAULT_LOAD_FACTOR);
    }

    public int getTotalBytesSize() {
        return this.totalBytesSize;
    }

    public int add(String origin, byte[] target, int dataOffset) {
        if (origin == null) {
            return -1;
        }
        int hash = function.hashString(origin, Charset.defaultCharset()).asInt();
        return this.add(hash, target, dataOffset);
    }

    /**
     * Add hash, if the size is exceed threshold, return -1 directly.
     *
     * @param hash
     * @param target
     * @param dataOffset
     * @return offset of the added data in the target, or -1 if already exceed threshold, or add error.
     */
    public int add(int hash, byte[] target, int dataOffset) {
        if (getSize(target, dataOffset) >= this.threshold) {
            return -1;
        }

        for (int i = 0; i < this.maxProbeLength; i++) {
            int bucket = (this.capacity - 1) & (hash + i);
            int bucketOffset = bucket * unitSize;
            if (target[dataOffset + statsOffset + bucket] == 0) {
                ByteArrayUtil.putInt(dataOffset + bucketOffset, target, hash);
                target[dataOffset + statsOffset + bucket] = 1;
                ByteArrayUtil.addInt(dataOffset + countReadOffset, target, 1);
                return dataOffset + bucketOffset;
            }
            if (ByteArrayUtil.getInt(dataOffset + bucketOffset, target) == hash) {
                return dataOffset + bucketOffset;
            }
            probCount++;
        }
        return -1;
    }

    public int getSize(byte[] target, int dataOffset) {
        return ByteArrayUtil.getInt(dataOffset + countReadOffset, target);
    }

    public boolean contains(String origin, byte[] target, int dataOffset) {
        if (origin == null) {
            return false;
        }

        int hash = function.hashString(origin, Charset.defaultCharset()).asInt();
        return find(hash, target, dataOffset) >= 0;
    }

    public int getPosition(String origin, byte[] target, int dataOffset) {
        if (origin == null) {
            return -1;
        }

        int hash = function.hashString(origin, Charset.defaultCharset()).asInt();
        return find(hash, target, dataOffset);
    }

    /**
     * If the hash already exist in the set.
     *
     * @param hash
     * @param target
     * @param dataOffset
     * @return offset of the hash stored in the target, or -1 if not found.
     */
    public int find(int hash, byte[] target, int dataOffset) {
        for (int i = 0; i < this.maxProbeLength; i++) {
            int bucket = (this.capacity - 1) & (hash + i);
            if (target[dataOffset + statsOffset + bucket] == 0) {
                return -1;
            }
            int offset = bucket * unitSize;
            if (ByteArrayUtil.getInt(dataOffset + offset, target) == hash) {
                return dataOffset + offset;
            }
            probCount++;
        }

        return -1;
    }

    /**
     * Merge from src array bytes. Hash value 0 will be ignore.
     *
     * @param src
     * @param srcDataOffset
     * @param dest
     * @param destDataOffset
     * @return the data that can't added to dest, and also not contains in the dest
     */
    public List<Integer> merge(byte[] src, int srcDataOffset, byte[] dest, int destDataOffset) {
        List<Integer> ignored = new ArrayList<>();
        int srcStatOffset = srcDataOffset + this.statsOffset;
        for (int i = srcDataOffset; i < srcStatOffset; i += this.unitSize) {
            int hash = ByteArrayUtil.getInt(i, src);
            if (hash != 0) {
                if (this.find(hash, dest, destDataOffset) > 0) {
                    continue;
                }
                if (this.add(hash, dest, destDataOffset) <= 0) {
                    ignored.add(hash);
                }

            }
        }

        return ignored;
    }

    public List<SetEntry> getAll(byte[] target, int dataOffset) {
        List<SetEntry> result = new ArrayList<>();
        for (int i = dataOffset; i < dataOffset + this.statsOffset; i += this.unitSize) {
            int hash = ByteArrayUtil.getInt(i, target);
            if (hash != 0) {
                result.add(new SetEntry(hash, i));
            }
        }

        return result;
    }

    static final int tableSizeFor(int cap) {
        int n = cap - 1;
        n |= n >>> 1;
        n |= n >>> 2;
        n |= n >>> 4;
        n |= n >>> 8;
        n |= n >>> 16;
        return (n < 0) ? 1 : (n >= MAXIMUM_CAPACITY) ? MAXIMUM_CAPACITY : n + 1;
    }

    public static class SetEntry {
        int offset;
        int hash;

        public SetEntry(int hash, int offset) {
            this.hash = hash;
            this.offset = offset;
        }

        public int getOffset() {
            return offset;
        }

        public void setOffset(int offset) {
            this.offset = offset;
        }

        public int getHash() {
            return hash;
        }

        public void setHash(int hash) {
            this.hash = hash;
        }
    }
}
