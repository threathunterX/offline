package com.threathunter.bordercollie.slot.compute.cache.wrapper.array;

import com.threathunter.bordercollie.slot.compute.cache.CacheType;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapperMeta;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.PrimaryData;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.StringPool;
import com.threathunter.bordercollie.slot.util.HashType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.threathunter.bordercollie.slot.compute.cache.wrapper.array.CacheConstants.GLOBAL_KEY;

/**
 * 
 */
public abstract class GlobalLongArrayCacheWrapper extends ByteArrayCacheWrapper<Long, Long> {
    protected static final byte NULL_FLAG = 0;
    protected static final byte VALUE_FLAG = 1;

    public GlobalLongArrayCacheWrapper(CacheWrapperMeta meta) {
        super(meta);
    }

    @Override
    public Object readAll(String key) {
        return null;
    }

    public static class GlobalSumLongArrayCacheWrapper extends GlobalLongArrayCacheWrapper {
        public static final CacheType TYPE = CacheType.GLOBAL_SUM_LONG;

        public GlobalSumLongArrayCacheWrapper(CacheWrapperMeta meta) {
            super(meta);
        }

        @Override
        public Long addData(Long value, String... keys) {
            if (value == null) {
                return null;
            }
            if (this.cacheStore.getCache(GLOBAL_KEY) == null) {
                this.cacheStore.allocate(GLOBAL_KEY);
            }
            byte[] target = this.cacheStore.getCache(GLOBAL_KEY);

            if (ByteArrayUtil.getByte(this.offset, target) == NULL_FLAG) {
                ByteArrayUtil.putByte(this.offset, target, VALUE_FLAG);
                return ByteArrayUtil.putLong(this.offset + 1, target, value);
            }

            return ByteArrayUtil.addLong(this.offset + 1, target, value);
        }

        @Override
        public Long getData(String... keys) {
            byte[] cache = this.cacheStore.getCache(GLOBAL_KEY);
            if (cache == null) {
                return null;
            }
            return ByteArrayUtil.getLong(this.offset + 1, cache);
        }

        @Override
        public int getCacheSize() {
            return 9;
        }

        @Override
        public PrimaryData merge(PrimaryData lastMerged, String... keys) {
            return null;
        }
    }

    public static class GlobalGroupSumLongArrayCacheWrapper extends GlobalLongArrayCacheWrapper {
        public static final CacheType TYPE = CacheType.GLOBAL_GROUP_SUM_LONG;
        private final LimitHashArraySet arraySet;

        public GlobalGroupSumLongArrayCacheWrapper(CacheWrapperMeta meta) {
            super(meta);
            this.arraySet = new LimitHashArraySet(20, 12, 3);
        }

        @Override
        public Long addData(Long value, String... keys) {
            if (this.cacheStore.getCache(GLOBAL_KEY) == null) {
                this.cacheStore.allocate(GLOBAL_KEY);
            }
            if (keys.length <= 0 || keys[0] == null) {
                return null;
            }
            byte[] target = this.cacheStore.getCache(GLOBAL_KEY);

            int hash = HashType.getHash(keys[0]);

            int currentOffset = this.arraySet.find(hash, target, this.offset);
            if (currentOffset >= 0) {
                // find and add count
                return ByteArrayUtil.addLong(currentOffset + 4, target, value);
            }
            // need add
            // ignore if hash conflict (advance 3 times)
            int addOffset = this.arraySet.add(hash, target, this.offset);
            if (addOffset >= 0) {
                StringPool.getInstance().putString(hash, keys[0]);
                return ByteArrayUtil.addLong(addOffset + 4, target, value);
            }

            return null;
        }

        @Override
        public Map getData(String... keys) {
            Map<String, Object> result = new HashMap<>();
            byte[] target = this.cacheStore.getCache(GLOBAL_KEY);
            if (target == null) {
                return null;
            }
            System.out.println(this.arraySet.getSize(target, this.offset));
            List<LimitHashArraySet.SetEntry> entries = this.arraySet.getAll(target, this.offset);
            entries.forEach(entry -> {
                String origin = StringPool.getInstance().getString(entry.getHash());
                if (origin != null) {
                    result.put(origin, ByteArrayUtil.getLong(entry.getOffset() + 4, target));
                }
            });

            return result;
        }

        @Override
        public int getCacheSize() {
            return this.arraySet.getTotalBytesSize();
        }

        @Override
        public PrimaryData merge(PrimaryData lastMerged, String... keys) {
            return null;
        }
    }
}
