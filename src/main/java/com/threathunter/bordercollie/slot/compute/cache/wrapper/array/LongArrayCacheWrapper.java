package com.threathunter.bordercollie.slot.compute.cache.wrapper.array;

import com.threathunter.bordercollie.slot.compute.cache.CacheType;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapperMeta;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.PrimaryData;

/**
 * Created by toyld on 3/21/17.
 */
public abstract class LongArrayCacheWrapper extends ByteArrayCacheWrapper<Long, Object> {
    protected static final byte NULL_FLAG = 0;
    protected static final byte VALUE_FLAG = 1;

    public LongArrayCacheWrapper(CacheWrapperMeta meta) {
        super(meta);
    }

    protected boolean nullOrEmpty(final String... keys) {
        if (keys == null || keys.length < 1) {
            return true;
        }
        return false;
    }

    public static class FirstLongArrayCacheWrapper extends LongArrayCacheWrapper {
        public static final CacheType TYPE = CacheType.FIRST_LONG;

        public FirstLongArrayCacheWrapper(CacheWrapperMeta meta) {
            super(meta);
        }

        @Override
        public Long addData(final Long value, final String... keys) {
            if (value == null || nullOrEmpty(keys)) {
                return null;
            }
            if (this.cacheStore.getCache(keys[0]) == null) {
                this.cacheStore.allocate(keys[0]);
                byte[] target = this.cacheStore.getCache(keys[0]);
                ByteArrayUtil.putByte(this.offset, target, VALUE_FLAG);
                return ByteArrayUtil.putLong(this.offset + 1, target, value);
            } else {
                byte[] target = this.cacheStore.getCache(keys[0]);
                if (ByteArrayUtil.getByte(this.offset, target) == NULL_FLAG) {
                    ByteArrayUtil.putByte(this.offset, target, VALUE_FLAG);
                    return ByteArrayUtil.putLong(this.offset + 1, target, value);
                } else {
                    return null;
                }
            }
        }

        @Override
        public Long getData(final String... keys) {
            if (nullOrEmpty(keys)) {
                return null;
            }
            byte[] cache = this.cacheStore.getCache(keys[0]);
            if (cache == null || ByteArrayUtil.getByte(this.offset, cache) == NULL_FLAG) {
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

        @Override
        public Object readAll(String key) {
            return null;
        }
    }

    // TODO add test
    public static class SumLongArrayCacheWrapper extends LongArrayCacheWrapper {
        public static final CacheType TYPE = CacheType.SUM_LONG;

        public SumLongArrayCacheWrapper(CacheWrapperMeta meta) {
            super(meta);
        }

        @Override
        public Long addData(Long value, String... keys) {
            if (value == null || nullOrEmpty(keys)) {
                return null;
            }
            if (this.cacheStore.getCache(keys[0]) == null) {
                this.cacheStore.allocate(keys[0]);
            }
            byte[] target = this.cacheStore.getCache(keys[0]);
            if (ByteArrayUtil.getByte(this.offset, target) == NULL_FLAG) {
                ByteArrayUtil.putByte(this.offset, target, VALUE_FLAG);
                return ByteArrayUtil.putLong(this.offset + 1, target, value);
            }

            return ByteArrayUtil.addLong(this.offset + 1, target, value);
        }

        @Override
        public Long getData(String... keys) {
            if (nullOrEmpty(keys)) {
                return null;
            }

            byte[] target = this.cacheStore.getCache(keys[0]);
            if (target == null || ByteArrayUtil.getByte(this.offset, target) == NULL_FLAG) {
                return null;
            }

            return ByteArrayUtil.getLong(this.offset + 1, target);
        }

        @Override
        public int getCacheSize() {
            return 9;
        }

        @Override
        public PrimaryData merge(PrimaryData lastMerged, String... keys) {
            return null;
        }

        @Override
        public Object readAll(String key) {
            return null;
        }
    }

    public static class LastLongArrayCacheWrapper extends LongArrayCacheWrapper {
        public static final CacheType TYPE = CacheType.LAST_LONG;

        public LastLongArrayCacheWrapper(CacheWrapperMeta meta) {
            super(meta);
        }

        @Override
        public Long addData(final Long value, final String... keys) {
            if (value == null || nullOrEmpty(keys)) {
                return null;
            }
            if (this.cacheStore.getCache(keys[0]) == null) {
                this.cacheStore.allocate(keys[0]);
            }
            byte[] target = this.cacheStore.getCache(keys[0]);
            ByteArrayUtil.putByte(this.offset, target, VALUE_FLAG);
            return ByteArrayUtil.putLong(this.offset + 1, target, value);

        }

        @Override
        public Long getData(final String... keys) {
            if (nullOrEmpty(keys)) {
                return null;
            }
            byte[] cache = this.cacheStore.getCache(keys[0]);
            if (cache == null || ByteArrayUtil.getByte(this.offset, cache) == NULL_FLAG) {
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

        @Override
        public Object readAll(String key) {
            return null;
        }
    }

    public static class MaxLongArrayCacheWrapper extends LongArrayCacheWrapper {
        public static final CacheType TYPE = CacheType.MAX_LONG;

        public MaxLongArrayCacheWrapper(CacheWrapperMeta meta) {
            super(meta);
        }

        @Override
        public Long addData(final Long value, final String... keys) {
            if (value == null || nullOrEmpty(keys)) {
                return null;
            }
            if (this.cacheStore.getCache(keys[0]) == null) {
                this.cacheStore.allocate(keys[0]);
            }
            byte[] target = this.cacheStore.getCache(keys[0]);
            if (ByteArrayUtil.getByte(this.offset, target) == NULL_FLAG) {
                ByteArrayUtil.putByte(this.offset, target, VALUE_FLAG);
                return ByteArrayUtil.putLong(this.offset + 1, target, value);
            }
            Long old = ByteArrayUtil.getLong(this.offset + 1, target);

            if (old < value) {
                ByteArrayUtil.putLong(this.offset + 1, target, value);
                return value;
            }
            return null;
        }

        @Override
        public Long getData(final String... keys) {
            if (nullOrEmpty(keys)) {
                return null;
            }
            byte[] cache = this.cacheStore.getCache(keys[0]);
            if (cache == null || ByteArrayUtil.getByte(this.offset, cache) == NULL_FLAG) {
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

        @Override
        public Object readAll(String key) {
            return null;
        }
    }

    public static class MinLongArrayCacheWrapper extends LongArrayCacheWrapper {
        public static final CacheType TYPE = CacheType.MIN_LONG;

        public MinLongArrayCacheWrapper(CacheWrapperMeta meta) {
            super(meta);
        }

        @Override
        public Long addData(final Long value, final String... keys) {
            if (value == null || nullOrEmpty(keys)) {
                return null;
            }
            if (this.cacheStore.getCache(keys[0]) == null) {
                this.cacheStore.allocate(keys[0]);
            }

            byte[] target = this.cacheStore.getCache(keys[0]);
            if (ByteArrayUtil.getByte(this.offset, target) == NULL_FLAG) {
                ByteArrayUtil.putByte(this.offset, target, VALUE_FLAG);
                return ByteArrayUtil.putLong(this.offset + 1, target, value);
            }
            long old = ByteArrayUtil.getLong(this.offset + 1, target);

            if (old > value) {
                ByteArrayUtil.putLong(this.offset + 1, target, value);
                return value;
            }
            return null;
        }

        @Override
        public Long getData(final String... keys) {
            if (nullOrEmpty(keys)) {
                return null;
            }
            byte[] cache = this.cacheStore.getCache(keys[0]);
            if (cache == null || ByteArrayUtil.getByte(this.offset, cache) == NULL_FLAG) {
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

        @Override
        public Object readAll(String key) {
            return null;
        }
    }

    public static class AvgLongArrayCacheWrapper extends LongArrayCacheWrapper {
        public static final CacheType TYPE = CacheType.AVG_LONG;

        public AvgLongArrayCacheWrapper(final CacheWrapperMeta meta) {
            super(meta);
        }

        @Override
        public Double addData(Long value, String... keys) {
            if (value == null || nullOrEmpty(keys)) {
                return null;
            }
            if (this.cacheStore.getCache(keys[0]) == null) {
                this.cacheStore.allocate(keys[0]);
            }
            byte[] target = this.cacheStore.getCache(keys[0]);
            int count = ByteArrayUtil.getInt(this.offset, target);
            long sum = ByteArrayUtil.getLong(this.offset + 4, target);

            sum += value;
            count++;
            ByteArrayUtil.putInt(this.offset, target, count);
            ByteArrayUtil.putLong(this.offset + 4, target, sum);

            return sum * 1.0 / count;
        }

        @Override
        public Double getData(String... keys) {
            if (nullOrEmpty(keys)) {
                return null;
            }
            byte[] target = this.cacheStore.getCache(keys[0]);
            if (target == null) {
                return null;
            }
            int count = ByteArrayUtil.getInt(this.offset, target);
            if (count == 0) {
                return null;
            }
            long sum = ByteArrayUtil.getLong(this.offset + 4, target);
            return sum * 1.0 / count;
        }

        @Override
        public int getCacheSize() {
            return 12;
        }

        @Override
        public PrimaryData merge(PrimaryData lastMerged, String... keys) {
            return null;
        }

        @Override
        public Object readAll(String key) {
            return null;
        }
    }

    public static class StddevLongArrayCacheWrapper extends LongArrayCacheWrapper {
        public static final CacheType TYPE = CacheType.STDDEV_LONG;

        public StddevLongArrayCacheWrapper(CacheWrapperMeta meta) {
            super(meta);
        }

        @Override
        public Double addData(Long value, String... keys) {
            if (value == null || nullOrEmpty(keys)) {
                return null;
            }
            if (this.cacheStore.getCache(keys[0]) == null) {
                this.cacheStore.allocate(keys[0]);
            }

            byte[] target = this.cacheStore.getCache(keys[0]);
            int count = ByteArrayUtil.getInt(this.offset, target);
            long sum = ByteArrayUtil.getLong(this.offset + 4, target);
            long squareSum = ByteArrayUtil.getLong(this.offset + 12, target);

            sum = sum + value;
            count += 1;
            squareSum += value * value;
            ByteArrayUtil.putInt(this.offset, target, count);
            ByteArrayUtil.putLong(this.offset + 4, target, sum);
            ByteArrayUtil.putLong(this.offset + 12, target, squareSum);
            if (count <= 1) {
                return 0.0;
            } else {
                double currentAvg = sum * 1.0 / count;
                return (squareSum - sum * currentAvg) / (count - 1);
            }
        }

        @Override
        public Double getData(String... keys) {
            if (nullOrEmpty(keys)) {
                return null;
            }
            byte[] target = this.cacheStore.getCache(keys[0]);
            if (target == null) {
                return null;
            }

            return StddevLongArrayCacheWrapper.getData(target, this.offset);
        }

        public static Double getData(byte[] target, int targetOffset) {
            int count = ByteArrayUtil.getInt(targetOffset, target);
            if (count <= 1) {
                return 0.0;
            }
            long sum = ByteArrayUtil.getLong(targetOffset + 4, target);
            double avg = sum * 1.0 / count;
            return (ByteArrayUtil.getLong(targetOffset + 12, target) - sum * avg) / (count - 1);
        }

        @Override
        public int getCacheSize() {
            return 20;
        }

        @Override
        public PrimaryData merge(PrimaryData lastMerged, String... keys) {
            if (nullOrEmpty(keys)) {
                return lastMerged;
            }
            byte[] currentData = this.cacheStore.getCache(keys[0]);
            if (currentData == null) {
                return lastMerged;
            }
            return StddevLongArrayCacheWrapper.merge(lastMerged, currentData, this.offset);
        }

        @Override
        public Object readAll(String key) {
            return null;
        }

        public static PrimaryData merge(PrimaryData lastMerged, byte[] currentTarget, int currentOffset) {
            int currentCount = ByteArrayUtil.getInt(currentOffset, currentTarget);
            if (currentCount == 0) {
                return lastMerged;
            }

            PrimaryData merged;
            if (lastMerged == null) {
                merged = new PrimaryData() {
                    @Override
                    public Object getResult() {
                        return StddevLongArrayCacheWrapper.getData(this.getRawData(), 0);
                    }
                };
                merged.setRawData(new byte[20]);
            } else {
                merged = lastMerged;
            }
            byte[] data = merged.getRawData();
            ByteArrayUtil.addInt(0, data, currentCount);
            ByteArrayUtil.addLong(4, data, ByteArrayUtil.getLong(currentOffset + 4, currentTarget));
            ByteArrayUtil.addLong(12, data, ByteArrayUtil.getLong(currentOffset + 12, currentTarget));
            return merged;
        }
    }

    public static class CVLongArrayCacheWrapper extends LongArrayCacheWrapper {
        public static final CacheType TYPE = CacheType.CV_DOUBLE;

        public CVLongArrayCacheWrapper(CacheWrapperMeta meta) {
            super(meta);
        }

        @Override
        public Double addData(Long value, String... keys) {
            if (value == null || nullOrEmpty(keys)) {
                return null;
            }
            if (this.cacheStore.getCache(keys[0]) == null) {
                this.cacheStore.allocate(keys[0]);
            }

            byte[] target = this.cacheStore.getCache(keys[0]);
            int count = ByteArrayUtil.getInt(this.offset, target);
            long sum = ByteArrayUtil.getLong(this.offset + 4, target);
            long squareSum = ByteArrayUtil.getLong(this.offset + 12, target);

            sum += value;
            count += 1;
            squareSum += value * value;
            ByteArrayUtil.putInt(this.offset, target, count);
            ByteArrayUtil.putLong(this.offset + 4, target, sum);
            ByteArrayUtil.putLong(this.offset + 12, target, squareSum);
            if (count <= 1) {
                return 0.0;
            } else {
                double currentAvg = sum * 1.0 / count;
                double stddev = (squareSum - sum * currentAvg) / (count - 1);
                return Math.sqrt(stddev) / currentAvg;
            }
        }

        @Override
        public Double getData(String... keys) {
            if (nullOrEmpty(keys)) {
                return null;
            }
            byte[] target = this.cacheStore.getCache(keys[0]);
            if (target == null) {
                return null;
            }

            return CVLongArrayCacheWrapper.getData(target, this.offset);
        }

        public static Double getData(byte[] target, int targetOffset) {
            int count = ByteArrayUtil.getInt(targetOffset, target);
            if (count <= 1) {
                return 0.0;
            }
            long sum = ByteArrayUtil.getLong(targetOffset + 4, target);
            double avg = sum * 1.0 / count;
            double stddev = (ByteArrayUtil.getLong(targetOffset + 12, target) - sum * avg) / (count - 1);
            return Math.sqrt(stddev) / avg;
        }

        @Override
        public int getCacheSize() {
            return 20;
        }

        @Override
        public PrimaryData merge(PrimaryData lastMerged, String... keys) {
            if (nullOrEmpty(keys)) {
                return lastMerged;
            }
            byte[] currentData = this.cacheStore.getCache(keys[0]);
            if (currentData == null) {
                return lastMerged;
            }
            return CVLongArrayCacheWrapper.merge(lastMerged, currentData, this.offset);
        }

        @Override
        public Object readAll(String key) {
            return null;
        }

        public static PrimaryData merge(PrimaryData lastMerged, byte[] currentTarget, int currentOffset) {
            int currentCount = ByteArrayUtil.getInt(currentOffset, currentTarget);
            if (currentCount == 0) {
                return lastMerged;
            }

            PrimaryData merged;
            if (lastMerged == null) {
                merged = new PrimaryData() {
                    @Override
                    public Object getResult() {
                        return CVLongArrayCacheWrapper.getData(this.getRawData(), 0);
                    }
                };
                merged.setRawData(new byte[20]);
            } else {
                merged = lastMerged;
            }
            byte[] data = merged.getRawData();
            ByteArrayUtil.addInt(0, data, currentCount);
            ByteArrayUtil.addLong(4, data, ByteArrayUtil.getLong(currentOffset + 4, currentTarget));
            ByteArrayUtil.addLong(12, data, ByteArrayUtil.getLong(currentOffset + 12, currentTarget));

            return merged;
        }
    }
}

