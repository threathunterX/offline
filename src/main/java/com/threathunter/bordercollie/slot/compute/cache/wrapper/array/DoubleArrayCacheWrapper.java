package com.threathunter.bordercollie.slot.compute.cache.wrapper.array;

import com.threathunter.bordercollie.slot.compute.cache.CacheType;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapperMeta;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.PrimaryData;

/**
 * 
 */
public abstract class DoubleArrayCacheWrapper extends ByteArrayCacheWrapper<Double, Double> {

    protected static final byte NULL_FLAG = 0;
    protected static final byte VALUE_FLAG = 1;

    public DoubleArrayCacheWrapper(final CacheWrapperMeta meta) {
        super(meta);
    }

    protected boolean nullOrEmpty(final String... keys) {
        if (keys == null || keys.length < 1) {
            return true;
        }
        return false;
    }

    // TODO test
    public static class SumDoubleArrayCacheWrapper extends DoubleArrayCacheWrapper {
        public static final CacheType TYPE = CacheType.SUM_DOUBLE;

        public SumDoubleArrayCacheWrapper(CacheWrapperMeta meta) {
            super(meta);
        }

        @Override
        public Double addData(Double value, String... keys) {
            if (value == null || nullOrEmpty(keys)) {
                return null;
            }
            if (this.cacheStore.getCache(keys[0]) == null) {
                this.cacheStore.allocate(keys[0]);
            }

            byte[] target = this.cacheStore.getCache(keys[0]);
            if (ByteArrayUtil.getByte(this.offset, target) == NULL_FLAG) {
                ByteArrayUtil.putByte(this.offset, target, VALUE_FLAG);
                return ByteArrayUtil.putDouble(this.offset + 1, target, value);
            }

            return ByteArrayUtil.addDouble(this.offset + 1, target, value);
        }

        @Override
        public Double getData(String... keys) {
            if (nullOrEmpty(keys)) {
                return null;
            }
            byte[] cache = this.cacheStore.getCache(keys[0]);
            if (cache == null || ByteArrayUtil.getByte(this.offset, cache) == NULL_FLAG) {
                return null;
            }

            return ByteArrayUtil.getDouble(this.offset + 1, cache);
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
        public Object readAll(final String key) {
            return null;
        }
    }

    public static class FirstDoubleArrayCacheWrapper extends DoubleArrayCacheWrapper {
        public static final CacheType TYPE = CacheType.FIRST_DOUBLE;

        public FirstDoubleArrayCacheWrapper(final CacheWrapperMeta meta) {
            super(meta);
        }


        @Override
        public Double addData(final Double value, final String... keys) {
            if (value == null || nullOrEmpty(keys)) {
                return null;
            }
            if (this.cacheStore.getCache(keys[0]) == null) {
                this.cacheStore.allocate(keys[0]);

                byte[] target = this.cacheStore.getCache(keys[0]);
                // flag set to 1 if there is any value set
                ByteArrayUtil.putByte(this.offset, target, VALUE_FLAG);
                return ByteArrayUtil.putDouble(this.offset + 1, target, value);
            }

            return null;
        }

        @Override
        public Double getData(final String... keys) {
            if (nullOrEmpty(keys)) {
                return null;
            }
            byte[] cache = this.cacheStore.getCache(keys[0]);
            if (cache == null || ByteArrayUtil.getByte(this.offset, cache) == NULL_FLAG) {
                return null;
            }

            return ByteArrayUtil.getDouble(this.offset + 1, cache);
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

    public static class LastDoubleArrayCacheWrapper extends DoubleArrayCacheWrapper {
        public static final CacheType TYPE = CacheType.LAST_DOUBLE;

        public LastDoubleArrayCacheWrapper(final CacheWrapperMeta meta) {
            super(meta);
        }

        @Override
        public Double addData(final Double value, final String... keys) {
            if (value == null || nullOrEmpty(keys)) {
                return null;
            }
            if (this.cacheStore.getCache(keys[0]) == null) {
                this.cacheStore.allocate(keys[0]);
            }

            byte[] target = this.cacheStore.getCache(keys[0]);
            ByteArrayUtil.putByte(this.offset, target, VALUE_FLAG);
            return ByteArrayUtil.putDouble(this.offset + 1, target, value);
        }

        @Override
        public Double getData(final String... keys) {
            if (nullOrEmpty(keys)) {
                return null;
            }
            byte[] cache = this.cacheStore.getCache(keys[0]);
            if (cache == null || ByteArrayUtil.getByte(this.offset, cache) == NULL_FLAG) {
                return null;
            }

            return ByteArrayUtil.getDouble(this.offset + 1, cache);
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

    public static class AvgDoubleArrayCacheWrapper extends DoubleArrayCacheWrapper {
        public static final CacheType TYPE = CacheType.AVG_DOUBLE;

        public AvgDoubleArrayCacheWrapper(final CacheWrapperMeta meta) {
            super(meta);
        }


        @Override
        public Double addData(final Double value, final String... keys) {
            if (value == null || nullOrEmpty(keys)) {
                return null;
            }
            if (this.cacheStore.getCache(keys[0]) == null) {
                this.cacheStore.allocate(keys[0]);
            }

            byte[] target = this.cacheStore.getCache(keys[0]);
            int count = ByteArrayUtil.getInt(this.offset, target);
            double sum = ByteArrayUtil.getDouble(this.offset + 4, target);

            sum = sum + value;
            count += 1;
            ByteArrayUtil.putInt(this.offset, target, count);
            ByteArrayUtil.putDouble(this.offset + 4, target, sum);

            return sum / count;
        }

        @Override
        public Double getData(final String... keys) {
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
            double sum = ByteArrayUtil.getDouble(this.offset + 4, target);
            return sum / count;
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

    public static class MaxDoubleArrayCacheWrapper extends DoubleArrayCacheWrapper {
        public static final CacheType TYPE = CacheType.MAX_DOUBLE;

        public MaxDoubleArrayCacheWrapper(final CacheWrapperMeta meta) {
            super(meta);
        }

        @Override
        public Double addData(final Double value, final String... keys) {
            if (value == null || nullOrEmpty(keys)) {
                return null;
            }
            if (this.cacheStore.getCache(keys[0]) == null) {
                this.cacheStore.allocate(keys[0]);
            }

            byte[] target = this.cacheStore.getCache(keys[0]);
            if (ByteArrayUtil.getByte(this.offset, target) == NULL_FLAG) {
                ByteArrayUtil.putByte(this.offset, target, VALUE_FLAG);
                return ByteArrayUtil.putDouble(this.offset + 1, target, value);
            }
            Double old = ByteArrayUtil.getDouble(this.offset + 1, target);
            if (old < value) {
                ByteArrayUtil.putDouble(this.offset + 1, target, value);
                return value;
            }
            return null;
        }

        @Override
        public Double getData(final String... keys) {
            if (nullOrEmpty(keys)) {
                return null;
            }
            byte[] cache = this.cacheStore.getCache(keys[0]);
            if (cache == null || ByteArrayUtil.getByte(this.offset, cache) == NULL_FLAG) {
                return null;
            }

            return ByteArrayUtil.getDouble(this.offset + 1, cache);
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

    public static class MinDoubleArrayCacheWrapper extends DoubleArrayCacheWrapper {
        public static final CacheType TYPE = CacheType.MIN_DOUBLE;

        public MinDoubleArrayCacheWrapper(final CacheWrapperMeta meta) {
            super(meta);
        }


        @Override
        public Double addData(final Double value, final String... keys) {
            if (value == null || nullOrEmpty(keys)) {
                return null;
            }
            if (this.cacheStore.getCache(keys[0]) == null) {
                this.cacheStore.allocate(keys[0]);
            }

            byte[] target = this.cacheStore.getCache(keys[0]);
            if (ByteArrayUtil.getByte(this.offset, target) == NULL_FLAG) {
                ByteArrayUtil.putByte(this.offset, target, VALUE_FLAG);
                return ByteArrayUtil.putDouble(this.offset + 1, target, value);
            }
            Double old = ByteArrayUtil.getDouble(this.offset + 1, target);

            if (old > value) {
                ByteArrayUtil.putDouble(this.offset + 1, target, value);
                return value;
            }
            return null;
        }

        @Override
        public Double getData(final String... keys) {
            if (nullOrEmpty(keys)) {
                return null;
            }
            byte[] cache = this.cacheStore.getCache(keys[0]);
            if (cache == null || ByteArrayUtil.getByte(this.offset, cache) == NULL_FLAG) {
                return null;
            }

            return ByteArrayUtil.getDouble(this.offset + 1, cache);
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

    public static class StddevDoubleArrayCacheWrapper extends DoubleArrayCacheWrapper {
        public static final CacheType TYPE = CacheType.STDDEV_DOUBLE;

        public StddevDoubleArrayCacheWrapper(CacheWrapperMeta meta) {
            super(meta);
        }

        @Override
        public Double addData(Double value, String... keys) {
            if (value == null || nullOrEmpty(keys)) {
                return null;
            }
            if (this.cacheStore.getCache(keys[0]) == null) {
                this.cacheStore.allocate(keys[0]);
            }

            byte[] target = this.cacheStore.getCache(keys[0]);
            int count = ByteArrayUtil.getInt(this.offset, target);
            double sum = ByteArrayUtil.getDouble(this.offset + 4, target);
            double squareSum = ByteArrayUtil.getDouble(this.offset + 12, target);

            sum = sum + value;
            count += 1;
            squareSum += value * value;
            ByteArrayUtil.putInt(this.offset, target, count);
            ByteArrayUtil.putDouble(this.offset + 4, target, sum);
            ByteArrayUtil.putDouble(this.offset + 12, target, squareSum);
            if (count <= 1) {
                return 0.0;
            } else {
                double currentAvg = sum / count;
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

            return this.getData(target, this.offset);
        }

        public Double getData(byte[] target, int targetOffset) {
            int count = ByteArrayUtil.getInt(targetOffset, target);
            if (count <= 1) {
                return 0.0;
            }
            double sum = ByteArrayUtil.getDouble(targetOffset + 4, target);
            double avg = sum / count;
            return (ByteArrayUtil.getDouble(targetOffset + 12, target) - sum * avg) / (count - 1);
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
            return this.merge(lastMerged, currentData, this.offset);
        }

        @Override
        public Object readAll(String key) {
            return null;
        }

        public PrimaryData merge(PrimaryData lastMerged, byte[] currentTarget, int currentOffset) {
            int currentCount = ByteArrayUtil.getInt(currentOffset, currentTarget);
            if (currentCount == 0) {
                return lastMerged;
            }

            PrimaryData merged;
            if (lastMerged == null) {
                merged = new PrimaryData() {
                    @Override
                    public Object getResult() {
                        return getData(this.getRawData(), 0);
                    }
                };
                merged.setRawData(new byte[20]);
            } else {
                merged = lastMerged;
            }
            byte[] data = merged.getRawData();
            ByteArrayUtil.addInt(0, data, currentCount);
            ByteArrayUtil.addDouble(4, data, ByteArrayUtil.getDouble(currentOffset + 4, currentTarget));
            ByteArrayUtil.addDouble(12, data, ByteArrayUtil.getDouble(currentOffset + 12, currentTarget));

            return merged;
        }
    }

    public static class CVDoubleArrayCacheWrapper extends DoubleArrayCacheWrapper {
        public static final CacheType TYPE = CacheType.CV_DOUBLE;

        public CVDoubleArrayCacheWrapper(CacheWrapperMeta meta) {
            super(meta);
        }

        @Override
        public Double addData(Double value, String... keys) {
            if (value == null || nullOrEmpty(keys)) {
                return null;
            }
            if (this.cacheStore.getCache(keys[0]) == null) {
                this.cacheStore.allocate(keys[0]);
            }

            byte[] target = this.cacheStore.getCache(keys[0]);
            int count = ByteArrayUtil.getInt(this.offset, target);
            double sum = ByteArrayUtil.getDouble(this.offset + 4, target);
            double squareSum = ByteArrayUtil.getDouble(this.offset + 12, target);

            sum += value;
            count += 1;
            squareSum += value * value;
            ByteArrayUtil.putInt(this.offset, target, count);
            ByteArrayUtil.putDouble(this.offset + 4, target, sum);
            ByteArrayUtil.putDouble(this.offset + 12, target, squareSum);
            if (count <= 1) {
                return 0.0;
            } else {
                double currentAvg = sum / count;
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
            return this.getData(target, this.offset);
        }

        public Double getData(byte[] target, int targetOffset) {
            int count = ByteArrayUtil.getInt(targetOffset, target);
            if (count <= 1) {
                return 0.0;
            }
            double sum = ByteArrayUtil.getDouble(targetOffset + 4, target);
            double avg = sum / count;
            double stddev = (ByteArrayUtil.getDouble(targetOffset + 12, target) - sum * avg) / (count - 1);
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
            return this.merge(lastMerged, currentData, this.offset);
        }

        @Override
        public Object readAll(String key) {
            return null;
        }

        public PrimaryData merge(PrimaryData lastMerged, byte[] currentTarget, int currentOffset) {
            int currentCount = ByteArrayUtil.getInt(currentOffset, currentTarget);
            if (currentCount == 0) {
                return lastMerged;
            }

            PrimaryData merged;
            if (lastMerged == null) {
                merged = new PrimaryData() {
                    @Override
                    public Object getResult() {
                        return getData(this.getRawData(), 0);
                    }
                };
                merged.setRawData(new byte[getCacheSize()]);
            } else {
                merged = lastMerged;
            }
            byte[] data = merged.getRawData();
            ByteArrayUtil.addInt(0, data, currentCount);
            ByteArrayUtil.addDouble(4, data, ByteArrayUtil.getDouble(currentOffset + 4, currentTarget));
            ByteArrayUtil.addDouble(12, data, ByteArrayUtil.getDouble(currentOffset + 12, currentTarget));

            return merged;
        }
    }
}

