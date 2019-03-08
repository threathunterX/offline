package com.threathunter.bordercollie.slot.compute.cache.wrapper.array;

import com.threathunter.babel.rpc.HashUtils;
import com.google.common.hash.Hashing;
import net.agkn.hll.HLL;
import net.agkn.hll.HLLType;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.charset.Charset;
import java.util.List;

/**
 * Created by daisy on 17-11-18
 */
public class LimitHashArraySetTest {
    @Test
    public void testHash() {
        LimitHashArraySet hashArraySet = new LimitHashArraySet(30);
        int totalByteSize = hashArraySet.getTotalBytesSize();
        System.out.println(totalByteSize);
        byte[] target = new byte[totalByteSize * 2];
        int dataOffset = totalByteSize;

        int[] position = new int[40];
        for (int i = 0; i < 40; i++) {
            int pos = hashArraySet.add(i + "", target, dataOffset);
            System.out.println(pos);
            position[i] = pos;
            if (pos > 0) {
                Assert.assertTrue(hashArraySet.contains(i + "", target, dataOffset));
            }
            Assert.assertEquals(pos, hashArraySet.getPosition(i + "", target, dataOffset));
        }

        int size = 0;
        for (int i = 0; i < 40; i++) {
            if (position[i] < 0) {
                Assert.assertFalse(hashArraySet.contains(i + "", target, dataOffset));
            } else {
                size++;
            }
            Assert.assertEquals(position[i], hashArraySet.getPosition(i + "", target, dataOffset));
        }
        Assert.assertEquals(size, hashArraySet.getSize(target, dataOffset));
        Assert.assertEquals(size, 30);
    }

    @Test
    public void testPerformance() {
        int max = 50;
        LimitHashArraySet hashArraySet = new LimitHashArraySet(max, 4, 0.75f, 3);
        int total = hashArraySet.getTotalBytesSize();
        byte[] target = new byte[total];
        int dataOffset = 0;

        long start = System.currentTimeMillis();
        for (int i = 0; i < 10000; i++) {
            String ind = i % max + "";
//            int hash = Hashing.murmur3_32().hashString(ind + "", Charset.defaultCharset()).asInt();
            int hash = ind.hashCode();
//            int ind = i % max;
//            int hash = Hashing.murmur3_32().hashString(ind + "", Charset.defaultCharset()).asInt();
            hashArraySet.add(hash, target, dataOffset);
            hashArraySet.getSize(target, dataOffset);
        }
        System.out.println(System.currentTimeMillis() - start);
        Assert.assertEquals(max, hashArraySet.getSize(target, dataOffset));
    }

    @Test
    public void testHLLPerformance() {
        int max = 50;
        HLL hll = new HLL(13, 5, 0, true, HLLType.SPARSE);

        long start = System.currentTimeMillis();
        for (int i = 0; i < 10000000; i++) {
            String ind = i % max + "";
            int hash = Hashing.murmur3_32().hashString(ind + "", Charset.defaultCharset()).asInt();
//            int hash = ind.hashCode();
//            System.out.println(hash);
            hll.addRaw(hash);
        }
        System.out.println(System.currentTimeMillis() - start);
        Assert.assertEquals(max, hll.cardinality(), 1);
    }

    /**
     * Test if merge is correct
     */
    @Test
    public void testMergeNoConflict() {
        int max = 50;
        LimitHashArraySet hashArraySet = new LimitHashArraySet(max, 4, 0.75f, 3);

        byte[] target1 = new byte[hashArraySet.getTotalBytesSize()];
        byte[] target2 = new byte[hashArraySet.getTotalBytesSize()];

        for (int i = 0; i < 20; i++) {
            hashArraySet.add((i + "").hashCode(), target1, 0);
        }
        int count1 = hashArraySet.getSize(target1, 0);
        System.out.println(count1);
        Assert.assertEquals(20, count1, 2);

        for (int i = 20; i < 40; i++) {
            hashArraySet.add((i + "").hashCode(), target2, 0);
        }
        int count2 = hashArraySet.getSize(target2, 0);
        System.out.println(count2);
        Assert.assertEquals(20, count2, 2);

        List<Integer> ignored = hashArraySet.merge(target2, 0, target1, 0);
        System.out.println(hashArraySet.getSize(target1, 0));
        Assert.assertEquals(40, hashArraySet.getSize(target1, 0), 2);
        Assert.assertEquals(0, ignored.size(), 2);
    }

    @Test
    public void testMergeSomeConflict() {
        int max = 50;
        LimitHashArraySet hashArraySet = new LimitHashArraySet(max, 4, 0.75f, 3);

        byte[] target1 = new byte[hashArraySet.getTotalBytesSize()];
        byte[] target2 = new byte[hashArraySet.getTotalBytesSize()];

        for (int i = 0; i < 20; i++) {
            hashArraySet.add((i + "").hashCode(), target1, 0);
        }
        int count1 = hashArraySet.getSize(target1, 0);
        System.out.println(count1);
        Assert.assertEquals(20, count1, 2);

        for (int i = 10; i < 30; i++) {
            hashArraySet.add((i + "").hashCode(), target2, 0);
        }
        int count2 = hashArraySet.getSize(target2, 0);
        System.out.println(count2);
        Assert.assertEquals(20, count2, 2);

        List<Integer> ignored = hashArraySet.merge(target2, 0, target1, 0);
        System.out.println(hashArraySet.getSize(target1, 0));
        Assert.assertEquals(30, hashArraySet.getSize(target1, 0), 2);
        Assert.assertEquals(0, ignored.size(), 2);
    }

    @Test
    public void testGetAll() {
        int max = 20;
        LimitHashArraySet hashArraySet = new LimitHashArraySet(max, 8, 3);
        byte[] target = new byte[22 + hashArraySet.getTotalBytesSize()];

        hashArraySet.add(HashUtils.getHash("taobao"), target, 22);
        hashArraySet.add(HashUtils.getHash("tengxun"), target, 22);

        Assert.assertEquals(2, hashArraySet.getSize(target, 22));
        Assert.assertEquals(2, hashArraySet.getAll(target, 22).size());
    }

    @Ignore
    @Test
    public void testTablesize() {
        int len = 1024 * 64;
        for (int i = 1; i < len + 13; i++) {
            System.out.println(String.format("i = %d\ttable_size=%d", i, LimitHashArraySet.tableSizeFor(i)));
        }
    }
}
