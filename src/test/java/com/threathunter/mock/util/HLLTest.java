package com.threathunter.mock.util;

import com.google.common.hash.Hashing;
import net.agkn.hll.HLL;
import net.agkn.hll.HLLType;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.Charset;

import static java.nio.charset.Charset.defaultCharset;

/**
 * Created by daisy on 17-11-22
 */
public class HLLTest {
    @Test
    public void testUnion() {
        HLL hll_from = new HLL(10, 5, 0, true, HLLType.SPARSE);
        HLL hll_to = new HLL(10, 5, 0, true, HLLType.SPARSE);
        for (int i = 0; i < 10000; i++) {
            hll_from.addRaw(Hashing.murmur3_32().hashString(i + "", Charset.defaultCharset()).asInt());
        }

        for (int i = 10000; i < 40000; i++) {
            hll_to.addRaw(Hashing.murmur3_32().hashString(i + "", Charset.defaultCharset()).asInt());
        }
        byte[] hll_from_Bytes = hll_from.toBytes();
        byte[] hll_to_bytes = hll_to.toBytes();
        System.out.println("origin serialized bytes size: " + hll_from_Bytes.length);
        System.out.println("origin to serialized bytes size: " + hll_to_bytes.length);

        int origin_from = (int) hll_from.cardinality();
        System.out.println("origin from: " + origin_from);
        int origin_to = (int) hll_to.cardinality();
        System.out.println("origin to: " + origin_to);

        hll_to.union(hll_from);
        System.out.println("merged origin from: " + hll_from.cardinality());
        System.out.println("merged origin to: " + hll_to.cardinality());

        HLL dHllFrom = HLL.fromBytes(hll_from_Bytes);
        HLL dHllTo = HLL.fromBytes(hll_to_bytes);
        System.out.println("origin from deserialize: " + dHllFrom.cardinality());
        System.out.println("origin to deserialize: " + dHllTo.cardinality());
        dHllTo.union(dHllFrom);
        System.out.println("merged deserialize from: " + dHllFrom.cardinality());
        System.out.println("merged deserialize origin to: " + dHllTo.cardinality());
    }

    @Test
    public void testSerialized() {
        HLL h1 = new HLL(9, 5, 0, false, HLLType.FULL);
        HLL h2 = new HLL(10, 5, 0, true, HLLType.FULL);
        HLL h3 = new HLL(10, 5, 1, true, HLLType.FULL);
        HLL h4 = new HLL(10, 5, 1, false, HLLType.FULL);
        int h1OriginSize = h1.toBytes().length;
        int h2OriginSize = h2.toBytes().length;
        int h3OriginSize = h3.toBytes().length;
        int h4OriginSize = h4.toBytes().length;
        System.out.println("initial size: " + h1OriginSize);
        System.out.println("initial size: " + h2OriginSize);
        System.out.println("initial size: " + h3OriginSize);
        System.out.println("initial size: " + h4OriginSize);

        int total = 10;
        for (int i = 0; i < total; i++) {
            int hash = Hashing.murmur3_32().hashString(i + "", defaultCharset()).asInt();
            h1.addRaw(hash);
            h2.addRaw(hash);
            h3.addRaw(hash);
            h4.addRaw(hash);
        }
        System.out.println("cardinality: " + h1.cardinality());
        System.out.println("cardinality: " + h2.cardinality());
        System.out.println("cardinality: " + h3.cardinality());
        System.out.println("cardinality: " + h4.cardinality());

        int h1CurrentSize = h1.toBytes().length;
        int h2CurrentSize = h2.toBytes().length;
        int h3CurrentSize = h3.toBytes().length;
        int h4CurrentSize = h4.toBytes().length;
        System.out.println("serialized size: " + h1CurrentSize);
        System.out.println("serialized size: " + h2CurrentSize);
        System.out.println("serialized size: " + h3CurrentSize);
        System.out.println("serialized size: " + h4CurrentSize);

        Assert.assertEquals(h1OriginSize, h1CurrentSize);
        Assert.assertEquals(h2OriginSize, h2CurrentSize);
        Assert.assertEquals(h3OriginSize, h3CurrentSize);
        Assert.assertEquals(h4OriginSize, h4CurrentSize);

        double errorRate = Math.abs((h1.cardinality() - total) * 1.0 / total);
        System.out.println("error: " + errorRate);

        Assert.assertEquals(0.0, errorRate, 0.05);
    }

    /**
     * HLL will hide its bytes.
     */
    @Test
    public void testProvidedBytes() {
        HLL hll1 = new HLL(9, 5, 0, false, HLLType.FULL);
        HLL hll2 = new HLL(9, 5, 0, false, HLLType.FULL);

        byte[] provided1 = hll1.toBytes();
        byte[] provided2 = hll2.toBytes();

        int count = 100;
        for (int i = 0; i < count; i++) {
            int hash = Hashing.murmur3_32().hashString(i + "", Charset.defaultCharset()).asInt();
            hll1.addRaw(hash);
        }
        Assert.assertEquals(count, hll1.cardinality(), 5);
        Assert.assertEquals(0, hll2.cardinality());

        HLL hll1Copy = HLL.fromBytes(provided1);
        HLL hll2Copy = HLL.fromBytes(provided2);
        Assert.assertNotEquals(count, hll1Copy.cardinality(), 5);
        Assert.assertEquals(0, hll2Copy.cardinality());
    }
}
