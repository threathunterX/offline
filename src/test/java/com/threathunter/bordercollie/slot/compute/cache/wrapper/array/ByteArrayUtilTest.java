package com.threathunter.bordercollie.slot.compute.cache.wrapper.array;

import com.google.common.primitives.Ints;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 
 */
@Slf4j
public class ByteArrayUtilTest {

    @Test
    public void testInt32() {
        byte[] target = new byte[17];
        ByteArrayUtil.addInt(7, target, -1);
        int anInt = ByteArrayUtil.getInt(7, target);
        assertThat(anInt).isSameAs(-1);
        ByteArrayUtil.addInt(7, target, 35);
        int an7 = ByteArrayUtil.getInt(7, target);
        assertThat(an7).isSameAs(34);
        //Don't do the overflow test
        /*ByteArrayUtil.addInt(7, target, Integer.MAX_VALUE);
        int maxValue = ByteArrayUtil.getInt(7, target);
        assertThat(maxValue).isSameAs(Integer.MAX_VALUE);
        ByteArrayUtil.addInt(7, target, Integer.MIN_VALUE);
        int minValue = ByteArrayUtil.getInt(7, target);
        assertThat(minValue).isSameAs(Integer.MIN_VALUE);*/
    }

    @Test
    public void testByte() {
        byte[] target = new byte[8];
        Assert.assertEquals(0, ByteArrayUtil.getByte(3, target));
        Assert.assertEquals(1, ByteArrayUtil.putByte(3, target, (byte) 1));
        Assert.assertEquals(1, ByteArrayUtil.getByte(3, target));
        Assert.assertEquals(3, ByteArrayUtil.putByte(3, target, (byte) 3));
        Assert.assertEquals(0, ByteArrayUtil.putByte(3, target, (byte) 0));
        Assert.assertEquals(0, ByteArrayUtil.getByte(3, target));
    }

    @Test
    public void testInt() {
        byte[] target = new byte[8];
        Assert.assertEquals(5, ByteArrayUtil.addInt(4, target, 5));
        Assert.assertEquals(5, ByteArrayUtil.getInt(4, target));

        Assert.assertEquals(6, ByteArrayUtil.addInt(4, target, 1));

        byte[] raw = new byte[4];
        System.arraycopy(target, 4, raw, 0, 4);
        Assert.assertEquals(6, Ints.fromByteArray(raw));
    }

    @Test
    public void testPerformance() {
        byte[] target = new byte[8];
        int count = 100000000;
        long current = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            ByteArrayUtil.addInt(4, target, 5);
        }
        System.out.println(System.currentTimeMillis() - current);

        current = System.currentTimeMillis();
        ByteBuffer buffer = ByteBuffer.wrap(new byte[8]);
        for (int i = 0; i < count; i++) {
            int result = buffer.getInt(4);
            buffer.putInt(4, 5 + result);
        }
        System.out.println(System.currentTimeMillis() - current);
        Assert.assertEquals(5 * count, ByteArrayUtil.getInt(4, target));
        buffer.position(4);
        Assert.assertEquals(5 * count, buffer.getInt(4));
        System.out.println(Arrays.toString(buffer.array()));
    }

    @Test
    public void testLong() {
        byte[] target = new byte[8];
        Assert.assertEquals(1000l, ByteArrayUtil.putLong(0, target, 1000l));
        Assert.assertEquals(1000l, ByteArrayUtil.getLong(0, target));
        Assert.assertEquals(2000l, ByteArrayUtil.putLong(0, target, 2000l));
    }

    @Test
    public void testDouble() {
        byte[] target = new byte[12];
        Assert.assertEquals(19.8, ByteArrayUtil.putDouble(4, target, 19.8), 0.0);
        Assert.assertEquals(19.8, ByteArrayUtil.getDouble(4, target), 0.0);
        Assert.assertEquals(29.83, ByteArrayUtil.addDouble(4, target, 10.03), 0.0);
        Assert.assertEquals(20.0, ByteArrayUtil.putDouble(4, target, 20.0), 0.0);
    }

    @Test
    public void testSearchAndInsertInt32() {
        byte[] target = new byte[84];
        Assert.assertEquals(-4, ByteArrayUtil.binarySearchInt32(target, 4, 0, 4, -1));
        ByteArrayUtil.insertInt32(target, 4, 4, 0, 4, -1);
        log.info("target : {}", target);
        Assert.assertEquals(-1, ByteArrayUtil.getInt(4, target));

        Assert.assertEquals(-8, ByteArrayUtil.binarySearchInt32(target, 4, 1, 4, 22345));
        ByteArrayUtil.insertInt32(target, 4, 8, 1, 4, 22345);
        Assert.assertEquals(22345, ByteArrayUtil.getInt(8, target));

        Assert.assertEquals(-8, ByteArrayUtil.binarySearchInt32(target, 4, 2, 4, 20345));
        ByteArrayUtil.insertInt32(target, 4, 8, 2, 4, 20345);
        Assert.assertEquals(20345, ByteArrayUtil.getInt(8, target));
        Assert.assertEquals(22345, ByteArrayUtil.getInt(12, target));

        Assert.assertEquals(-4, ByteArrayUtil.binarySearchInt32(target, 4, 3, 4, -9));
        Assert.assertEquals(-16, ByteArrayUtil.binarySearchInt32(target, 4, 3, 4, 111111));
        Assert.assertEquals(8, ByteArrayUtil.binarySearchInt32(target, 4, 3, 4, 20345));
        Assert.assertEquals(-8, ByteArrayUtil.binarySearchInt32(target, 4, 3, 4, 0));
    }

    @Test
    public void testInsertInt32() {
        byte[] target = new byte[100];
        ByteArrayUtil.insertInt32(target, 20, 20, 0, 5, -232324);
        Assert.assertEquals(-232324, ByteArrayUtil.getInt(20, target));

        ByteArrayUtil.insertInt32(target, 20, 25, 1, 5, 123);
        Assert.assertEquals(-232324, ByteArrayUtil.getInt(20, target));
        Assert.assertEquals(123, ByteArrayUtil.getInt(25, target));

        ByteArrayUtil.insertInt32(target, 20, 25, 2, 5, 0);
        Assert.assertEquals(-232324, ByteArrayUtil.getInt(20, target));
        Assert.assertEquals(0, ByteArrayUtil.getInt(25, target));
        Assert.assertEquals(123, ByteArrayUtil.getInt(30, target));

        ByteArrayUtil.insertInt32(target, 20, 30, 3, 5, 11);
        Assert.assertEquals(-232324, ByteArrayUtil.getInt(20, target));
        Assert.assertEquals(0, ByteArrayUtil.getInt(25, target));
        Assert.assertEquals(11, ByteArrayUtil.getInt(30, target));
        Assert.assertEquals(123, ByteArrayUtil.getInt(35, target));

        ByteArrayUtil.insertInt32(target, 20, 20, 4, 5, -2323220);
        Assert.assertEquals(-2323220, ByteArrayUtil.getInt(20, target));
        Assert.assertEquals(-232324, ByteArrayUtil.getInt(25, target));
        Assert.assertEquals(0, ByteArrayUtil.getInt(30, target));
        Assert.assertEquals(11, ByteArrayUtil.getInt(35, target));
        Assert.assertEquals(123, ByteArrayUtil.getInt(40, target));
    }

    @Test
    public void testBinarySearch() {
        int[] target = new int[1];
        System.out.println(Arrays.binarySearch(target, 1));
    }
}
