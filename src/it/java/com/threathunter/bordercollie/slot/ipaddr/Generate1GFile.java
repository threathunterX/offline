package com.threathunter.bordercollie.slot.ipaddr;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author Yuan Yi <yi.yuan@threathunter.cn>
 * @since: 2.16
 */
@Slf4j
public class Generate1GFile {
    public static ByteBuffer ds = null;
    public static Long M1Size = 1024 * 1024L;
    public static Long K1Size = 1024L;

    static {
        try {
            ds = ByteBuffer.wrap(getDefaultSeparator());
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    public static byte[] getDefaultSeparator() throws UnsupportedEncodingException {
        return File.separator.getBytes("utf-8");
    }

    @Test
    public void generator1GIpFile() throws IOException {
        RandomAccessFile memoryFile = new RandomAccessFile("t", "rw");
        memoryFile.setLength(1024 * 1024 * 1024);
        FileChannel channel = memoryFile.getChannel();
        MappedByteBuffer mappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, M1Size * 1024);
        long start = System.currentTimeMillis();
        boolean first = false;
        int count = 0;
        long pre = 0;
        long cur = 0;
        while (mappedByteBuffer.remaining() > 0) {
            cur = mappedByteBuffer.position() / K1Size;
            if (cur != pre) {
                pre = cur;
                long end = System.currentTimeMillis();
                log.info("size:{}K,time:{}", ++count, end - start);

            }
            mappedByteBuffer.put(ByteBuffer.wrap(getIpByte()));
            mappedByteBuffer.put(ds.asReadOnlyBuffer());
        }

    }

    public byte[] getIpByte() throws UnsupportedEncodingException {
        return getRandomChinaIp().getBytes("utf-8");
    }

    public String getRandomChinaIp() {
        return IPAddressGenerator.generateIPAddress(Country.CHINA);
    }
}
