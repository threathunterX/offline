package com.threathunter.mock.util;

import com.threathunter.bordercollie.slot.util.LimitMaxPriorityQueue;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

/**
 * Created by daisy on 17-11-18
 */
public class LimitMaxPriorityQueueTest {
    @Test
    public void testQueue() {
        LimitMaxPriorityQueue queue = new LimitMaxPriorityQueue(100);

        Map<Integer, String> map = new HashMap<>();
        Random random = new Random();
        for (int i = 0; i < 100; i++) {
            Integer in = random.nextInt(10000);
            while (map.containsKey(in)) {
                in = random.nextInt(10000);
            }
            map.put(in, "key_" + i);
            queue.update("key_" + i, in);
        }

        List<Integer> list = new ArrayList<>(map.keySet());
        list.sort((in1, in2) -> in2 - in1);
        Assert.assertEquals(100, queue.getSize());

        List<Map<String, Object>> queueList = queue.getCopy();
        Map<String, Object> last = queueList.get(0);
        Assert.assertEquals(map.get(last.get("value")), last.get("key"));
        for (int i = 1; i < queueList.size(); i++) {
            Map<String, Object> data = queueList.get(i);
            Assert.assertTrue((Integer) last.get("value") > (Integer) data.get("value"));
            Assert.assertEquals(map.get(data.get("value")), data.get("key"));
            last = data;
        }
    }
}
