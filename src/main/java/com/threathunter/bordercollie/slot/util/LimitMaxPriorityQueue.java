package com.threathunter.bordercollie.slot.util;

import java.util.*;

/**
 * Created by daisy on 16/8/25.
 */
public class LimitMaxPriorityQueue {
    private final int capacity;
    private final PriorityQueue<PriorityData> maxPriorityQueue;
    private final PriorityQueue<PriorityData> minPriorityQueue;
    private final Comparator<PriorityData> maxComparator;
    private int size = 0;

    public LimitMaxPriorityQueue(final int capacity) {
        this.capacity = capacity;

        this.maxComparator = (s1, s2) -> {
            if (s1 == null || s2 == null) {
                if (s1 == null && s2 != null) {
                    return -1 * (0 - s2.value.intValue());
                }
                if (s1 != null && s2 == null) {
                    return -1 * (s1.value.intValue() - 0);
                }
                if (s1 == null && s2 == null) {
                    return 0;
                }
            }
            return -1 * (s1.value.intValue() - s2.value.intValue());
        };

        this.maxPriorityQueue = new PriorityQueue<>(capacity, maxComparator);

        this.minPriorityQueue = new PriorityQueue<>(capacity, Comparator.comparingInt(s -> s.value.intValue()));
    }

    public void update(final String key, final Number value) {
        PriorityData queryItem = new PriorityData(key, value);
        if (size >= capacity) {
            // must check if contains first, if check minimum first,
            // the queue contains the key will not be update,
            // this is the bug why the minPriorityQueue can't keep the order
            if (maxPriorityQueue.contains(queryItem)) {
                _update(new PriorityData(key, value));
            } else {
                if (value.intValue() <= minPriorityQueue.peek().value.intValue()) {
                    return;
                } else {
                    PriorityData min = minPriorityQueue.poll();
                    maxPriorityQueue.remove(min);

                    PriorityData data = new PriorityData(key, value);
                    minPriorityQueue.add(data);
                    maxPriorityQueue.add(data);
                }
            }
        } else {
            if (!maxPriorityQueue.contains(queryItem)) {
                size++;
            }
            _update(new PriorityData(key, value));
        }
    }

    public int getSize() {
        return maxPriorityQueue.size();
    }

    public PriorityData poll() {
        PriorityData max = maxPriorityQueue.poll();
        minPriorityQueue.remove(max);
        return max;
    }

    public List<Map<String, Object>> getCopy() {
        List<PriorityData> queue = new LinkedList<>(this.maxPriorityQueue);
        queue.sort(maxComparator);
        List<Map<String, Object>> result = new ArrayList<>(queue.size());
        queue.forEach(d -> {
            if (d == null) return;
            HashMap<String, Object> map = new HashMap<>(4);
            map.put("key", d.getKey());
            map.put("value", d.getValue());
            result.add(map);
        });
        return result;
    }

    private void _update(final PriorityData data) {
        maxPriorityQueue.remove(data);
        maxPriorityQueue.add(data);

        minPriorityQueue.remove(data);
        minPriorityQueue.add(data);
    }

    public class PriorityData {
        private final String key;
        private final Number value;

        public PriorityData(String key, Number value) {
            this.key = key;
            this.value = value;
        }

        public String getKey() {
            return key;
        }

        public Number getValue() {
            return value;
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof PriorityData))
                return false;
            return key.equals(((PriorityData) obj).getKey());
        }

        @Override
        public int hashCode() {
            if (key != null)
                return key.hashCode();
            else
                return 0;
        }
    }
}
