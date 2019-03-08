package com.threathunter.bordercollie.slot.util;


import com.threathunter.bordercollie.slot.compute.graph.node.CacheNode;

import java.util.*;

/**
 * Created by daisy on 17/4/14.
 */
public class LimitMaxPriorityNodeQueue {
    private int size = 0;
    private final int capacity;
    private final PrioritySearchCacheNode searchCacheNode;
    private final PriorityQueue<String> maxPriorityQueue;
    private final PriorityQueue<String> minPriorityQueue;
    private final Comparator<String> maxComparator;

    public LimitMaxPriorityNodeQueue(final int capacity, final CacheNode node, final String firstKey) {
        this.capacity = capacity;
        this.searchCacheNode = new PrioritySearchCacheNode(node, firstKey);

        this.maxComparator = (s1, s2) -> -1 * this.searchCacheNode.getData(s1).intValue() - this.searchCacheNode.getData(s2).intValue();

        this.maxPriorityQueue = new PriorityQueue<>(capacity, maxComparator);

        this.minPriorityQueue = new PriorityQueue<>(capacity, (s1, s2) -> this.searchCacheNode.getData(s1).intValue() - this.searchCacheNode.getData(s2).intValue());
    }

    public void update(final String key) {
        if (size >= capacity) {
            // must check if contains first, if check minimum first,
            // the queue contains the key will not be update,
            // this is the bug why the minPriorityQueue can't keep the order
            if (maxPriorityQueue.contains(key)) {
                _update(key);
            } else {
                if (searchCacheNode.getData(key).longValue() <= searchCacheNode.getData(minPriorityQueue.peek()).longValue()) {
                    return;
                } else {
                    String min = minPriorityQueue.poll();
                    maxPriorityQueue.remove(min);

                    minPriorityQueue.add(key);

                    maxPriorityQueue.add(key);
                }
            }
        } else {
            if (!maxPriorityQueue.contains(key)) {
                size++;
            }
            _update(key);
        }
    }

    public int getSize() {
        return maxPriorityQueue.size();
    }

    public String poll() {
        String max = maxPriorityQueue.poll();
        minPriorityQueue.remove(max);
        return max;
    }

    public List<String> getCopy() {
        List<String> queue = new LinkedList<>(this.maxPriorityQueue);
        Collections.sort(queue, maxComparator);
        return queue;
    }

    public Number getData(final String key) {
        return this.searchCacheNode.getData(key);
    }

    private void _update(final String key) {
        maxPriorityQueue.remove(key);
        maxPriorityQueue.add(key);

        minPriorityQueue.remove(key);
        minPriorityQueue.add(key);
    }

    private class PrioritySearchCacheNode {
        private final CacheNode cacheNode;
        private final String firstKey;

        public PrioritySearchCacheNode(final CacheNode node, final String firstKey) {
            this.cacheNode = node;
            this.firstKey = firstKey;
        }

        public Number getData(final String key) {
            if (firstKey != null) {
                return (Number) this.cacheNode.getData(firstKey, key);
            }
            return (Number) this.cacheNode.getData(key);
        }
    }
}
