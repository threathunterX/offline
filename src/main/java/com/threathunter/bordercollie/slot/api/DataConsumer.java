package com.threathunter.bordercollie.slot.api;

import java.util.Map;

/**
 * 
 */
public interface DataConsumer {
    void start();
    void store(Map mapData);
    void stop();
}
