package com.threathunter.bordercollie.slot.api;

import java.util.Map;

/**
 * Created by toyld on 3/1/17.
 */
public interface DataConsumer {
    void start();
    void store(Map mapData);
    void stop();
}
