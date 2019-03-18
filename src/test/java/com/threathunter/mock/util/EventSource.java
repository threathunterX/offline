package com.threathunter.mock.util;

import com.threathunter.model.Event;

/**
 * 
 */
public interface EventSource {
    Event nextEvent();

    void close();
}
