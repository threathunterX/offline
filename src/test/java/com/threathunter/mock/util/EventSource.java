package com.threathunter.mock.util;

import com.threathunter.model.Event;

/**
 * Created by daisy on 16/11/28.
 */
public interface EventSource {
    Event nextEvent();

    void close();
}
