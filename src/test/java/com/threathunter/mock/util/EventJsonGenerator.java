package com.threathunter.mock.util;

import com.threathunter.model.Event;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;

/**
 * Created by daisy on 17/6/29.
 */
public class EventJsonGenerator {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventJsonGenerator.class);

    public static boolean generateEvents(int ipCount, int sum, String persistPath) {
        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new FileWriter(persistPath));
        } catch (Exception e) {
            LOGGER.error("initial file writer error", writer);
            return false;
        }

        Gson gson = new Gson();
        HttpDynamicEventMaker eventMaker = new HttpDynamicEventMaker(ipCount);
        int generateSum = sum;
        while (generateSum > 0) {
            Event event = eventMaker.nextEvent();
            try {
                writer.write(gson.toJson(event));
                writer.write("\n");
            } catch (Exception e) {
                if (generateSum % 100 == 0) {
                    LOGGER.error("write event json error", e);
                }
            }
            generateSum--;
        }

        try {
            writer.close();
        } catch (Exception e) {
            LOGGER.error("close writer error", e);
        }

        return true;
    }
}
