package com.threathunter.mock.simulator;

import com.threathunter.model.Event;
import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Yuan Yi <yi.yuan@threathunter.cn>
 * @since: 1.5
 */
@Slf4j
public class EventParser {

    public static Event getEventFromString(String eventStr) {
        Event event = EventParser.fromJsonString(eventStr);
        Map<String, Object> propertyValues = event.getPropertyValues();
        Object keys = propertyValues.get("keys");
        List<String> ss = (List) keys;

        propertyValues.put("keys", ss);
        Object times = propertyValues.get("time_list");
        List<Double> timesDouble = (List) times;
        List<Long> timesLong = new ArrayList<>();
        for (Double dble : timesDouble) {
            Long l = dble.longValue();
            timesLong.add(l);
        }
        propertyValues.put("time_list", timesLong);
        event.setPropertyValues(propertyValues);
        assertThat(event).isNotNull();
        log.info("event : {}", event);
        return event;
    }

    public static Event fromJsonString(String jsonstr) {
//        String json = createEscapedString(jsonstr);
        Gson g = new Gson();
        Event event = g.fromJson(jsonstr, Event.class);
        return event;
    }

    private static String createEscapedString(String src) {
        // StringBuilder for the new String
        src = src.replaceAll("\n", "");
        String newSrc = src.replaceAll("\\/", "^");
        return newSrc;
//        StringBuilder builder = new StringBuilder();

        // First occurrence
      /*  int index = src.indexOf('/');
        // lastAdded starting at position 0
        int lastAdded = 0;

        while (index >= 0) {
            // append first part without a /
            builder.append(src.substring(lastAdded, index));

            // if / doesn't have a \ directly in front - add a \
            if (index - 1 >= 0 && !src.substring(index - 1, index).equals("\\")) {
                builder.append("\\");
                // if we are at index 0 we also add it because - well it's the
                // first character
            } else if (index == 0) {
                builder.append("\\");
            }

            // change last added to index
            lastAdded = index;
            // change index to the new occurrence of the /
            index = src.indexOf('/', index + 1);
        }

        // add the rest of the string
        builder.append(src.substring(lastAdded, src.length()));
        // return the new String
        return builder.toString();*/
    }
}
