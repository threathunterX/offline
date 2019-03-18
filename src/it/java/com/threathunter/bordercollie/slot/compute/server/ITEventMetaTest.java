package com.threathunter.bordercollie.slot.compute.server;

import com.threathunter.model.*;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import java.net.URL;
import java.util.List;

/**
 * 
 */
public class ITEventMetaTest {
    @Test
    public void testEventMeta() {
        PropertyCondition.init();
        PropertyMapping.init();
        PropertyReduction.init();
        VariableMeta.init();
        initEventMeta();
    }

    private static void initEventMeta() {
        ObjectMapper mapper = new ObjectMapper();
        List<Object> eventsObjects = null;
        try {
            URL in = ITCommonBase.class.getClassLoader().getResource("events_only_one.json");
            eventsObjects = mapper.reader(List.class).readValue(in);
        } catch (Exception e) {
            e.printStackTrace();
        }
        eventsObjects.forEach(event -> EventMetaRegistry.getInstance().addEventMeta(BaseEventMeta.from_json_object(event)));
    }
}
