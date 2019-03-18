package com.threathunter.bordercollie.slot.compute.util;

import com.threathunter.bordercollie.slot.compute.server.ITCommonBase;
import com.threathunter.bordercollie.slot.util.SlotVariableMetaRegister;
import com.threathunter.model.*;
import com.threathunter.variable.VariableMetaBuilder;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import java.net.URL;
import java.util.List;

/**
 * 
 */
public class ITVariableMetaTest {
    @Test
    public void testEventMeta() {
        PropertyCondition.init();
        PropertyMapping.init();
        PropertyReduction.init();
        VariableMeta.init();
        initEventMeta();
        initVariableMeta();
    }

    private void initVariableMeta() {
        ObjectMapper mapper = new ObjectMapper();
        List<Object> variableObjects = null;
        try {
            URL in = ITCommonBase.class.getClassLoader().getResource("slot_metas.json");
            variableObjects = mapper.reader(List.class).readValue(in);
        } catch (Exception e) {
            e.printStackTrace();
        }
        VariableMetaBuilder builder = new VariableMetaBuilder();
        List<VariableMeta> metas = builder.buildFromJson(variableObjects);
//        List<VariableMeta> metas = builder.buildFromJson(JsonFileReader.getValuesFromFile("slot_metas.json", JsonFileReader.ClassType.LIST));
        SlotVariableMetaRegister.getInstance().update(metas);
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
