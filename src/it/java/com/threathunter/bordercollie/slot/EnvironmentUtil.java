package com.threathunter.bordercollie.slot;

import com.threathunter.bordercollie.slot.util.SlotVariableMetaRegister;
import com.threathunter.bordercollie.slot.util.StrategyInfoCache;
import com.threathunter.model.*;
import com.threathunter.variable.VariableMetaBuilder;
import org.codehaus.jackson.map.ObjectMapper;

import java.net.URL;
import java.util.List;
import java.util.Map;

/**
 * @author Yuan Yi <yi.yuan@threathunter.cn>
 * @since: 2.16
 */
public class EnvironmentUtil {

    public static void initAll() {
        PropertyCondition.init();
        PropertyMapping.init();
        PropertyReduction.init();
        VariableMeta.init();
        EnvironmentUtil.initEventMeta();
        EnvironmentUtil.initVariablemeta();
        EnvironmentUtil.initStrategyInfoCache();
    }

    public static void initEventMeta() {
        ObjectMapper mapper = new ObjectMapper();
        List<Object> eventsObjects = null;
        try {
            URL in = EnvironmentUtil.class.getClassLoader().getResource("events.json");
            eventsObjects = mapper.reader(List.class).readValue(in);
        } catch (Exception e) {
            e.printStackTrace();
        }
        eventsObjects.forEach(event -> EventMetaRegistry.getInstance().addEventMeta(BaseEventMeta.from_json_object(event)));
    }

    public static void initVariablemeta() {
        ObjectMapper mapper = new ObjectMapper();
        List<Object> variableObjects = null;
        try {
            URL in = EnvironmentUtil.class.getClassLoader().getResource("slot_metas.json");
            variableObjects = mapper.reader(List.class).readValue(in);
        } catch (Exception e) {
            e.printStackTrace();
        }
        VariableMetaBuilder builder = new VariableMetaBuilder();
        List<VariableMeta> metas = builder.buildFromJson(variableObjects);
//        List<VariableMeta> metas = builder.buildFromJson(JsonFileReader.getValuesFromFile("slot_metas.json", JsonFileReader.ClassType.LIST));
        SlotVariableMetaRegister.getInstance().update(metas);
    }

    public static void initStrategyInfoCache() {
        ObjectMapper mapper = new ObjectMapper();
        List<Map<String, Object>> strategyObjects;
        try {
            URL in = EnvironmentUtil.class.getClassLoader().getResource("strategy.json");
            strategyObjects = mapper.reader(List.class).readValue(in);
        } catch (Exception e) {
            throw new RuntimeException("strategy cache init", e);
        }
        StrategyInfoCache.getInstance().update(strategyObjects);
    }
}
