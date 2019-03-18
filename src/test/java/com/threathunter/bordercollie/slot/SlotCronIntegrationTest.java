package com.threathunter.bordercollie.slot;

import com.threathunter.bordercollie.slot.api.OfflineCronServer;
import com.threathunter.bordercollie.slot.util.JsonFileReader;
import com.threathunter.bordercollie.slot.util.SlotVariableMetaRegister;
import com.threathunter.bordercollie.slot.util.StrategyInfoCache;
import com.threathunter.config.CommonDynamicConfig;
import com.threathunter.model.BaseEventMeta;
import com.threathunter.model.EventMeta;
import com.threathunter.model.EventMetaRegistry;
import com.threathunter.model.VariableMeta;
import com.threathunter.variable.VariableMetaBuilder;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.threathunter.bordercollie.slot.util.JsonFileReader.ClassType.LIST;

/**
 * 
 */
public class SlotCronIntegrationTest {
    private void initMetas(final String metaFile) throws IOException {
        List<EventMeta> eventMetas = new ArrayList<>();
        JsonFileReader.getValuesFromFile("current_events.json", LIST).forEach(o -> eventMetas.add(
                BaseEventMeta.from_json_object(o)
        ));
        EventMetaRegistry.getInstance().updateEventMetas(eventMetas);

        List<VariableMeta> variableMetas = new VariableMetaBuilder().buildFromJson(
                JsonFileReader.getFromResourceFile(metaFile, List.class));
        SlotVariableMetaRegister.getInstance().update(variableMetas);
        StrategyInfoCache.getInstance().update(JsonFileReader.getValuesFromFile("current_strategies.json", JsonFileReader.ClassType.LIST));
    }

    @Test
    public void testOrderAndTransaction() throws IOException {
        initMetas("huazhu_dashboard_slot.json");
        CommonDynamicConfig.getInstance().addOverrideProperty("enable_profile_output", false);
        CommonDynamicConfig.getInstance().addOverrideProperty("enable_continuous_output", false);
        OfflineCronServer manager = new OfflineCronServer("/home/daisy/workplace/Code_threathunter/current_bordercollie/bordercollie/persistent/2018020615");

        manager.start();
    }

    @Test
    public void testAllSlotMetas() throws IOException {
        initMetas("current_slot.json");

        CommonDynamicConfig.getInstance().addOverrideProperty("enable_profile_output", false);
        CommonDynamicConfig.getInstance().addOverrideProperty("enable_continuous_output", false);
//        CommonDynamicConfig.getInstance().addOverrideProperty("enable_offline_slot_output", false);

        CommonDynamicConfig.getInstance().addOverrideProperty("nebula.slot.graph.dimension.shard.ip", 1);
        CommonDynamicConfig.getInstance().addOverrideProperty("nebula.slot.graph.dimension.shard.did", 1);
        CommonDynamicConfig.getInstance().addOverrideProperty("nebula.slot.graph.dimension.shard.uid", 1);
        CommonDynamicConfig.getInstance().addOverrideProperty("nebula.slot.graph.dimension.shard.page", 1);
        CommonDynamicConfig.getInstance().addOverrideProperty("nebula.slot.graph.dimension.shard.global", 1);
        CommonDynamicConfig.getInstance().addOverrideProperty("nebula.slot.graph.dimension.shard.other", 1);

        CommonDynamicConfig.getInstance().addOverrideProperty("nebula.slot.incident.shard.ip", 1);
        CommonDynamicConfig.getInstance().addOverrideProperty("nebula.slot.incident.shard.did", 1);
        CommonDynamicConfig.getInstance().addOverrideProperty("nebula.slot.incident.shard.uid", 1);
        CommonDynamicConfig.getInstance().addOverrideProperty("nebula.slot.incident.shard.page", 1);
        CommonDynamicConfig.getInstance().addOverrideProperty("nebula.slot.incident.shard.global", 1);
        CommonDynamicConfig.getInstance().addOverrideProperty("nebula.slot.incident.shard.other", 1);

//        CommonDynamicConfig.getInstance().addOverrideProperty("slot_dimensions", "did");
        CommonDynamicConfig.getInstance().addOverrideProperty("babel_server", "redis");

        OfflineCronServer manager = new OfflineCronServer("/home/daisy/workplace/Code_threathunter/current_bordercollie/bordercollie/persistent/2018020820");

        manager.start();
    }
}
