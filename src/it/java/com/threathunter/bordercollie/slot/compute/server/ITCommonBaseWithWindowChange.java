package com.threathunter.bordercollie.slot.compute.server;

import com.threathunter.bordercollie.slot.compute.SlotEngine;
import com.threathunter.bordercollie.slot.compute.SlotFactory;
import com.threathunter.bordercollie.slot.util.SlotVariableMetaRegister;
import com.threathunter.config.CommonDynamicConfig;
import com.threathunter.mock.simulator.AccountLoginEventsAction;
import com.threathunter.mock.simulator.HttpDynamicEventsAction;
import com.threathunter.model.*;
import com.threathunter.variable.DimensionType;
import com.threathunter.variable.VariableMetaBuilder;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.AfterClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Created by yy on 17-11-14.
 */
public abstract class ITCommonBaseWithWindowChange {
    private static Logger logger = LoggerFactory.getLogger(ITCommonBaseWithWindowChange.class);
    protected static Set<DimensionType> dimensionTypes = new HashSet<>();
    protected static SlotEngine engine;
    protected static List<Event> events;
    static List<VariableMeta> metas;

    //    private static boolean setUpIsDone=false;
//    private static boolean tearDownIsDone =false;
//    @BeforeClass
    public static void setUp() {
        CommonDynamicConfig.getInstance().addConfigFile("nebula.conf");
        CommonDynamicConfig.getInstance().addConfigFile("online.conf");
      /*  dimensionTypes.add(DimensionType.IP);
        dimensionTypes.add(DimensionType.DID);*/
        dimensionTypes.add(DimensionType.UID);
       /* dimensionTypes.add(DimensionType.GLOBAL);
        dimensionTypes.add(DimensionType.PAGE);
        dimensionTypes.add(DimensionType.OTHER);*/
        PropertyCondition.init();
        PropertyMapping.init();
        PropertyReduction.init();
        VariableMeta.init();
        initEventMeta();
        initVariablemeta();
        events = new ArrayList<>();
//        List<EventMeta>
       /* List<VariableMeta> metas=new ArrayList<>();
        for(DimensionType type : dimensionTypes){
            metas.addAll(SlotVariableMetaRegister.getInstance().getDimensionedVariables(type));
        }*/
        engine = SlotFactory.createSlotEngine(1, TimeUnit.HOURS, dimensionTypes, metas);
//        events.addAll(getUidEvents());
        engine.start();
//        engine.add(events);
        try {
            Thread.sleep(10000);
//            Thread.sleep(5000);
            logger.info("============start first uid event");
            engine.add(getUidEvents());
            logger.info("============end fir uid event");
            /*   Thread.sleep(2000);
            engine.add(getUaEvents());
            Thread.sleep(2000);
            engine.add(getGeoEvents());
            Thread.sleep(2000);
            engine.add(getDidEvents());*/
            Thread.sleep(60000);
            engine.add(getUidEvents());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


      /*  assertThat(engine).isNotNull();
        assertThat(events).isNotEmpty();*/

    }


    @AfterClass
    public static void tearDown() {
//      engine.stop();
    }

    private static void initEventMeta() {
        ObjectMapper mapper = new ObjectMapper();
        List<Object> eventsObjects = null;
        try {
            URL in = ITCommonBaseWithWindowChange.class.getClassLoader().getResource("events.json");
            eventsObjects = mapper.reader(List.class).readValue(in);
        } catch (Exception e) {
            e.printStackTrace();
        }
        eventsObjects.forEach(event -> EventMetaRegistry.getInstance().addEventMeta(BaseEventMeta.from_json_object(event)));
    }

    private static void initVariablemeta() {
        ObjectMapper mapper = new ObjectMapper();
        List<Object> variableObjects = null;
        try {
            URL in = ITCommonBaseWithWindowChange.class.getClassLoader().getResource("slot_metas.json");
            variableObjects = mapper.reader(List.class).readValue(in);
        } catch (Exception e) {
            e.printStackTrace();
        }
        VariableMetaBuilder builder = new VariableMetaBuilder();
        List<VariableMeta> metas = builder.buildFromJson(variableObjects);
        ITCommonBaseWithWindowChange.metas = metas;
//        List<VariableMeta> metas = builder.buildFromJson(JsonFileReader.getValuesFromFile("slot_metas.json", JsonFileReader.ClassType.LIST));
        logger.info("init in common base: metas size = {}", metas.size());
        SlotVariableMetaRegister.getInstance().update(metas);
    }

    protected static List<Event> getUidEvents() {
        HttpDynamicEventsAction action = new HttpDynamicEventsAction("www.threathunter.cn", 1);
        action.constructEvents();
        List<Event> events = action.getEvents();
        events.forEach(event -> {
//           int i=(int)(Math.random()*100);
            event.getPropertyValues().put("uid", "345736470");
          /* if(i%3==0 )
           event.getPropertyValues().put("geo_province","上海市");
           else if(i%3==1)
               event.getPropertyValues().put("geo_province","南京市");
           else if (i%3 ==2)
               event.getPropertyValues().put("geo_province","北京市");*/
//            event.getPropertyValues().put("useragent","mozila");
        });
        if (events == null || events.size() == 0)
            throw new RuntimeException("events should not null");
        return events;
    }

    protected static List<Event> getUaEvents() {
        AccountLoginEventsAction action = new AccountLoginEventsAction("www.threathunter.cn", 1, "127.0.0.1");
        action.constructEvents();
        List<Event> events = action.getEvents();
        events.forEach(event -> {
//            int i=(int)(Math.random()*100);
            event.getPropertyValues().put("uid", "345736470");
          /* if(i%3==0 )
           event.getPropertyValues().put("geo_province","上海市");
           else if(i%3==1)
               event.getPropertyValues().put("geo_province","南京市");
           else if (i%3 ==2)
               event.getPropertyValues().put("geo_province","北京市");*/
            event.getPropertyValues().put("useragent", "mozila");
        });
        if (events == null || events.size() == 0)
            throw new RuntimeException("events should not null");
        return events;
    }

    protected static List<Event> getGeoEvents() {
        AccountLoginEventsAction action = new AccountLoginEventsAction("www.threathunter.cn", 1, "127.0.0.1");
        action.constructEvents();
        List<Event> events = action.getEvents();
        events.forEach(event -> {
//            int i=(int)(Math.random()*100);
            event.getPropertyValues().put("uid", "345736470");
            event.getPropertyValues().put("geo_province", "上海市");
          /* if(i%3==0 )
           else if(i%3==1)
               event.getPropertyValues().put("geo_province","南京市");
           else if (i%3 ==2)
               event.getPropertyValues().put("geo_province","北京市");*/
            // event.getPropertyValues().put("useragent","mozila");
        });
        if (events == null || events.size() == 0)
            throw new RuntimeException("events should not null");
        return events;
    }


    protected static List<Event> getDidEvents() {
        AccountLoginEventsAction action = new AccountLoginEventsAction("www.threathunter.cn", 1, "127.0.0.1");
        action.constructEvents();
        List<Event> events = action.getEvents();
        events.forEach(event -> {
//            int i=(int)(Math.random()*100);
            event.getPropertyValues().put("uid", "345736470");
            event.getPropertyValues().put("did", "device00000001");
          /* if(i%3==0 )
           else if(i%3==1)
               event.getPropertyValues().put("geo_province","南京市");
           else if (i%3 ==2)
               event.getPropertyValues().put("geo_province","北京市");*/
            // event.getPropertyValues().put("useragent","mozila");
        });
        if (events == null || events.size() == 0)
            throw new RuntimeException("events should not null");
        return events;
    }
}
