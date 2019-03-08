package com.threathunter.bordercollie.slot.benchmark;

import com.threathunter.bordercollie.slot.EnvironmentUtil;
import com.threathunter.bordercollie.slot.compute.SlotFactory;
import com.threathunter.bordercollie.slot.compute.cache.StorageType;
import com.threathunter.bordercollie.slot.util.SlotVariableMetaRegister;
import com.threathunter.bordercollie.slot.util.StrategyInfoCache;
import com.threathunter.config.CommonDynamicConfig;
import com.threathunter.metrics.MetricsAgent;
import com.threathunter.model.Event;
import com.threathunter.variable.DimensionType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author Yuan Yi <yi.yuan@threathunter.cn>
 * @since: 2.16
 */
@Slf4j
public class SlotEnginePerformanceTest extends PerformanceEnvironmentBase {


    public static List<DimensionType> enableTypes = new ArrayList<>();

    private static void addIncidentInfoToEvent(Event event) {
        String[] notices = ((String) event.getPropertyValues().get("notices")).split(",");
        Map<String, MutableLong> sceneScore = new HashMap<>();
        Map<String, List<String>> sceneStrategies = new HashMap<>();
        Set<String> tags = new HashSet<>();
        Set<String> noticeList = new HashSet<>();
        for (String notice : notices) {
            if (!StrategyInfoCache.getInstance().containsStrategy(notice)) {
                continue;
            }
            tags.addAll(StrategyInfoCache.getInstance().getTags(notice));
            sceneStrategies.computeIfAbsent(StrategyInfoCache.getInstance().getCategory(notice), s -> new ArrayList<>()).add(notice);
            noticeList.add(notice);
        }
        if (noticeList.size() <= 0) {
            return;
        }
        sceneStrategies.forEach((scene, strategies) ->
                strategies.forEach(strategy -> sceneScore.computeIfAbsent(strategy, s -> new MutableLong(0)).add(
                        StrategyInfoCache.getInstance().getScore(strategy))));
        event.getPropertyValues().put("scores", sceneScore);
        event.getPropertyValues().put("strategies", sceneStrategies);
        event.getPropertyValues().put("tags", tags);
        event.getPropertyValues().put("noticelist", noticeList);
    }

    @BeforeClass
    public static void setup() {
//        CommonDynamicConfig.getInstance().addOverrideProperty("persist_path", "/opt/test/persistent");
//        CommonDynamicConfig.getInstance().addOverrideProperty("persist_path", "/home/yy/source/bordercollie/persistent");
        CommonDynamicConfig.getInstance().addConfigFile("nebula.conf");
        MetricsAgent.getInstance().start();
        List<DimensionType> dimensionTypes = new ArrayList<DimensionType>();
        dimensionTypes.add(DimensionType.IP);
        dimensionTypes.add(DimensionType.DID);
        dimensionTypes.add(DimensionType.UID);
        dimensionTypes.add(DimensionType.GLOBAL);
        dimensionTypes.add(DimensionType.PAGE);
        dimensionTypes.add(DimensionType.OTHER);
        enableTypes.addAll(dimensionTypes);
        EnvironmentUtil.initAll();
        HashSet set = new HashSet();
        set.addAll(enableTypes);
        engine = SlotFactory.createSlotEngine(1, TimeUnit.HOURS, set, SlotVariableMetaRegister.getInstance().getAllMetas(), StorageType.BYTES_ARRAY);
    }

    /**
     * 一组性能测试,环境是默认的slot_meta,默认的events_metas,默认的strategy
     * <p>
     * <strong>
     * No wait 算不过 直接丢:engine.mode.wait = false
     * </strong>
     * <p>
     * test for SlotEngine compute,只是计算，不涉及IO,所有结果都在内存中
     * 1) 输入 5000Events;
     * Test testSlotEngine_5KEvents took 8946ms, 1.7892 ms/event
     * 2) 输入 50000Events;
     * Test testSlotEngine_5WEvents took 21971ms 0.4394 ms/event
     * 3) 输入 100000Events;
     * Test testSlotEngine_10WEvents took 30844ms 0.3084 ms/event
     * <p>
     * <strong>
     * wait 模式 不丢弃 配置项： engine.mode.wait =true
     * </strong>
     * <p>
     * test for SlotEngine compute,只是计算，不涉及IO,所有结果都在内存中
     * 1) 输入 5000Events;
     * Test testSlotEngine_5KEvents took 8360ms, 1.672 ms/event
     * 2) 输入 50000Events;
     * Test testSlotEngine_5WEvents took 27425ms 0.5485 ms/event
     * 3) 输入 100000Events;
     * Test testSlotEngine_10WEvents took 48418ms 0.48418 ms/event
     * <p>
     */
    @Ignore
    @Test
    public void testSlotEngine_5KEvents() {
        Event[] events = generateRandom5KEvents();
        doComputationAtEngine(events, "testSlotEngine_5KEvents");
    }

    @Override
    public void decorateEvent(Event event) {

        String notices = (String) event.getPropertyValues().get("notices");
        if (notices != null && !notices.isEmpty()) {
            addIncidentInfoToEvent(event);
        }
    }

    private void doComputationAtEngine(Event[] events, String name) {
        log.info("================{}:start======================", name);
        long start = System.currentTimeMillis();
        engine.start();
        for (Event event : events)
            engine.add(event);
        engine.save();
        engine.stop();
        long end = System.currentTimeMillis();
        log.info("name:{}\tsize:{}\tcost:{}", name, events.length, end - start);
        log.info("================{}:end======================", name);
    }

    @Ignore
    @Test
    public void testSlotEngine_5WEvents() {
        Event[] events = generateRandom5WEvents();
        doComputationAtEngine(events, "testSlotEngine_5WEvents");
    }

    @Ignore
    @Test
    public void testSlotEngine_10WEvents() {
        Event[] events = generateRandom10WEvents();
        doComputationAtEngine(generateRandom10WEvents(), "testSlotEngine_10WEvents");
    }

    /**
     *
     * 除去去base的变量，总共98个变量，所有dimension。
     * 1)本地 String.format.().intern();
     * name:testSlotEngine_250MEvents data:250M size:234069	cost:159465   0.681273 ms/event
     * name:testSlotEngine_500MEvents data:500M	size:499853	cost:322899   0.645987 ms/event
     * name:testSlotEngine_1GEvents   data:1G	size:1000294 cost:994854  0.994561 ms/event
     *
     * 测试服务其上数据约速度快
     * name:testSlotEngine_250MEvents	size:234069	cost:85180        (8 )
     * name:testSlotEngine_500MEvents	size:499853	cost:206669     (206 )
     * name:testSlotEngine_1GEvents   size:1000294    cost:401702 (401 )
     *2 服务器上VariableDataContext getKeyContext 改为StringBuilder 之后
     * name:testSlotEngine_250MEvents       size:234069     cost:8624
     * namee:testSlotEngine_500MEvents       size:499853     cost:49787
     * name:testSlotEngine_1GEvents         size:1000294    cost:104898    0.104867 ms/event=10000events/s 性能提升4.5倍
     * */
    @Ignore
    @Test
    public void testSlotEngine_250MEvents() {
        calculate("2017122201", "testSlotEngine_250MEvents");
    }

    //    @Ignore
    @Test
    public void testSlotEngine_500MEvents() {
        calculate("2017122322", "testSlotEngine_500MEvents");
    }

    //    @Ignore
    @Test
    public void testSlotEngine_1GEvents() {
        compositeCalculate(new String[]{"2017122122", "2017122322"}, "testSlotEngine_1GEvents");
    }


}
