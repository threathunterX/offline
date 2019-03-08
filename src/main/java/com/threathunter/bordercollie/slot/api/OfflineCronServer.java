package com.threathunter.bordercollie.slot.api;

import com.threathunter.bordercollie.slot.compute.SlotEngine;
import com.threathunter.bordercollie.slot.compute.SlotFactory;
import com.threathunter.bordercollie.slot.compute.SlotWindow;
import com.threathunter.bordercollie.slot.compute.cache.StorageType;
import com.threathunter.bordercollie.slot.util.*;
import com.threathunter.config.CommonDynamicConfig;
import com.threathunter.model.Event;
import com.threathunter.persistent.core.api.SequenceReadContext;
import com.threathunter.persistent.core.api.Visitor;
import com.threathunter.variable.DimensionType;
import com.google.gson.Gson;
import org.apache.commons.lang3.mutable.MutableLong;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;

/**
 * Created by daisy on 18-2-9
 */
public class OfflineCronServer {
    private static final Logger LOGGER = LoggerFactory.getLogger(OfflineCronServer.class);
    private static final SlotUtils slotUtils = new SlotUtils();

    private final boolean enableOffline;
    private final boolean enableProfile;
    private final boolean enableAS;
    private final Set<DimensionType> enableDimensions;

    private final String workingHourString;
    private final String fullHourPath;
    private final long workingHourMillis;

    public OfflineCronServer() {
        this(slotUtils.getLastHourStr());
    }

    public OfflineCronServer(final String hourPath) {
        String splitor = Matcher.quoteReplacement(System.getProperty("file.separator"));

        if (hourPath.contains(splitor)) {
            String[] splitArray = hourPath.split(splitor);
            this.workingHourString = splitArray[splitArray.length - 1];
            this.fullHourPath = hourPath;
        } else {
            this.workingHourString = hourPath;
            this.fullHourPath = String.format("%s/%s", CommonDynamicConfig.getInstance().getString("persist_path"), workingHourString);
        }
        this.workingHourMillis = DateTimeFormat.forPattern("yyyyMMddHH").parseDateTime(this.workingHourString).getMillis();

        this.enableOffline = CommonDynamicConfig.getInstance().getBoolean("enable_offline_slot_output", true);
        this.enableProfile = CommonDynamicConfig.getInstance().getBoolean("enable_profile_output", true);
        this.enableAS = CommonDynamicConfig.getInstance().getBoolean("enable_continuous_output", true);
        this.enableDimensions = new HashSet<>();
        for (String ed : CommonDynamicConfig.getInstance().getString("slot_dimensions", "ip|uid|did|global|page|other").split("\\|")) {
            this.enableDimensions.add(DimensionType.valueOf(ed.toUpperCase()));
        }
        //输出配置
        LOGGER.debug("CONFIG:");
        LOGGER.debug("\t  workingHourString: " + this.workingHourString); // 示例：workingHourString: 2018101216
        LOGGER.debug("\t  fullHourPath: " + this.fullHourPath); // 示例：fullHourPath: /data/persistent/2018101216
        LOGGER.debug("\t  workingHourMillis: " + this.workingHourMillis); // 示例：workingHourMillis: 1539331200000
        LOGGER.debug("\t  enableOffline: " + this.enableOffline); // 示例：enableOffline: true
        LOGGER.debug("\t  enableProfile: " + this.enableProfile); // 示例：enableProfile: true
        LOGGER.debug("\t  enableAS: " + this.enableAS); // 示例：enableAS: true

        String temp = "";
        for (DimensionType dimension: this.enableDimensions
        ) {
            temp +=  dimension.toString() + "|";
        }
        LOGGER.debug("\t  enableDimensions: "+ temp); // 示例：enableDimensions: ip|uid|did|global|page|other|
    }

    public void start() throws FileNotFoundException {
        List<DataConsumer> outputs = this.createConsumers();

        LOGGER.info("initial slot compute engine.");
        SlotEngine engine = SlotFactory.createSlotEngine(1, TimeUnit.HOURS, this.enableDimensions,
                SlotVariableMetaRegister.getInstance().getAllMetas(), StorageType.BYTES_ARRAY);

        //回调函数
        Visitor persistentQuery = (event) -> {
            String notices = (String) event.getPropertyValues().get("notices");
            if (notices != null && !notices.isEmpty()) {
                LOGGER.debug("SequenceReadContext.");
                //是否触发策略，如果触发策略则添加对应信息
                addIncidentInfoToEvent(event);

            }
            engine.add(event);

        };
        //从文件中读取当前小时的Event
        SequenceReadContext context = new SequenceReadContext(this.workingHourString, persistentQuery);

        LOGGER.info("start to compute slot graph data");
        long startTime = System.currentTimeMillis();
        try {
            //启动引擎
            engine.start();
            //开始读取本地文件，读到Event就回调上面的persistentQuery
            context.startQuery();
            context.endQuery();
            //保存计算结果到内存
            engine.save();
        } catch (Exception e) {
            throw new RuntimeException("process cron variable data error", e);
        } finally {
            engine.stop();
        }

        while (!engine.isStopped()) {
            try {
                Thread.sleep(2000);
            } catch (Exception e) {
                ;
            }
        }
        long duration = System.currentTimeMillis() - startTime;
        LOGGER.info("compute spends: " + duration + "millis");
        //获取当前窗口计算结果,如：1539054000000 -> 2018-10-09 11:00:00
        SlotWindow dataWindow = engine.getSlot(this.workingHourMillis);

        //获取计算维度
        LOGGER.info("start persistent computed graph data");
        Set<DimensionType> enableDimensions = new HashSet<>();
        String enableDimensionsString = CommonDynamicConfig.getInstance().getString("slot_dimensions", "ip|uid|did|global|page|other");
        for (String dimension : enableDimensionsString.split("\\|")) {
            DimensionType type = DimensionType.valueOf(dimension.toUpperCase());
            enableDimensions.add(type);
        }

        LOGGER.debug("ZJP.OfflineCronServer.start.enableDimensions: " + enableDimensions.toString());

        //事件
        // get incident variable data.
        LOGGER.info("start scan incident data");
        // incident variable data list
        Map<DimensionType, Map<String, Map<String, Object>>> dimensionedAllData = new HashMap<>();
        enableDimensions.forEach(d -> dimensionedAllData.put(d, new HashMap<>()));
        try {
            if (dataWindow != null && dataWindow.getDimensionedIncidentVariableGraphManager() != null) {
                dataWindow.getDimensionedIncidentVariableGraphManager().forEach(((dimensionType, manager) -> {
                    manager.getCacheIterators().forEach(variableCacheIterator -> {
                        DimensionType current = variableCacheIterator.getDimensionType();
                        //启用的维度
                        if (enableDimensions.contains(current)) {
                            while (variableCacheIterator.hasNext()) {
                                Map.Entry<String, Map<String, Object>> next = variableCacheIterator.next();
                                Map<String, Object> variableDataMap = dimensionedAllData.get(current).computeIfAbsent(next.getKey(), k -> new HashMap<>());
                                //如果是GLOBAL维度，进行合并，最终只产生一个事件
                                if (current.equals(DimensionType.GLOBAL)) {
                                    // global need to merge
                                    // do merge
                                    next.getValue().forEach((variable, variableData) -> {

                                        if (!variableDataMap.containsKey(variable)) {
                                            variableDataMap.put(variable, variableData);
                                        } else {

                                            Object data = variableDataMap.get(variable);
                                            //如果已经有此事件（变量），且value为数值，将value相加
                                            if (data instanceof Number) {
                                                variableDataMap.put(variable, ((Number) data).longValue() + ((Number) variableData).longValue());
                                            } else if (data instanceof Collection) {
                                                //如果已经有此事件（变量），且value为数组，放入数组
                                                ((Collection) data).addAll((Collection) variableData);
                                            } else if (data instanceof Map) {
                                                //如果已经有此事件（变量），且value为map，放入map
                                                ((Map) data).putAll((Map) variableData);
                                            }
                                        }
                                    });
                                } else {
                                    //其他维度，不进行合并
                                    // other dimension will not override
                                    variableDataMap.putAll(next.getValue());
                                }
                            }
                        }
                    });
                }));
            }
        } catch (Exception e) {
            LOGGER.error("iterator exception", e);
            try {
                Thread.sleep(1000);
            } catch (Exception te) {
                LOGGER.error("interrupted", te);
            }
        }

        LOGGER.debug("ZJP.OfflineCronServer.start.dimensionedAllData1: " + new Gson().toJson(dimensionedAllData));

        //维度
        // Collect every shard of each dimension, and do map.
        try {
            if (dataWindow != null && dataWindow.getDimensionedGraphManagers() != null) {
                dataWindow.getDimensionedGraphManagers().forEach((dimensionType, manager) -> {
                    if (enableDimensions.contains(dimensionType)) {
                        // every shard
                        manager.getCacheIterators().forEach(variableCacheIterator -> {
                            DimensionType current = variableCacheIterator.getDimensionType();
                            if (enableDimensions.contains(current)) {
                                while (variableCacheIterator.hasNext()) {
                                    Map.Entry<String, Map<String, Object>> variableValueEntryPerKey = variableCacheIterator.next();
                                    Map<String, Object> variableDataMap = dimensionedAllData.get(current).computeIfAbsent(variableValueEntryPerKey.getKey(), k -> new HashMap<>());
                                    if (current.equals(DimensionType.GLOBAL)) {
                                        variableValueEntryPerKey.getValue().forEach((variable, variableData) -> {
                                            if (!variableDataMap.containsKey(variable)) {
                                                variableDataMap.put(variable, variableData);
                                            } else {
                                                Object data = variableDataMap.get(variable);
                                                if (data instanceof Number) {
                                                    variableDataMap.put(variable, ((Number) data).longValue() + ((Number) variableData).longValue());
                                                } else if (data instanceof Collection) {
                                                    ((Collection) data).addAll((Collection) variableData);
                                                } else if (data instanceof Map) {
                                                    ((Map) data).putAll((Map) variableData);
                                                }
                                            }
                                        });
                                    } else {
                                        variableDataMap.putAll(variableValueEntryPerKey.getValue());
                                    }
                                }
                            }
                        });
                    }
                });
            }
        } catch (Exception e) {
            LOGGER.error("iterator error", e);
        }

        LOGGER.info("generated data: " + dimensionedAllData.size());
        SlotMetricsHelper.getInstance().addMetrics("offline.generated", (double) dimensionedAllData.size());

        LOGGER.debug("ZJP.OfflineCronServer.start.dimensionedAllData2: " + new Gson().toJson(dimensionedAllData));

        //产生的数据全部存储起来
        dimensionedAllData.forEach((dimension, keyData) -> {
            keyData.forEach((key, variableData) -> {
                variableData.put("dimension", dimension.toString());
                variableData.put("key", key);
//                LOGGER.debug("generated data to store, dimension: "+dimension.toString()+", key:"+key+", data:" + variableData.toString());
                LOGGER.debug("generated data to store, dimension: " + dimension.toString() + ", key:" + key + ", data:" + new Gson().toJson(variableData));
                outputs.forEach(consumer -> consumer.store(variableData));
            });
        });

        outputs.forEach(o -> o.stop());
    }

    public void stop() {}

    private void addIncidentInfoToEvent(final Event event) {
        LOGGER.warn("addIncidentInfoToEvent: " + new Gson().toJson(event));
        //Event 中  notices(策略名组成)  是什么？
        String[] notices = ((String) event.getPropertyValues().get("notices")).split(",");
        LOGGER.debug("ZJP.OfflineCronServer.addIncidentInfoToEvent.notices: " + new Gson().toJson(notices));

        Map<String, MutableLong> sceneScore = new HashMap<>();
        Map<String, List<String>> sceneStrategies = new HashMap<>();
        Set<String> tags = new HashSet<>();
        Set<String> noticeList = new HashSet<>();
        for (String notice : notices) {
            //是否有策略对应，没有就跳过
            if (!StrategyInfoCache.getInstance().containsStrategy(notice)) {
                continue;
            }
            //获取策略对应标签
            tags.addAll(StrategyInfoCache.getInstance().getTags(notice));
            //根据场景归类
            sceneStrategies.computeIfAbsent(StrategyInfoCache.getInstance().getCategory(notice), s -> new ArrayList<>()).add(notice);
            //添加到通知列表
            noticeList.add(notice);
        }
        //
        if (noticeList.size() <= 0) {
            return;
        }
        //遍历场景及其对应策略
        sceneStrategies.forEach((scene, strategies) ->
                //获取策略对应分值(权重)
                strategies.forEach(strategy -> sceneScore.computeIfAbsent(strategy, s -> new MutableLong(0)).add(
                        StrategyInfoCache.getInstance().getScore(strategy))));
        //设置权重
        event.getPropertyValues().put("scores", sceneScore);
        //设置触发的策略
        event.getPropertyValues().put("strategies", sceneStrategies);
        //策略对应的标签
        event.getPropertyValues().put("tags", tags);
        //通知列表
        event.getPropertyValues().put("noticelist", noticeList);
        LOGGER.debug("ZJP.OfflineCronServer.addIncidentInfoToEvent.event: " + new Gson().toJson(event));
    }

    private List<DataConsumer> createConsumers() throws FileNotFoundException {
        List<DataConsumer> outputs = new ArrayList<>(4);
        if (this.enableProfile) {
            LOGGER.info("create profile consumer");
            //输出到redis -> batch_event_send (notify)
            outputs.add(new ProfileStatDataHelper(this.workingHourString));
        }
        if (this.enableAS) {
            LOGGER.info("create continuous data consumer");
            //存储到AS
            outputs.add(new ContinuousDataHelper(this.workingHourString));
        }
        if (this.enableOffline) {
            LOGGER.info("create offline data consumer");
            //输出到redis -> incident_notify (topic)
            outputs.add(new IncidentDataHelper());
            //输出到leveldb
            outputs.add(new OfflineSlotDataHelper(this.workingHourMillis, false));
        }
        outputs.forEach(o -> o.start());

        return outputs;
    }
}
