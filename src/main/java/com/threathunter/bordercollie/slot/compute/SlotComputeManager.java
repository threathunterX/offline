package com.threathunter.bordercollie.slot.compute;

import com.threathunter.bordercollie.slot.compute.cache.StorageType;
import com.threathunter.bordercollie.slot.compute.graph.DimensionVariableGraphManager;
import com.threathunter.bordercollie.slot.compute.graph.VariableGraphManager;
import com.threathunter.bordercollie.slot.compute.graph.extension.incident.IncidentVariableGraphManager;
import com.threathunter.bordercollie.slot.compute.graph.extension.incident.IncidentVariableMetaRegister;
import com.threathunter.bordercollie.slot.compute.graph.query.VariableQuery;
import com.threathunter.bordercollie.slot.util.*;
import com.threathunter.common.Identifier;
import com.threathunter.config.CommonDynamicConfig;
import com.threathunter.model.Event;
import com.threathunter.model.VariableMeta;
import com.threathunter.persistent.core.EventReadHelper;
import com.threathunter.persistent.core.io.BufferedRandomAccessFile;
import com.threathunter.variable.DimensionType;
import org.apache.commons.lang3.mutable.MutableLong;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by daisy on 17/3/6.
 */
public class SlotComputeManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(SlotComputeManager.class);

    private static final long HOUR_MILLIS = 3600000;
    private long currentHourMillis = -1;
    private long graphUpdateTime = -1;
    private final Map<DimensionType, DimensionVariableGraphManager> dimensionedGraphManagers = new HashMap<>();
    private final Map<DimensionType, IncidentVariableGraphManager> dimensionedIncidentVariableGraphManager = new HashMap<>();
    private final boolean isOffline;

    private int loading = 0;

    public int getLoadingCount() {
        return this.loading;
    }

    public void addEvent(final Event event) {
        boolean dummy = (event.getName() == null || event.getName().isEmpty());
        if (isOffline && !dummy) {
            this.dispatch(event);
            return;
        }
        if (this.checkForRightHour(event.getTimestamp())) {
            if (dummy) {
                return;
            }
            this.dispatch(event);
        } else {
            if (!dummy) {
                // add expire metrics
            }
        }
    }

    public Object queryData(final Identifier id, final String key, int topCount) {
        VariableMeta meta;
        VariableGraphManager manager;
        if (SlotVariableMetaRegister.getInstance().containsMeta(id)) {
            meta = SlotVariableMetaRegister.getInstance().getMeta(id);
            manager = dimensionedGraphManagers.get(DimensionType.valueOf(meta.getDimension().toUpperCase()));
        } else {
            meta = IncidentVariableMetaRegister.getMeta(id);
            manager = dimensionedIncidentVariableGraphManager.get(DimensionType.valueOf(meta.getDimension().toUpperCase()));
        }
        if (meta == null) {
            LOGGER.error("variable does not exist");
            return null;
        }

        boolean topValue = false;//meta.isTopValue();
        boolean keyTopValue = false;//meta.isKeyTopValue();

        VariableQuery query;
        if (key == null || key.isEmpty()) {
            if (topValue) {
                query = VariableQueryUtil.broadcastTopQuery(manager, id, topCount);
            } else {
                query = VariableQueryUtil.broadcastQuery(manager, id);
            }
        } else {
            if (keyTopValue) {
                query = VariableQueryUtil.sendKeyTopQuery(manager, id, key, topCount);
            } else {
                query = VariableQueryUtil.sendKeyQuery(manager, id, key);
            }
        }
        return query.waitQueryResult(1, TimeUnit.SECONDS);
    }

    public Object queryData(final Identifier id, final Collection<String> keys, int topCount) {
        VariableMeta meta;
        VariableGraphManager manager;
        if (SlotVariableMetaRegister.getInstance().containsMeta(id)) {
            meta = SlotVariableMetaRegister.getInstance().getMeta(id);
            manager = dimensionedGraphManagers.get(DimensionType.valueOf(meta.getDimension().toUpperCase()));
        } else {
            meta = IncidentVariableMetaRegister.getMeta(id);
            manager = dimensionedIncidentVariableGraphManager.get(DimensionType.valueOf(meta.getDimension().toUpperCase()));
        }
        if (meta == null) {
            LOGGER.error("variable does not exist");
            return null;
        }

        VariableQuery query;
 /*       if (meta.isKeyTopValue()) {
            query = VariableQueryUtil.sendKeyTopQuery(manager, id, keys, topCount);
        } else {*/
        query = VariableQueryUtil.sendKeyQuery(manager, id, keys);
//        }
        return query.waitQueryResult(1, TimeUnit.SECONDS);
    }

    public Object queryData(final Identifier id, final String firstKey, final Collection<String> secondKeys) {
        VariableMeta meta;
        VariableGraphManager manager;
        if (SlotVariableMetaRegister.getInstance().containsMeta(id)) {
            meta = SlotVariableMetaRegister.getInstance().getMeta(id);
            manager = dimensionedGraphManagers.get(DimensionType.valueOf(meta.getDimension().toUpperCase()));
        } else {
            meta = IncidentVariableMetaRegister.getMeta(id);
            manager = dimensionedIncidentVariableGraphManager.get(DimensionType.valueOf(meta.getDimension().toUpperCase()));
        }
        if (meta == null) {
            LOGGER.error("variable does not exist");
            return null;
        }

        VariableQuery query = VariableQueryUtil.sendKeyQuery(manager, id, firstKey, secondKeys);
        return query.waitQueryResult(1, TimeUnit.SECONDS);
    }

    public SlotComputeManager(final Set<DimensionType> enableDimensions, final StorageType storageType, final boolean offline) {
        this.isOffline = offline;
        enableDimensions.forEach(dimension -> {
            List<VariableMeta> dimensionMetas = SlotVariableMetaRegister.getInstance().getDimensionedVariables(dimension);
            if (dimensionMetas != null) {
                this.dimensionedGraphManagers.put(dimension, new DimensionVariableGraphManager(dimension,
                        dimensionMetas, storageType));
            }
            List<VariableMeta> incidentMetas = IncidentVariableMetaRegister.getDimensionedMetas(dimension);
            if (incidentMetas != null) {
                this.dimensionedIncidentVariableGraphManager.put(dimension, new IncidentVariableGraphManager(dimension,
                        incidentMetas, storageType));
            }
        });
        if (this.isOffline) {
            LOGGER.warn("!!! Mode Offline");
        } else {
            LOGGER.warn("!!! Mode Online");
        }
    }

    public boolean isAllEmpty() {
        for (VariableGraphManager manager : this.dimensionedGraphManagers.values()) {
            if (!manager.isAllEmpty()) {
                return false;
            }
        }
        for (VariableGraphManager manager : this.dimensionedIncidentVariableGraphManager.values()) {
            if (!manager.isAllEmpty()) {
                return false;
            }
        }
        return true;
    }

    public boolean isIncidentVariable(final Identifier id) {
        return IncidentVariableMetaRegister.getMeta(id) != null;
    }

    public void start() {
        // start Slot Compute at current hour.
        start(null);
    }

    public void start(final String workingHour) {
        if (workingHour != null) {
            SlotMetricsHelper.getInstance().setWorkingHour(workingHour);
            SlotMetricsHelper.getInstance().setDb("nebula.offline");
        }
        LOGGER.info("slot compute started.");
        // start Slot Compute at Specify hour.
        this.dimensionedGraphManagers.values().forEach(graphManager -> graphManager.start());
        this.dimensionedIncidentVariableGraphManager.values().forEach(graphManager -> graphManager.start());

        try {
            if (CommonDynamicConfig.getInstance().getBoolean("nebula.online.slot.enable", true) == false && workingHour == null) {
                return;
            }
            loadExistingLogEvents(workingHour);
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error(String.format("failed to load event into offline variable at Working Hour: %s", workingHour), e);
            throw new RuntimeException(String.format("failed to load event into offline variable at Working Hour: %s.", workingHour), e);
        }
    }

    public void stop() {
        this.dimensionedGraphManagers.values().forEach(graphManager -> graphManager.stop());
        this.dimensionedIncidentVariableGraphManager.values().forEach(graphManager -> graphManager.stop());
    }

    private void loadExistingLogEvents(final String workingHour) throws IOException {
        String baseDir = CommonDynamicConfig.getInstance().getString("persist_path", PathHelper.getModulePath() + "/persistent");
        int shardCount = CommonDynamicConfig.getInstance().getInt("nebula.persistent.log.shard", 16);

        if (!(new File(baseDir)).exists()) {
            return;
        }

        String loadingDir;
        if (workingHour == null || workingHour.isEmpty()) {
            // Online Slot.
            loadingDir = String.format("%s/%s", baseDir, new DateTime(System.currentTimeMillis()).toString("yyyyMMddHH"));
            LOGGER.warn("loading logs from current log dir: " + loadingDir);
        } else {
            loadingDir = String.format("%s/%s", baseDir, workingHour);
            LOGGER.info("offline slot compute on " + loadingDir);
        }

        if (!(new File(loadingDir)).exists()) {
            LOGGER.error(loadingDir + " is not exists.");
            return;
        }

        String logDir = String.format("%s/%s", loadingDir, "log");
        if (!(new File(logDir)).exists()) {
            LOGGER.error(logDir + " is not exists.");
            return;
        }
        String schemaPath = String.format("%s/%s", loadingDir, "events_schema.json");
        String versionKeys = String.format("%s/%s", loadingDir, "header_version.json");
//        CurrentHourPersistInfoRegister.getInstance().update(schemaPath, versionKeys);

        EventReadHelper eventReadHelper = new EventReadHelper();

//        EventsGrouper grouper = new EventsGrouper(2000);

        LOGGER.warn("loading logs from current log dir.");
        for (int i = 0; i < shardCount; i++) {
            int eventCount = 0;
            File shard = new File(String.format("%s/%d", logDir, i));
            if (!shard.exists()) {
                continue;
            }
            long starttime = System.currentTimeMillis();
            BufferedRandomAccessFile randomAccessFile = null;
            try {
                Event event = new Event();
                randomAccessFile = new BufferedRandomAccessFile(shard, "r");
                long offset = eventReadHelper.readEvent(randomAccessFile, event, 0);
                while (offset > 0) {
                    String notices = (String) event.getPropertyValues().get("notices");
                    if (notices != null && !notices.isEmpty()) {
//                        System.out.println("notice is not empty");
                        addIncidentInfoToEvent(event);
                    }
                    addEvent(event);
                    eventCount++;
                    loading++;

                    event = new Event();
                    offset = eventReadHelper.readEvent(randomAccessFile, event, offset);
                }
                randomAccessFile.close();
            } catch (Exception e) {
                LOGGER.error("loading persistent error", e);
            } finally {
                long endtime = System.currentTimeMillis();
                LOGGER.info(String.format("shard: %d, events count: %d, costs: %dms", i, eventCount, (endtime - starttime)));
                if (randomAccessFile != null) {
                    randomAccessFile.close();
                }
            }
        }
    }

    private void addIncidentInfoToEvent(final Event e) {
        String[] notices = ((String) e.getPropertyValues().get("notices")).split(",");
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
        e.getPropertyValues().put("scores", sceneScore);
        e.getPropertyValues().put("strategies", sceneStrategies);
        e.getPropertyValues().put("tags", tags);
        e.getPropertyValues().put("noticelist", noticeList);
    }

    private List<VariableMeta> getDimensionedVariables(final DimensionType dimensionType) {
        return SlotVariableMetaRegister.getInstance().getDimensionedVariables(dimensionType);
    }

    private void dispatch(final Event event) {
        dimensionedGraphManagers.values().forEach(graph -> {
            while (!graph.compute(event) && isOffline) {
                try {
                    Thread.sleep(200);
                } catch (Exception e) {
                    LOGGER.error("interrupted", e);
                }
            }
        });
        dimensionedIncidentVariableGraphManager.values().forEach(graph -> {
            while (!graph.compute(event) && isOffline) {
                try {
                    Thread.sleep(200);
                } catch (Exception e) {
                    LOGGER.error("interrupted", e);
                }
            }
        });
    }

    private boolean checkForRightHour(long currentTimestamp) {
        long currentHour = currentTimestamp / HOUR_MILLIS * HOUR_MILLIS;
        if (currentHour == this.currentHourMillis) {
            return true;
        }
        if (currentHour > this.currentHourMillis) {
            LOGGER.warn("clear cache for a new hour");
            this.currentHourMillis = currentHour;
            this.signClean();
            return true;
        }
        return false;
    }

    private void signClean() {
        if (SlotVariableMetaRegister.getInstance().getUpdateTimestamp() > this.graphUpdateTime) {
            dimensionedGraphManagers.forEach((dimension, graph) -> graph.update(SlotVariableMetaRegister.getInstance().getDimensionedVariables(dimension)));
            this.graphUpdateTime = SystemClock.getCurrentTimestamp();
        } else {
            dimensionedGraphManagers.values().forEach(graph -> graph.clear());
        }
        dimensionedIncidentVariableGraphManager.values().forEach(graph -> graph.clear());
    }

    public boolean containsKey(final String keyType, final String key) {
        DimensionType dimensionType = DimensionType.valueOf(keyType);
        return this.dimensionedGraphManagers.get(dimensionType).containsKey(key);
    }
}
