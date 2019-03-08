package com.threathunter.bordercollie.slot.compute;

import com.threathunter.bordercollie.slot.compute.cache.StorageType;
import com.threathunter.bordercollie.slot.compute.graph.DimensionVariableGraphManager;
import com.threathunter.bordercollie.slot.compute.graph.VariableGraphManager;
import com.threathunter.bordercollie.slot.compute.graph.extension.incident.IncidentVariableGraphManager;
import com.threathunter.bordercollie.slot.compute.graph.extension.incident.IncidentVariableMetaRegister;
import com.threathunter.bordercollie.slot.util.SlotMetricsHelper;
import com.threathunter.config.CommonDynamicConfig;
import com.threathunter.model.Event;
import com.threathunter.model.VariableMeta;
import com.threathunter.variable.DimensionType;
import com.threathunter.variable.exception.NotSupportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * This class is the entrance of slot compute.
 * <p>The run and getInstance  of this class all throw a <tt>RuntimeException</tt>
 *
 * @author  Yuan Yi
 * @see     Thread
 * @since   2.15
 */
public class SlotEngine implements SlotComputable {
    private static Logger logger = LoggerFactory.getLogger(SlotEngine.class);
    private static final Logger LOGGER = LoggerFactory.getLogger("bordercollie");
    private final Long intervalInMillis;
    private final Set<DimensionType> dimensions;
    private final StorageType storageType;
    private final Map<Long, SlotWindow> memory;
    private final List<VariableMeta> metas;
    private final boolean isWait = CommonDynamicConfig.getInstance().getBoolean("engine.mode.wait", false);
    volatile boolean engineRunning = false;
    private volatile Map<DimensionType, DimensionVariableGraphManager> dimensionedGraphManagers;
    private volatile Map<DimensionType, IncidentVariableGraphManager> dimensionedIncidentVariableGraphManager;
    private volatile Long currentId;
    private ManagerPool pool;

    /**
     * @param duration    integer duration
     * @param timeUnit    {@link java.util.concurrent.TimeUnit}
     * @param dimensions  enable dimensions for compute, like(ip, uid, did, page, global)
     * @param storageType {@link com.threathunter.bordercollie.slot.compute.cache.StorageType}
     *
     *
     */
    public SlotEngine(int duration, TimeUnit timeUnit, Set<DimensionType> dimensions, List<VariableMeta> metas, final StorageType storageType) {
        this.intervalInMillis = timeUnit.toMillis(duration);
        currentId = -1L;
        this.dimensions = dimensions;
        this.storageType = storageType;
        this.memory = new ConcurrentHashMap<>();
        this.metas = metas;
        LOGGER.warn("SlotEngine init , intervalInMillis:{} , dimensions:{} , storageType:{} , metas: {}" ,intervalInMillis,dimensions,storageType,metas);
    }

    public List<VariableMeta> getMetas() {
        return metas;
    }

    @Override
    public boolean isStopped() {
        return !this.engineRunning;
    }

    @Override
    public boolean save() {
        try {
            saveWindow();
            return true;
        } catch (Exception e) {
            logger.warn(String.format("save window error. currentId %d.", currentId), e);
            return false;
        }
    }

    private void startPool() {
        Thread.UncaughtExceptionHandler h = (th, ex) -> {
            logger.error(String.format("thread name:%s, error in engine pool,exit", th), ex);
            SlotMetricsHelper.getInstance().addMetrics("enginePoolError", 1.0, "ex", ex.getMessage());
            stop();
        };
        pool = new ManagerPool(dimensions, metas);
        pool.start();
    }

    public void stop() {
        pool.dispose();
        engineRunning = false;
    }

    @Override
    public synchronized Long add(Event event) {
        if (engineRunning == false)
            throw new NotSupportException("start engine first");
        Long eventWindowId = event.getTimestamp() / intervalInMillis * intervalInMillis;
        if (currentId < eventWindowId) {
            if (currentId > 0) {
                saveWindow();
            }
            currentId = eventWindowId;
            createWindow();
            compute(event);
            return currentId;
        } else if (currentId.equals(eventWindowId)) {
            compute(event);
            return currentId;
        } else {
            return -1l;
        }
    }

    private void compute(Event event) {
        boolean dummy = (event.getName() == null || event.getName().isEmpty());
        if (dummy) {
            //logger.info(" wangbo 打印太多先屏蔽掉 add a dummy event, just return, event={}",event);
            return;
        }
        dimensionedGraphManagers.values().forEach(graph -> {
            doCompute(graph, event);
        });
        dimensionedIncidentVariableGraphManager.values().forEach(graph -> {
            doCompute(graph, event);
        });
    }

    private void doCompute(VariableGraphManager graph, Event event) {
        if (isWait) {
            while (!graph.compute(event)) {
                try {
                    SlotMetricsHelper.getInstance().addMetrics("engineComputeDiscardEvent", 1.0, "event", event.getName());
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    logger.warn(String.format("stop compute incident --> event:%s", event), e);
                    Thread.currentThread().interrupt();
                    throw new RuntimeException();
                }
            }
        } else {
            boolean success = graph.compute(event);
            if (!success) {
                SlotMetricsHelper.getInstance().addMetrics("engineComputeDiscardEvent", 1.0, "event", event.getName());
            }
        }
    }

    private synchronized void createWindow() {
        dimensionedGraphManagers = pool.getInstance(InstanceType.COMMON);
        dimensionedIncidentVariableGraphManager = pool.getInstance(InstanceType.INCIDENT);
        if (dimensionedGraphManagers != null) {
            this.dimensionedGraphManagers.values().forEach(graphManager -> graphManager.start());
        }
        if (dimensionedIncidentVariableGraphManager != null) {
            this.dimensionedIncidentVariableGraphManager.values().forEach(graphManager -> graphManager.start());
        }
        //logger.warn("created window --> id : {}, interval : {}(ms)", currentId, intervalInMillis);
        LOGGER.warn("created window --> id : {}, interval : {}(ms)", currentId, intervalInMillis);

    }

    private synchronized void saveWindow() {
        SlotWindow slotComputeMediator = SlotFactory.createSlotWindow(currentId);
        slotComputeMediator.setMetas(this.metas);
        slotComputeMediator.setDimensionedGraphManagers(this.dimensionedGraphManagers);
        slotComputeMediator.setDimensionedIncidentVariableGraphManager(this.dimensionedIncidentVariableGraphManager);
        if (this.dimensionedGraphManagers != null) {
            this.dimensionedGraphManagers.forEach((k, v) -> {
                while (!v.isAllEmpty()) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                v.stop();
            });
        }
        if (this.dimensionedIncidentVariableGraphManager != null) {
            this.dimensionedIncidentVariableGraphManager.forEach((k, v) -> {
                while (!v.isAllEmpty()) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                v.stop();
            });
        }
        slotComputeMediator.setMetas(this.getMetas());
        memory.put(currentId, slotComputeMediator);
        LOGGER.info("saved window --> window id:{}", currentId);
    }

    @Override
    public synchronized Map<Integer, Long> add(List<Event> events) {
        Map<Integer, Long> map = new HashMap<Integer, Long>();
        for (Event event : events) {
            Long id = add(event);
            int hash = event.hashCode();
            map.put(hash, id);
        }
        return map;
    }

    @Override
    public Map<Long, SlotWindow> getAllSlots() {
        return this.memory;
    }

    @Override
    public SlotWindow getSlot(Long slotPoint) {
        return memory.get(slotPoint);
    }

    @Override
    public void removeSlot(Long slotPoint) {
        memory.remove(slotPoint);
    }

    private void startPoolWithMetas(List<VariableMeta> metas) {
        pool = new ManagerPool(this.dimensions, metas);
        pool.start();
        logger.warn("start...");
    }

    @Override
    public void update(List<VariableMeta> metas) {
        pool.dispose();
        startPoolWithMetas(metas);
        logger.warn("update the variable metas --> metas size:{}", metas.size());
    }

    @Override
    public synchronized Map<DimensionType, DimensionVariableGraphManager> getCurrentCommonManagers() {
        if (!this.engineRunning)
            throw new RuntimeException("start slot engine first");
        logger.warn("get current common managers,currentId:{},current hour date{}:,current date:{}", currentId, new Date(currentId), new Date());
        return this.dimensionedGraphManagers;
    }

    @Override
    public synchronized Map<DimensionType, IncidentVariableGraphManager> getCurrentIncidentManagers() {
        if (!this.engineRunning)
            throw new RuntimeException("start slot engine first");
        logger.warn("get current incident managers,currentId:{},current hour date{}:,current date:{}", currentId, new Date(currentId), new Date());
        return this.dimensionedIncidentVariableGraphManager;
    }

    @Override
    public void start() {
        engineRunning = true;
        startPool();
        createWindow();
    }

    enum InstanceType {
        COMMON, INCIDENT
    }

    /**
     * This class supports  a poll instance of
     * Map<DimensionType, DimensionVariableGraphManager> and
     * Map<DimensionType, IncidentVariableGraphManager>.
     * <p>The run and getInstance  of this class all throw a <tt>RuntimeException</tt>
     *
     * @author  Yuan Yi
     * @see     Thread
     * @since   2.15
     */
    private class ManagerPool extends Thread {
        private final Set<DimensionType> innerDimensions;
        BlockingQueue<Map<DimensionType, DimensionVariableGraphManager>> dimensionedQueue = new ArrayBlockingQueue<>(5);
        BlockingQueue<Map<DimensionType, IncidentVariableGraphManager>> incidentQueue = new ArrayBlockingQueue<>(5);
        volatile boolean running = false;
        private List<VariableMeta> metas;

        public ManagerPool(Set<DimensionType> innerDimensions, List<VariableMeta> metas) {
            this.innerDimensions = innerDimensions;
            this.metas = metas;
        }

        @Override
        public void start() {
            running = true;
            super.start();
        }

        @Override
        public void run() {
            while (running) {
                try {
                    if (dimensionedQueue.size() < 5)
                        dimensionedQueue.offer(createDimensionedInstance());
                    if (incidentQueue.size() < 5)
                        incidentQueue.offer(createIncidentInstance());
                    if (dimensionedQueue.size() >= 5 && incidentQueue.size() >= 5) {
//                        logger.warn("engine_pool:running, size:5");
                        Thread.sleep(10000);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
            dimensionedQueue.clear();
            incidentQueue.clear();
            logger.warn("pool have released resource");
        }

        public void dispose() {
            running = false;
            logger.warn("pool trying to dispose resource.");
        }

        public Map getInstance(InstanceType type) {
            try {
                if (type == InstanceType.COMMON)
                    return dimensionedQueue.poll(15L, TimeUnit.SECONDS);
                else if (type == InstanceType.INCIDENT)
                    return incidentQueue.poll(15L, TimeUnit.SECONDS);
            } catch (InterruptedException exception) {
                logger.warn(String.format("pool get instance error --> type:{} ", type.name()), exception);
                Thread.currentThread().interrupt();
                throw new RuntimeException(exception);
            }
            return null;
        }

        public Map<DimensionType, DimensionVariableGraphManager> createDimensionedInstance() {
            HashMap<DimensionType, DimensionVariableGraphManager> obj = new HashMap<>();
            innerDimensions.forEach(dimension -> {
                List<VariableMeta> dimensionMetas = getDimensionMetas(dimension, InstanceType.COMMON);
                if (dimensionMetas != null) {
                    obj.put(dimension, new DimensionVariableGraphManager(dimension,
                            dimensionMetas, storageType));
                }
            });
            return obj;
        }

        private List<VariableMeta> getDimensionMetas(DimensionType dimension, InstanceType type) {
            final List<VariableMeta> dimensionMetas = new ArrayList<>();
            if (InstanceType.COMMON == type) {
                metas.forEach(meta -> {
                    if (meta.getDimension().equals(dimension.toString()) || meta.getDimension().trim().isEmpty())
                        dimensionMetas.add(meta);
                });
                return dimensionMetas;
            }
            if (InstanceType.INCIDENT == type) {
                List<VariableMeta> iMetas = IncidentVariableMetaRegister.getDimensionedMetas(dimension);
                return iMetas;
            }
            return null;
        }

        public Map<DimensionType, IncidentVariableGraphManager> createIncidentInstance() {
            HashMap<DimensionType, IncidentVariableGraphManager> obj = new HashMap<>();
            innerDimensions.forEach(dimension -> {
                List<VariableMeta> incidentMetas = getDimensionMetas(dimension, InstanceType.INCIDENT);
                if (incidentMetas != null) {
                    obj.put(dimension, new IncidentVariableGraphManager(dimension,
                            incidentMetas, storageType));
                }
            });
            return obj;
        }
    }
}
