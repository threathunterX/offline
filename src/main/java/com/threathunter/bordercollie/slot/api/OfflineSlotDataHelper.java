package com.threathunter.bordercollie.slot.api;

import com.threathunter.bordercollie.slot.util.PathHelper;
import com.threathunter.bordercollie.slot.util.ResultFormatter;
import com.threathunter.bordercollie.slot.util.SlotMetricsHelper;
import com.threathunter.bordercollie.slot.util.SlotUtils;
import com.threathunter.common.ShutdownHookManager;
import com.threathunter.config.CommonDynamicConfig;
import com.google.gson.Gson;
import org.apache.commons.lang3.mutable.Mutable;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.impl.Iq80DBFactory;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

import java.nio.charset.Charset;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.nio.file.FileVisitResult.CONTINUE;
import static java.nio.file.FileVisitResult.TERMINATE;


/**
 * 
 */
public class OfflineSlotDataHelper implements DataConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(OfflineSlotDataHelper.class);
    private static final Integer ADD_BUFFER_LIMIT = 1000; // flush 1000 entry per time.
    private static final String PERSIST_PATH = CommonDynamicConfig.getInstance().getString("persist_path", PathHelper.getModulePath() + "/persistent");
    private final Long hourTimeMillis;
    private final DB db;
    private SlotUtils slotUtils = new SlotUtils();
    private HashMap<byte[], byte[]> addBuffer = new HashMap<>();

    private final boolean writeMode;
    private final String fullHourPath;

    private volatile boolean running = false;

    public OfflineSlotDataHelper(final Long hourTimeMillis, final boolean readOnly) {
        this.hourTimeMillis = hourTimeMillis;
        if (!readOnly) {
            this.writeMode = true;
            this.fullHourPath = String.format("%s/%s", PERSIST_PATH, new DateTime(hourTimeMillis).toString("yyyyMMddHH"));
            this.deleteOriginSlotDataDir(this.fullHourPath);
            Options options = new Options();
            options.createIfMissing(true);
            try {
                this.db = Iq80DBFactory.factory.open(new File(fullHourPath + "/data_tmp"), options);
            } catch (Exception e) {
                LOGGER.error("error when open level db resources, path: " + fullHourPath + "/data_tmp", e);
                throw new RuntimeException(e);
            }
            ShutdownHookManager.get().addShutdownHook(() -> this.stop(), 100);
        } else {
            this.writeMode = false;
            this.db = OfflineLevelDbCache.getInstance().getLevelDb(hourTimeMillis, false);
            this.fullHourPath = null;
        }
    }

    public Map<String, Object> getStatistic(final String key, final String dimension, final List<String> varList) {
        byte[] dbKey = slotUtils.get_stat_key(key, dimension);
        Map<String, Object> result = new HashMap<>();
        if (dbKey == null) {
            return result;
        }

        if (db == null) {
            LOGGER.error(String.format("error when getting leveldb, key: %s, dimension: %s, varlist: %s, time: %s",
                    key, dimension, varList, new DateTime(this.hourTimeMillis).toString("yyyyMMddHH")));
        }
        try {
            byte[] dbValue = db.get(dbKey);
            if (dbKey == null || dbValue == null) {
                return result;
            }
            Gson gson = new Gson();
            Map<String, Object> input = gson.fromJson(new String(dbValue), Map.class);

            // String[] 2 Set could be simpler?
            for (String var : varList) {
                Object obj = input.get(var);
                List<String> forGlobal = new ArrayList<>();
                forGlobal.add(var);
                //may be it is global dimension :
                // ip__visit_dynamic_countXXXX,存储在global维度
                //主要是为了取top100的那些变量值
                if (obj == null && !"global".equals(dimension)) {
                    Map<String, Object> globalQuery = getStatistic("__GLOBAL__", "global", forGlobal);
                    result.put(var, globalQuery.get(var));
                } else {
                    result.put(var, ResultFormatter.parse(hourTimeMillis, obj));
                }

            }
        } catch (Exception e) {
            LOGGER.error(String.format("!!!!!!key: %s, dimension: %s, varlist: %s", key, dimension, varList), e);
        }
        return result;
    }

    /**
     * Format a key's statistic cache hashmap to OfflineSlot storage convenient.
     *
     * @param o a HashMap which should have key: key, dimension, and bunch of variables
     * @return com.threathunter.nebula.slot.offline.OfflineSlotDataObj
     **/
    public OfflineSlotDataObj format(final Map o) {
        byte[] key = slotUtils.get_stat_key((String) o.get("key"), (String) o.get("dimension"));
        if (key == null) {
            return null;
        }
        Gson gson = new Gson();
        // Mutable need getValue otherwise serialize will get unexpected data.
        for (Object k : o.keySet()) {
            Object v = o.get(k);
            if (v instanceof Mutable) {
                o.put(k, ((Mutable) v).getValue());
            }
            if (v instanceof Map) {
                for (Object vk : ((Map) v).keySet()) {
                    Object vv = ((Map) v).get(vk);
                    if (vv instanceof Mutable) {
                        ((Map) v).put(vk, ((Mutable) vv).getValue());
                    }
                }
            }
        }
        byte[] value = gson.toJson(o).getBytes();
        OfflineSlotDataObj dataObj = new OfflineSlotDataObj(key, value);
        return dataObj;
    }

    public void add(byte[] key, byte[] value) {
        db.put(key, value);
    }

    public void add_batch(byte[] key, byte[] value) {
        addBuffer.put(key, value);
        if (addBuffer.size() >= ADD_BUFFER_LIMIT) {
            SlotMetricsHelper.getInstance().addMetrics("slot.flush", (double) addBuffer.size());
            flush();
        }
    }

    public void flush() {
        WriteBatch rb = null;
        try {
            rb = this.db.createWriteBatch();
            for (Map.Entry<byte[], byte[]> entry : addBuffer.entrySet()) {
                LOGGER.debug(">>>>>>key:{}  value:{}", new String(entry.getKey(), Charset.forName("utf-8")), new String(entry.getValue(), Charset.forName("utf-8")));
                rb.put(entry.getKey(), entry.getValue());
            }

            this.db.write(rb);
        } catch (Exception ex) {
            ex.printStackTrace();
            LOGGER.error("Fail: fail to write batch to LevelDB.", ex);
            return;
        } finally {
            if (rb != null) {
                try {
                    rb.close();
                } catch (IOException ignore) {
                    ignore.printStackTrace();
                    LOGGER.error("Fail: fail to close LevelDB writebatch object.", ignore);
                    return;
                }
            }
        }
        // Any Exception will not send success metrics and clean the addBuffer.
        SlotMetricsHelper.getInstance().addMetrics("store.slot.success", (double) addBuffer.size());
        addBuffer.clear();
    }

    public void stop() {
        LOGGER.debug("ZJP.OfflineSlotDataHelper.stop");
        if (this.writeMode) {
            if (running) {
                running = false;
                this.flush();
                try {
                    this.db.close();
                } catch (IOException e) {
                    LOGGER.error("error when close level db resources", e);
                } finally {
                    this.moveSlotDataTempDir(this.fullHourPath);
                }
            }
        }
    }

    @Override
    public void start() {
        this.running = true;
    }

    public void store(final Map mapData) {
        LOGGER.debug("ZJP.OfflineSlotDataHelper.store: " + new Gson().toJson(mapData));
        OfflineSlotDataObj dataObj = this.format(mapData);
        if (dataObj != null) {
            add_batch(dataObj.getKey(), dataObj.getValue());
        }
    }

    private void deleteOriginSlotDataDir(final String fullHourPath) {
        File tempSlotDataDir = new File(String.format("%s/%s", fullHourPath, "data_tmp"));
        File slotDataDir = new File(String.format("%s/%s", fullHourPath, "data"));

        if (slotDataDir.exists()) {
            LOGGER.warn("delete temp slot data dir.");
            try {
                OfflineSlotDataHelper.deleteFileOrFolder(slotDataDir.getPath());
            } catch (Exception e) {
                LOGGER.error("failed to delete slot data dir.", e);
                throw new RuntimeException("level db dir cannot be deleted: " + slotDataDir.getPath());
            }
            if (slotDataDir.exists()) {
                LOGGER.error("failed to delete slot data dir.");
                throw new RuntimeException("level db dir cannot be deleted: " + slotDataDir.getPath());
            }
        }

        if (tempSlotDataDir.exists()) {
            LOGGER.warn("delete temp slot data dir.");
            try {
                OfflineSlotDataHelper.deleteFileOrFolder(tempSlotDataDir.getPath());
                if (tempSlotDataDir.exists()) {
                    LOGGER.error("failed to delete temp slot data dir.");
                    throw new RuntimeException("temp level db dir cannot be deleted: " + tempSlotDataDir.getPath());
                }
                LOGGER.error("success in deleting temp slot data dir.");
            } catch (Exception e) {
                LOGGER.error("failed to delete temp slot data dir.");
                throw new RuntimeException("temp level db dir cannot be deleted: " + tempSlotDataDir.getPath());
            }
        }
        if (!tempSlotDataDir.mkdir()) {
            LOGGER.error("failed to create temp slot data dir.");
            throw new RuntimeException("temp level db dir cannot be created: " + tempSlotDataDir.getPath());
        }
    }
    private void moveSlotDataTempDir(final String fullHourPath) {
        File tempSlotDataDir = new File(String.format("%s/%s", fullHourPath, "data_tmp"));
        File slotDataDir = new File(fullHourPath + "/data");
        try {
            tempSlotDataDir.renameTo(slotDataDir);
        } catch (Exception e) {
            LOGGER.error("failed to move temp dir.", e);
            throw new RuntimeException(e);
        }
        LOGGER.info("success move data_tmp dir to data");
    }

    public static void deleteFileOrFolder(final String pathString) throws IOException {
        File file = new File(pathString);
        if (file.exists()) {
            Files.walkFileTree(file.toPath(), new SimpleFileVisitor<Path>(){
                @Override public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs)
                        throws IOException {
                    Files.delete(file);
                    return CONTINUE;
                }

                @Override public FileVisitResult visitFileFailed(final Path file, final IOException e) {
                    return handleException(e);
                }

                private FileVisitResult handleException(final IOException e) {
                    e.printStackTrace(); // replace with more robust error handling
                    return TERMINATE;
                }

                @Override public FileVisitResult postVisitDirectory(final Path dir, final IOException e)
                        throws IOException {
                    if(e!=null)return handleException(e);
                    Files.delete(dir);
                    return CONTINUE;
                }
            });
        }
    }

}
