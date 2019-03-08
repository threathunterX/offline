package com.threathunter.bordercollie.slot.api;

import com.threathunter.bordercollie.slot.util.PathHelper;
import com.threathunter.common.ShutdownHookManager;
import com.threathunter.config.CommonDynamicConfig;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.impl.Iq80DBFactory;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Created by daisy on 17-10-19
 */
public class OfflineLevelDbCache {
    private static final Logger LOGGER = LoggerFactory.getLogger(OfflineLevelDbCache.class);
    private static final OfflineLevelDbCache INSTANCE = new OfflineLevelDbCache();

    private final String persistPath = CommonDynamicConfig.getInstance().getString("persist_path", PathHelper.getModulePath() + "/persistent");
    private final Cache<Long, DB> cache;

    private OfflineLevelDbCache() {
        this.cache = CacheBuilder.newBuilder().expireAfterAccess(5, TimeUnit.MINUTES).removalListener((notification) -> {
            DB db = (DB) notification.getValue();
            try {
                db.close();
            } catch (Exception e) {
                LOGGER.error(String.format("close leveldb error, hour: %s", notification.getKey()), e);
            }
        }).build();

        ShutdownHookManager.get().addShutdownHook(() -> this.cache.invalidateAll(), 100);
    }

    public static OfflineLevelDbCache getInstance() {
        return INSTANCE;
    }

    public void close() {
        this.cache.invalidateAll();
    }

    public DB getLevelDb(final long hourTimeMillis, final boolean createIfMissing) {
        try {
            //offline mode, in write mode, close after stop.
            if (createIfMissing) {
                String path = String.format("%s/%s/data", this.persistPath, new DateTime(hourTimeMillis).toString("yyyyMMddHH"));
                Options options = new Options();
                options.createIfMissing(createIfMissing);
                LOGGER.info(">>>>>>>info db path:{}", path);
                final DB ret = Iq80DBFactory.factory.open(new File(path), options);
                ShutdownHookManager.get().addShutdownHook(() -> {
                    try {
                        ret.close();
                    } catch (IOException e) {
                        LOGGER.error("open level db error, time: " + new DateTime(hourTimeMillis).toString("yyyyMMddHH"), e);
                    }
                }, 100);
                return ret;
            }
            return this.cache.get(hourTimeMillis, () -> {
                String path = String.format("%s/%s/data", this.persistPath, new DateTime(hourTimeMillis).toString("yyyyMMddHH"));
                Options options = new Options();
                options.createIfMissing(createIfMissing);
                LOGGER.info(">>>>>>>info db path:{}", path);
                return Iq80DBFactory.factory.open(new File(path), options);
            });
        } catch (Exception e) {
            LOGGER.error("open level db error, time: " + new DateTime(hourTimeMillis).toString("yyyyMMddHH"), e);
            return null;
        }
    }
}
