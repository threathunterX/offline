package com.threathunter.bordercollie.slot.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
/**
 * Created by daisy on 16/7/20.
 */
public class StrategyInfoCache {
    private static Logger logger = LoggerFactory.getLogger(StrategyInfoCache.class);

    private static final StrategyInfoCache INSTANCE = new StrategyInfoCache();

    public StrategyInfoCache() {
    }

    public static StrategyInfoCache getInstance() {
        return INSTANCE;
    }

    private static final Map<String, Integer> priorMap = new HashMap<>();

    private volatile Map<String, StrategyInfo> cache;
    private volatile long lastUpdateTime = -1;

    private ReentrantReadWriteLock locker = new ReentrantReadWriteLock();

    static {
        priorMap.put("OTHER", 0);
        priorMap.put("VISITOR", 1);
        priorMap.put("ACCOUNT", 2);
        priorMap.put("MARKETING", 3);
        priorMap.put("ORDER", 4);
        priorMap.put("TRANSACTION", 5);
    }

    public void update(List<Map<String, Object>> strategies) {
        locker.writeLock().lock();
        Map<String, StrategyInfo> newCache = new HashMap<>();
        for (Map<String, Object> strategy : strategies) {
            StrategyInfo info = new StrategyInfo();
            info.setCategory((String) strategy.get("category"));
            info.setScore(((Number) strategy.get("score")).longValue());
            info.setTags(new HashSet<>((List<String>) strategy.get("tags")));
            info.setTest((Boolean) strategy.getOrDefault("test", false));
            info.setCheckValue((String) strategy.getOrDefault("checkvalue", "c_ip"));
            info.setProfileScope((strategy.get("scope")).equals("profile"));
            info.setCheckType((String) strategy.getOrDefault("checktype", ""));
            info.setCheckPoints((String) strategy.getOrDefault("checkpoints", ""));
            info.setDecision((String) strategy.getOrDefault("decision", ""));
            info.setExpire(((Number) strategy.get("expire")).longValue());
            info.setTtl(((Number) strategy.getOrDefault("ttl", 300)).longValue());
            info.setRemark((String) strategy.getOrDefault("remark", ""));
            newCache.put((String) strategy.get("name"), info);
            logger.warn("StrategyInfoCache add strategy: {}", strategy);
        }
        cache = newCache;
        locker.writeLock().unlock();
        lastUpdateTime = SystemClock.getCurrentTimestamp();

    }

    public boolean containsStrategy(String strategy) {
        boolean ret;
        locker.readLock().lock();
        if (this == null){
            logger.error("StrategyInfoCache.containsStrategy this is null: {}", this);
            return false;
        }
        if (this.cache == null){
            logger.error("StrategyInfoCache.containsStrategy this.cache is null: {}", this.cache);
            return false;
        }
        if (strategy== null){
            logger.error("StrategyInfoCache.containsStrategy strategy is null: {}", strategy);
            return false;
        }else {
            logger.error("StrategyInfoCache.containsStrategy strategy : {}", strategy);
        }
        try {
            ret = this.cache.containsKey(strategy);
        }catch (Exception e) {
            ret = false;
            logger.error("StrategyInfoCache.containsStrategy error : {}", e);
        }
        locker.readLock().unlock();
        return ret;
    }

    public String getCategory(String strategy) {
        String ret;
        locker.readLock().lock();
        ret =  this.cache.get(strategy).getCategory();
        locker.readLock().unlock();
        return ret;
    }

    public Long getScore(String strategy) {
        Long ret;
        locker.readLock().lock();
        ret = this.cache.get(strategy).getScore();
        locker.readLock().unlock();
        return ret;
    }

    public Set<String> getTags(String strategy) {
        Set<String> ret;
        locker.readLock().lock();
        ret =  this.cache.get(strategy).getTags();
        locker.readLock().unlock();
        return ret;
    }

    public Boolean isTest(String strategy) {
        Boolean ret;
        locker.readLock().lock();
        ret =  this.cache.get(strategy).isTest();
        locker.readLock().unlock();
        return ret;
    }

    public Boolean isProfileScope(String strategy) {
        Boolean ret;
        locker.readLock().lock();
        ret =  this.cache.get(strategy).isProfileScope();
        locker.readLock().unlock();
        return ret;
    }

    public String getPriorCategory(String category1, String category2) {
        if (priorMap.containsKey(category1) && priorMap.containsKey(category2)) {
            return priorMap.get(category1) - priorMap.get(category2) > 0 ? category1 : category2;
        } else {
            if (priorMap.containsKey(category1)) {
                return category1;
            } else {
                return category2;
            }
        }
    }

    public StrategyInfo getStrategyInfo(String strategy) {
        StrategyInfo ret;
        locker.readLock().lock();
        ret = this.cache.get(strategy);
        locker.readLock().unlock();
        return ret;
    }

    public Long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public static class StrategyInfo {
        private Set<String> tags = new HashSet<>();
        private Long score;
        private String category;
        private Boolean test;
        private String checkType;
        private Boolean profileScope;
        private Long expire;
        private Long ttl;
        private String checkValue;
        private String decision;
        private String checkPoints;
        private String remark;

        public Long getExpire() {
            return expire;
        }

        public void setExpire(Long expire) {
            this.expire = expire;
        }

        public String getCheckValue() {
            return checkValue;
        }

        public void setCheckValue(String checkValue) {
            this.checkValue = checkValue;
        }

        public String getDecision() {
            return decision;
        }

        public void setDecision(String decision) {
            this.decision = decision;
        }

        public String getCheckPoints() {
            return checkPoints;
        }

        public void setCheckPoints(String checkPoints) {
            this.checkPoints = checkPoints;
        }

        public String getRemark() {
            return remark;
        }

        public void setRemark(String remark) {
            this.remark = remark;
        }

        public Boolean isProfileScope() {
            return profileScope;
        }

        public void setProfileScope(Boolean profileScope) {
            this.profileScope = profileScope;
        }

        public String getCheckType() {
            return checkType;
        }

        public void setCheckType(String checkType) {
            this.checkType = checkType;
        }

        public Boolean isTest() {
            return test;
        }

        public void setTest(Boolean test) {
            this.test = test;
        }

        public Set<String> getTags() {
            return tags;
        }

        public void setTags(Set<String> tags) {
            if (tags != null) {
                this.tags = tags;
            }
        }

        public Long getScore() {
            return score;
        }

        public void setScore(Long score) {
            this.score = score;
        }

        public String getCategory() {
            return category;
        }

        public void setCategory(String category) {
            this.category = category;
        }

        public Long getTtl() {
            return ttl;
        }

        public void setTtl(Long ttl) {
            this.ttl = ttl;
        }
    }
}
