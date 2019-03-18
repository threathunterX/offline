package com.threathunter.mock.util;

import com.threathunter.model.Event;
import org.apache.commons.lang3.mutable.MutableLong;

import java.util.*;

/**
 * 
 */
public class IncidentEventMaker implements EventMaker {
    private Map<String, StrategyInfo> strategyInfos;
    private Map<String, List<String>> sceneStrategies;
    private String[] tags;
    private String[] categories;
    private EventMaker eventMaker;

    private Random r = new Random();

    public IncidentEventMaker(int moCount, EventMaker rawEventMaker) {
        initial(moCount);
        this.eventMaker = rawEventMaker;
    }

    public void updateRawEventMaker(EventMaker eventMaker) {
        this.eventMaker = eventMaker;
    }

    public IncidentEventMaker(int moCount) {
        this(moCount, new HttpDynamicEventMaker(moCount));
    }

    private void initial(int moCount) {
        strategyInfos = new HashMap<>();
        tags = new String[moCount];
        for (int i = 0; i < tags.length; i++) {
            tags[i] = "tag_" + i;
        }
        sceneStrategies = new HashMap<>();
        categories = new String[]{"OTHER", "VISITOR", "ACCOUNT", "MARKETING", "ORDER", "TRANSACTION"};
        for (int i = 0; i < moCount; i++) {
            StrategyInfo info = new StrategyInfo();
            String strategyName = "strategy_" + (i % moCount);
            info.setId(strategyName);
            String scene = categories[r.nextInt(categories.length)];
            sceneStrategies.computeIfAbsent(scene, s -> new ArrayList<>()).add(strategyName);
            info.setScene(scene);

            Set<String> tagsSet = new HashSet<>();
            for (int k = 0; k < r.nextInt(tags.length) + 1; k++) {
                tagsSet.add(tags[r.nextInt(tags.length)]);
            }
            info.setTags(new ArrayList<>(tagsSet));

            strategyInfos.put(info.getId(), info);
        }

        sceneStrategies.forEach((scene, strategies) -> {
            int size = strategies.size();
            int left = 100;
            for (String strategy : strategies) {
                if (size > 1) {
                    int curScore = r.nextInt(left);
                    strategyInfos.get(strategy).setScore(curScore);
                    left -= curScore;
                    size--;
                } else {
                    strategyInfos.get(strategy).setScore(left);
                }
            }
        });
    }

    @Override
    public Event nextEvent() {
        Event event = eventMaker.nextEvent();
        int count = r.nextInt(2) + 1;
        Set<String> ids = new HashSet<>();
        for (int i = 0; i < count; i++) {
            ids.add(strategyInfos.get("strategy_" + r.nextInt(strategyInfos.size())).getId());
        }
        event.getPropertyValues().putAll(getIncidentInfo(new ArrayList<>(ids)));
        return event;
    }

    @Override
    public void close() {

    }

    @Override
    public String getEventName() {
        return "HTTP_DYNAMIC";
    }

    private Map<String, Object> getIncidentInfo(List<String> strategyIds) {
        Map<String, Object> properties = new HashMap<>();
        properties.put("notices", String.join(",", strategyIds));
        properties.put("noticelist", strategyIds);

        Map<String, MutableLong> sceneScore = new HashMap<>();
        Map<String, List<String>> sceneStrategies = new HashMap<>();
        Set<String> tags = new HashSet<>();
        for (String id : strategyIds) {
            tags.addAll(strategyInfos.get(id).getTags());
            sceneStrategies.computeIfAbsent(strategyInfos.get(id).getScene(), s -> new ArrayList<>()).add(id);
        }
        sceneStrategies.forEach((scene, strategies) ->
                strategies.forEach(strategy -> sceneScore.computeIfAbsent(scene, s -> new MutableLong(0)).add(
                        strategyInfos.get(strategy).getScore())));
        properties.put("scores", sceneScore);
        properties.put("strategies", sceneStrategies);
        properties.put("tags", new ArrayList<>(tags));

        return properties;
    }

    private static class StrategyInfo {
        private String id;
        private List<String> tags;
        private long score;
        private String scene;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public List<String> getTags() {
            return tags;
        }

        public void setTags(List<String> tags) {
            this.tags = tags;
        }

        public long getScore() {
            return score;
        }

        public void setScore(long score) {
            this.score = score;
        }

        public String getScene() {
            return scene;
        }

        public void setScene(String scene) {
            this.scene = scene;
        }
    }
}
