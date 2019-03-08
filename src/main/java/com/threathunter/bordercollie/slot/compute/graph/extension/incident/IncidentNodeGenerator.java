package com.threathunter.bordercollie.slot.compute.graph.extension.incident;

import com.threathunter.bordercollie.slot.compute.cache.StorageType;
import com.threathunter.common.Identifier;
import com.threathunter.variable.DimensionType;
import com.threathunter.variable.exception.NotSupportException;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by daisy on 17/4/20.
 */
public class IncidentNodeGenerator {

    public static Map<Identifier, IncidentNode> getIncidentNode(final DimensionType dimensionType, final StorageType storageType) {
        if (dimensionType.equals(DimensionType.IP)) {
            Map<Identifier, IncidentNode> map = new HashMap<>();
            map.put(Identifier.fromKeys("nebula", "ip__visit_incident_score__1h__slot"), new IPMaxSceneScoreNode(storageType));
            map.put(Identifier.fromKeys("nebula", "ip_tag__visit_incident_count_top20__1h__slot"), new DimensionTagHitCountNode(storageType, DimensionType.IP));
            map.put(Identifier.fromKeys("nebula", "ip_scene_strategy__visit_incident_group_count__1h__slot"), new IPSceneStrategyCountNode(storageType));
            map.put(Identifier.fromKeys("nebula", "ip__visit_incident_max_rate__1h__slot"), new IPIncidentMaxRateNode(storageType));
            map.put(Identifier.fromKeys("nebula", "ip_scene__visit_incident_group_count__1h__slot"), new DimensionSceneHitCountNode(DimensionType.IP, storageType));
            return map;
        }
//        if (dimensionType.equals(DimensionType.GLOBAL)) {
//            Map<Identifier, IncidentNode> map = new HashMap<>();
//            map.put(Identifier.fromKeys("nebula", "total__visit__visitor_incident_count__1h__slot"), new NamedSceneHitCountNode("VISITOR", storageType, "total__visit__visitor_incident_count__1h__slot"));
//            map.put(Identifier.fromKeys("nebula", "total__visit__account_incident_count__1h__slot"), new NamedSceneHitCountNode("ACCOUNT", storageType, "total__visit__account_incident_count__1h__slot"));
//            map.put(Identifier.fromKeys("nebula", "total__visit__order_incident_count__1h__slot"), new NamedSceneHitCountNode("ORDER", storageType, "total__visit__order_incident_count__1h__slot"));
//            map.put(Identifier.fromKeys("nebula", "total__visit__marketing_incident_count__1h__slot"), new NamedSceneHitCountNode("MARKETING", storageType, "total__visit__marketing_incident_count__1h__slot"));
//            map.put(Identifier.fromKeys("nebula", "total__visit__transaction_incident_count__1h__slot"), new NamedSceneHitCountNode("TRANSACTION", storageType, "total__visit__transaction_incident_count__1h__slot"));
//            map.put(Identifier.fromKeys("nebula", "total__visit__other_incident_count__1h__slot"), new NamedSceneHitCountNode("OTHER", storageType, "total__visit__other_incident_count__1h__slot"));
//            return map;
//        }
        if (dimensionType.equals(DimensionType.DID)) {
            Map<Identifier, IncidentNode> map = new HashMap<>();
            map.put(Identifier.fromKeys("nebula", "did_scene__visit_incident_group_count__1h__slot"), new DimensionSceneHitCountNode(DimensionType.DID, storageType));
            map.put(Identifier.fromKeys("nebula", "did_tag__visit_incident_count_top20__1h__slot"), new DimensionTagHitCountNode(storageType, DimensionType.DID));
            return map;
        }
        if (dimensionType.equals(DimensionType.UID)) {
            Map<Identifier, IncidentNode> map = new HashMap<>();
            map.put(Identifier.fromKeys("nebula", "uid_scene__visit_incident_group_count__1h__slot"), new DimensionSceneHitCountNode(DimensionType.UID, storageType));
            map.put(Identifier.fromKeys("nebula", "uid_tag__visit_incident_count_top20__1h__slot"), new DimensionTagHitCountNode(storageType, DimensionType.UID));
            return map;
        }
        if (dimensionType.equals(DimensionType.OTHER)) {
            Map<Identifier, IncidentNode> map = new HashMap<>();
            map.put(Identifier.fromKeys("nebula", "scene_strategy__visit_incident_count_top20__1h__slot"), new SceneStrategyHitCountNode(storageType));
//            map.put(Identifier.fromKeys("nebula", "page__visit__visitor_incident_count__1h__slot"), new PageVisitorHitCountNode(storageType));
            return map;
        }
        throw new NotSupportException("dimension type not support: " + dimensionType);
    }
}
