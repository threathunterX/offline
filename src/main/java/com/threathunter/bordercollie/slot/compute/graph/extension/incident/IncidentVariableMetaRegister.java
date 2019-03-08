package com.threathunter.bordercollie.slot.compute.graph.extension.incident;

import com.threathunter.common.Identifier;
import com.threathunter.model.Property;
import com.threathunter.model.VariableMeta;
import com.threathunter.variable.DimensionType;

import java.util.*;

/**
 * Created by daisy on 17/4/19.
 */
public class IncidentVariableMetaRegister {
    private static final Map<DimensionType, List<VariableMeta>> DIMENSION_INCIDENT_VARIABLE_METAS = new HashMap<>();
    private static final Map<Identifier, VariableMeta> INCIDENT_VARIABLE_METAS = new HashMap<>();

    static {
        List<VariableMeta> ipDimensionMetas = new ArrayList<>();
        ipDimensionMetas.add(new IncidentVariableMeta(
                "ip", "nebula", "ip__visit_incident_score__1h__slot", true, false, new ArrayList<>(Arrays.asList(Property.buildStringProperty("c_ip")))));
        ipDimensionMetas.add(new IncidentVariableMeta(
                "ip", "nebula", "ip__visit_incident_score_top100__1h__slot", true, false,
                new ArrayList<>(Arrays.asList(Property.buildStringProperty("c_ip")))));
        ipDimensionMetas.add(new IncidentVariableMeta(
                "ip", "nebula", "ip_tag__visit_incident_count_top20__1h__slot", false, true,
                new ArrayList<>(Arrays.asList(Property.buildStringProperty("c_ip"), Property.buildStringProperty("tag")))));
        ipDimensionMetas.add(new IncidentVariableMeta(
                "ip", "nebula", "ip_scene_strategy__visit_incident_group_count__1h__slot", false, true,
                new ArrayList<>(Arrays.asList(Property.buildStringProperty("c_ip"), Property.buildStringProperty("scene")))));
        ipDimensionMetas.add(new IncidentVariableMeta(
                "ip", "nebula", "ip__visit_incident_max_rate__1h__slot", false, false,
                new ArrayList<>(Arrays.asList(Property.buildStringProperty("c_ip")))));
        ipDimensionMetas.add(new IncidentVariableMeta(
                "ip", "nebula", "ip_scene__visit_incident_group_count__1h__slot", false, false,
                new ArrayList<>(Arrays.asList(Property.buildStringProperty("c_ip"), Property.buildStringProperty("scene")))));
        DIMENSION_INCIDENT_VARIABLE_METAS.put(DimensionType.IP, ipDimensionMetas);

//        List<VariableMeta> globalDimensionMetas = new ArrayList<>();
//        globalDimensionMetas.add(new IncidentVariableMeta(
//                "global", "nebula", "total__visit__visitor_incident_count__1h__slot", false, false));
//        globalDimensionMetas.add(new IncidentVariableMeta(
//                "global", "nebula", "total__visit__account_incident_count__1h__slot", false, false));
//        globalDimensionMetas.add(new IncidentVariableMeta(
//                "global", "nebula", "total__visit__order_incident_count__1h__slot", false, false));
//        globalDimensionMetas.add(new IncidentVariableMeta(
//                "global", "nebula", "total__visit__marketing_incident_count__1h__slot", false, false));
//        globalDimensionMetas.add(new IncidentVariableMeta(
//                "global", "nebula", "total__visit__transaction_incident_count__1h__slot", false, false));
//        globalDimensionMetas.add(new IncidentVariableMeta(
//                "global", "nebula", "total__visit__other_incident_count__1h__slot", false, false));
//        DIMENSION_INCIDENT_VARIABLE_METAS.put(DimensionType.GLOBAL, globalDimensionMetas);

        // TODO one thread
        List<VariableMeta> otherDimensionMetas = new ArrayList<>();
        otherDimensionMetas.add(new IncidentVariableMeta(
//                "other", "nebula", "scene__visit__strategy_incident_count__1h__slot", false, true));
                "other", "nebula", "scene_strategy__visit_incident_count_top20__1h__slot", false, true,
                new ArrayList<>(Arrays.asList(Property.buildStringProperty("scene"), Property.buildStringProperty("strategy")))));
//         TODO hardcode here, just don't want to create a new thread
//        otherDimensionMetas.add(new IncidentVariableMeta(
//                "other", "nebula", "page__visit__visitor_incident_count__1h__slot", false, false));
        DIMENSION_INCIDENT_VARIABLE_METAS.put(DimensionType.OTHER, otherDimensionMetas);

        List<VariableMeta> didDimensionMetas = new ArrayList<>();
        didDimensionMetas.add(new IncidentVariableMeta(
//                "did", "nebula", "did__visit__scene_incident_count__1h__slot", false, false));
                "did", "nebula", "did_scene__visit_incident_group_count__1h__slot", false, false,
                new ArrayList<>(Arrays.asList(Property.buildStringProperty("did"), Property.buildStringProperty("scene")))));
        didDimensionMetas.add(new IncidentVariableMeta(
                // did_tag__visit_incident_count_top20__1h__slot
                "did", "nebula", "did_tag__visit_incident_count_top20__1h__slot", false, true,
                new ArrayList<>(Arrays.asList(Property.buildStringProperty("did"), Property.buildStringProperty("tag")))));
//        "did", "nebula", "did__visit__tag_dynamic_count__1h__slot", false, true));
        DIMENSION_INCIDENT_VARIABLE_METAS.put(DimensionType.DID, didDimensionMetas);

        List<VariableMeta> userDimensionMetas = new ArrayList<>();
        userDimensionMetas.add(new IncidentVariableMeta(
                "uid", "nebula", "uid_scene__visit_incident_group_count__1h__slot", false, false,
                new ArrayList<>(Arrays.asList(Property.buildStringProperty("uid"), Property.buildStringProperty("scene")))));
        userDimensionMetas.add(new IncidentVariableMeta(
                "uid", "nebula", "uid_tag__visit_incident_count_top20__1h__slot", false, true,
                new ArrayList<>(Arrays.asList(Property.buildStringProperty("uid"), Property.buildStringProperty("tag")))));
        DIMENSION_INCIDENT_VARIABLE_METAS.put(DimensionType.UID, userDimensionMetas);

        DIMENSION_INCIDENT_VARIABLE_METAS.values().forEach(list -> list.forEach(v ->
                INCIDENT_VARIABLE_METAS.put(Identifier.fromKeys(v.getApp(), v.getName()), v)));
    }

    public static List<VariableMeta> getDimensionedMetas(final DimensionType dimensionType) {
        return DIMENSION_INCIDENT_VARIABLE_METAS.get(dimensionType);
    }

    public static VariableMeta getMeta(final Identifier id) {
        return INCIDENT_VARIABLE_METAS.get(id);
    }

    public static Map<DimensionType, List<VariableMeta>> getAllDimensionVariables() {
        return DIMENSION_INCIDENT_VARIABLE_METAS;
    }
}
