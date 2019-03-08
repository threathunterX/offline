package com.threathunter.bordercollie.slot.util;

import com.threathunter.common.Identifier;
import com.threathunter.model.VariableMeta;
import com.threathunter.variable.DimensionType;
import com.google.common.collect.ImmutableMap;

import java.util.*;

/**
 * Created by daisy on 16/5/30.
 */
public class SlotVariableMetaRegister {
    private static final SlotVariableMetaRegister REGISTER = new SlotVariableMetaRegister();

    private volatile ImmutableMap<DimensionType, List<VariableMeta>> dimensionedVariables;
    private volatile ImmutableMap<Identifier, VariableMeta> metaMap;
    private volatile long lastUpdateTime = -1;

    private SlotVariableMetaRegister() {
    }

    public static SlotVariableMetaRegister getInstance() {
        return REGISTER;
    }

    public List<VariableMeta> getAllMetas() {
        return metaMap.values().asList();
    }

    public void update(final List<VariableMeta> metaList) {
        metaList.forEach(meta -> {
            // TODO srcId setting
            if (meta.getPropertyCondition() != null) {
                meta.getPropertyCondition().getSrcProperties().forEach(p -> {
                    if (p.getIdentifier() == null) {
                        p.setIdentifier(meta.getSrcVariableMetasID().get(0));
                    }
                });
            }
        });

        Map<DimensionType, List<VariableMeta>> dimensionVariableMetaListMap = new HashMap<>();
        Map<Identifier, VariableMeta> variableMetaMap = new HashMap<>();
        List<VariableMeta> basicVariables = new ArrayList<>();

        metaList.forEach(meta -> variableMetaMap.put(Identifier.fromKeys(meta.getApp(), meta.getName()), meta));

        // filter the event variables out.
        metaList.stream().filter(m -> m.getDimension().isEmpty()).forEach(m -> basicVariables.add(m));
        metaList.stream().filter(m -> !m.getDimension().isEmpty()).forEach(meta -> {
                    dimensionVariableMetaListMap.computeIfAbsent(DimensionType.valueOf(meta.getDimension().toUpperCase()), d -> new ArrayList<>()).add(meta);
                }
        );

        // append the event variables to each dimension variables' list, instead of merge both list when getDimensionedVariables.
        dimensionVariableMetaListMap.values().forEach(v -> {
            v.addAll(basicVariables);
            Collections.sort(v, (v1, v2) -> v1.getPriority() - v2.getPriority());
        });

        dimensionedVariables = ImmutableMap.copyOf(dimensionVariableMetaListMap);
        metaMap = ImmutableMap.copyOf(variableMetaMap);
        lastUpdateTime = SystemClock.getCurrentTimestamp();
    }

    public List<VariableMeta> getDimensionedVariables(final DimensionType dimensionType) {
        return dimensionedVariables.get(dimensionType);
    }

    public Map<DimensionType, List<VariableMeta>> getAllDimensionVariables() {
        return this.dimensionedVariables;
    }

    public VariableMeta getMeta(final Identifier id) {
        return this.metaMap.get(id);
    }

    public boolean containsMeta(final Identifier id) {
        return this.metaMap.containsKey(id);
    }

    public long getUpdateTimestamp() {
        return lastUpdateTime;
    }
}
