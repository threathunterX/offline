package com.threathunter.bordercollie.slot.compute;

import com.threathunter.bordercollie.slot.compute.graph.DimensionVariableGraphManager;
import com.threathunter.bordercollie.slot.compute.graph.extension.incident.IncidentVariableGraphManager;
import com.threathunter.model.VariableMeta;
import com.threathunter.variable.DimensionType;

import java.util.List;
import java.util.Map;

/**
 * Created by yy on 17-11-7.
 */
public class SlotWindow {
    private final Long id;
    private List<VariableMeta> metas;
    private Map<DimensionType, DimensionVariableGraphManager> dimensionedGraphManagers;
    private Map<DimensionType, IncidentVariableGraphManager> dimensionedIncidentVariableGraphManager;

    public SlotWindow(Long id) {
        this.id = id;
    }


    public void merge(SlotWindow slotWindow) {
     /*   if(slotWindow == null )
            throw new RuntimeException("merge null slotWindow");
        getCommondVariableMap().forEach((k,v)->{
            CacheNode<Object> node=slotWindow.getCommondVariableMap().get(k);
            if(node!=null){
                v.merge(node);
            }
        });*/
    }

    public Map<DimensionType, DimensionVariableGraphManager> getDimensionedGraphManagers() {
        return dimensionedGraphManagers;
    }

    public void setDimensionedGraphManagers(Map<DimensionType, DimensionVariableGraphManager> dimensionedGraphManagers) {
        this.dimensionedGraphManagers = dimensionedGraphManagers;
    }

    public Map<DimensionType, IncidentVariableGraphManager> getDimensionedIncidentVariableGraphManager() {
        return dimensionedIncidentVariableGraphManager;
    }

    public void setDimensionedIncidentVariableGraphManager(Map<DimensionType, IncidentVariableGraphManager> dimensionedIncidentVariableGraphManager) {
        this.dimensionedIncidentVariableGraphManager = dimensionedIncidentVariableGraphManager;
    }

    public List<VariableMeta> getMetas() {
        return metas;
    }

    public void setMetas(List<VariableMeta> metas) {
        this.metas = metas;
    }
}
