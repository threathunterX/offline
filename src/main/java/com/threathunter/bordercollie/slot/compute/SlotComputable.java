package com.threathunter.bordercollie.slot.compute;

import com.threathunter.bordercollie.slot.compute.graph.DimensionVariableGraphManager;
import com.threathunter.bordercollie.slot.compute.graph.extension.incident.IncidentVariableGraphManager;
import com.threathunter.model.Event;
import com.threathunter.model.VariableMeta;
import com.threathunter.variable.DimensionType;

import java.util.List;
import java.util.Map;

/**
 * this interface is a collection of core methods that support the
 * computation of slot. The input method includes {@link SlotComputable#add}
 * the module online, offline fly all the events in to slot use there methods.
 * there are two models in the computing, waiting or no waiting, the default
 * config (engine.mode.wait) is set to false.By contrast, if set to true,
 * which mean if the events is too much to add, then slot will discard some
 * events. The output method {@link SlotComputable#getAllSlots()}, return all
 * the slot windows, in the format of Map<Long, SlotWindow>
 * <p>
 * <p>When query results from {@link SlotEngine}, the suggest query api is
 * {@link SlotQuery}, please not query direct from slotComputable.
 *
 * @author Yuan Yi
 * @see SlotEngine
 * @since 2.15
 */
public interface SlotComputable {
    Long add(Event event);

    Map<Integer, Long> add(List<Event> events);

    Map<Long, SlotWindow> getAllSlots();

    SlotWindow getSlot(Long slotPoint);

    void removeSlot(Long slotPoint);

    void update(List<VariableMeta> metas);

    Map<DimensionType, DimensionVariableGraphManager> getCurrentCommonManagers();

    Map<DimensionType, IncidentVariableGraphManager> getCurrentIncidentManagers();

    void start();

    void stop();

    List<VariableMeta> getMetas();

    boolean isStopped();

    boolean save();
}
