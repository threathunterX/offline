package com.threathunter.bordercollie.slot.compute;

import com.threathunter.common.Identifier;

import java.util.Collection;

/**
 * query the variable of slot, default include all the previous windows
 * creates by engine,the variable contain all the 3 windows variables. 1, 2, 3
 * ...n means the window instance.
 *
 * eg:
 * (frame 1)(frame2)(frame 3)...(frame n)
 * |---1---|---2---|---3---|...|---n---|
 *
 * @author  Yuan Yi
 * @since 2.15
 */
public interface SlotQueryable {

    Object queryCurrent(Identifier identifier, Collection<String> key);

    Object queryPrevious(Identifier identifier, Collection<String> key);

    Object mergePrevious(Identifier identifier, Collection<String> key);

    Object mergePrevious(Identifier identifier, String key);
}
