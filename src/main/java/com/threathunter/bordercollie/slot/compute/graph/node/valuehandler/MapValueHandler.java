package com.threathunter.bordercollie.slot.compute.graph.node.valuehandler;

import com.threathunter.bordercollie.slot.compute.VariableDataContext;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapper;
import com.threathunter.common.NamedType;

import java.util.Map;

/**
 * Created by daisy on 17/4/28.
 */
public abstract class MapValueHandler<R> implements ValueHandler<Map<String, Object>, R> {
    protected final int indexCount;
    protected final CacheWrapper cacheWrapper;

    protected MapValueHandler(final int indexCount, final CacheWrapper wrapper) {
        this.indexCount = indexCount;
        this.cacheWrapper = wrapper;
    }

    @Override
    public final NamedType valueType() {
        return NamedType.MAP;
    }

    public static class FirstMapValueHandler<R> extends MapValueHandler {

        public FirstMapValueHandler(final boolean mergeMapKey, final CacheWrapper wrapper) {
            super(mergeMapKey ? 1 : 0, wrapper);
        }

        @Override
        public Object addValue(Object value, VariableDataContext context) {
            return null;
        }
    }
}
