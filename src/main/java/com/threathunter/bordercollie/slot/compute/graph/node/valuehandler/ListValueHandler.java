package com.threathunter.bordercollie.slot.compute.graph.node.valuehandler;

import com.threathunter.bordercollie.slot.compute.VariableDataContext;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapper;
import com.threathunter.common.NamedType;

import java.util.List;

/**
 * 
 */
public abstract class ListValueHandler<R> implements ValueHandler<List<Object>, R> {
    protected final int indexCount;
    protected final CacheWrapper cacheWrapper;

    protected ListValueHandler(final int indexCount, final CacheWrapper wrapper) {
        this.indexCount = indexCount;
        this.cacheWrapper = wrapper;
    }

    @Override
    public final NamedType valueType() {
        return NamedType.LIST;
    }

    public static class GlobalListValueHandler<R> extends ListValueHandler<R> {

        public GlobalListValueHandler(final CacheWrapper wrapper) {
            super(0, wrapper);
        }

        @Override
        public R addValue(final List<Object> value, final VariableDataContext context) {
            value.forEach(v -> this.cacheWrapper.addData(value));
            return (R) this.cacheWrapper.getData();
        }
    }

    public static class FirstListValueHandler<R> extends ListValueHandler<R> {
        private final String firstKeyName;

        public FirstListValueHandler(final CacheWrapper wrapper, final String firstKey) {
            super(0, wrapper);
            this.firstKeyName = firstKey;
        }

        @Override
        public R addValue(final List<Object> value, final VariableDataContext context) {
            value.forEach(v -> this.cacheWrapper.addData(value, (String) context.getFromContext(firstKeyName)));
            return (R) this.cacheWrapper.getData((String) context.getFromContext(firstKeyName));
        }
    }

    public static class SecondaryListValueHandler<R> extends ListValueHandler<R> {
        private final String firstKeyName;
        private final String secondKeyName;

        public SecondaryListValueHandler(final CacheWrapper wrapper, final String firstKey, final String secondKey) {
            super(0, wrapper);
            this.firstKeyName = firstKey;
            this.secondKeyName = secondKey;
        }

        @Override
        public R addValue(final List<Object> value, final VariableDataContext context) {
            value.forEach(v -> this.cacheWrapper.addData(value, (String) context.getFromContext(firstKeyName)));
            return (R) this.cacheWrapper.getData((String) context.getFromContext(firstKeyName),
                    (String) context.getFromContext(secondKeyName));
        }
    }
}
