package com.threathunter.bordercollie.slot.compute.graph.node.valuehandler;

import com.threathunter.bordercollie.slot.compute.VariableDataContext;
import com.threathunter.bordercollie.slot.compute.cache.wrapper.CacheWrapper;
import com.threathunter.common.NamedType;


/**
 * 
 */
public abstract class SimpleValueHandler<R> implements ValueHandler<R, R> {
    protected int indexCount;
    protected CacheWrapper cacheWrapper;

    protected SimpleValueHandler(final int indexCount, final CacheWrapper wrapper) {
        this.indexCount = indexCount;
        this.cacheWrapper = wrapper;
    }

    @Override
    public final NamedType valueType() {
        return NamedType.OBJECT;
    }

    public static class GlobalSimpleValueHandler<R> extends SimpleValueHandler {

        public GlobalSimpleValueHandler(final CacheWrapper wrapper) {
            super(0, wrapper);
        }

        @Override
        public R addValue(final Object value, final VariableDataContext context) {
            return (R) this.cacheWrapper.addData(value);
        }
    }

    public static class FirstSimpleValueHandler<R> extends SimpleValueHandler {
        private final String firstKeyName;

        public FirstSimpleValueHandler(final CacheWrapper wrapper, final String keyName) {
            super(1, wrapper);
            this.firstKeyName = keyName;
        }

        @Override
        public R addValue(final Object value, final VariableDataContext context) {
            return (R) this.cacheWrapper.addData(value, (String) context.getFromContext(firstKeyName));
        }
    }

    public static class SecondarySimpleValueHandler<R> extends SimpleValueHandler {
        private final String firstKeyName;
        private final String secondKeyName;

        public SecondarySimpleValueHandler(final CacheWrapper wrapper, final String firstKey, final String secondKey) {
            super(2, wrapper);
            this.firstKeyName = firstKey;
            this.secondKeyName = secondKey;
        }

        @Override
        public R addValue(final Object value, final VariableDataContext context) {
            return (R) this.cacheWrapper.addData(value, (String) context.getFromContext(firstKeyName),
                    (String) context.getFromContext(secondKeyName));
        }
    }
}
