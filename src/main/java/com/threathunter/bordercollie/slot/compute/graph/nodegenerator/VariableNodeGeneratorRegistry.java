package com.threathunter.bordercollie.slot.compute.graph.nodegenerator;

import com.threathunter.bordercollie.slot.util.ClassBasedRegistry;
import com.threathunter.model.VariableMeta;
import com.threathunter.variable.meta.*;

/**
 * @author daisy
 * @since 1.4
 */
public class VariableNodeGeneratorRegistry {
    private static final ClassBasedRegistry<VariableMeta, VariableNodeGenerator> registry
            = new ClassBasedRegistry<>(VariableMeta.class);

    static {
        registry.register(EventVariableMeta.class, EventVariableNodeGenerator.class);
        registry.register(FilterVariableMeta.class, FilterVariableNodeGenerator.class);
        registry.register(AggregateVariableMeta.class, AggregateVariableNodeGenerator.class);
        registry.register(SequenceVariableMeta.class, SequenceVariableNodeGenerator.class);
        registry.register(DualVariableMeta.class, DualvarVariableNodeGenerator.class);
        registry.register(TopVariableMeta.class, TopVariableNodeGenerator.class);
    }

    public static void registerVariable(Class<? extends VariableMeta> v, Class<? extends VariableNodeGenerator> g) {
        registry.register(v, g);
    }

    public static Class<? extends VariableNodeGenerator> getGenerator(Class<? extends VariableMeta> v) {
        return registry.get(v);
    }
}
