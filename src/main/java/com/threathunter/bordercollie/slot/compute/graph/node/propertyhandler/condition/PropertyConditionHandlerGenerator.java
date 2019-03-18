package com.threathunter.bordercollie.slot.compute.graph.node.propertyhandler.condition;

import com.threathunter.bordercollie.slot.compute.graph.node.operator.LogicalOperator;
import com.threathunter.bordercollie.slot.util.ClassBasedRegistry;
import com.threathunter.model.PropertyCondition;
import com.threathunter.variable.condition.*;
import com.threathunter.variable.exception.NotSupportException;

import java.util.ArrayList;
import java.util.List;

/**
 * 
 */
public abstract class PropertyConditionHandlerGenerator<C extends PropertyCondition> {
    private static final ClassBasedRegistry<PropertyCondition, PropertyConditionHandlerGenerator> registry =
            new ClassBasedRegistry<>(PropertyCondition.class);

    static {
        registerCondition(StringPropertyCondition.class, StringConditionHandlerGenerator.class);
        registerCondition(LongPropertyCondition.class, LongConditionHandlerGenerator.class);
        registerCondition(CompoundCondition.class, CompoundConditionHandlerGenerator.class);
        registerCondition(DoublePropertyCondition.class, DoubleConditionHandlerGenerator.class);
        registerCondition(GeneralPropertyCondition.class, GeneralConditionHandlerGenerator.class);
        registerCondition(IPPropertyCondition.class, IPConditionHandlerGenerator.class);
    }

    public static void registerCondition(Class<? extends PropertyCondition> c, Class<? extends PropertyConditionHandlerGenerator> g) {
        registry.register(c, g);
    }

    public static PropertyConditionHandler generateConditionHandler(PropertyCondition c) {
        Class<? extends PropertyConditionHandlerGenerator> handlerGeneratorClass = registry.get(c.getClass());
        if (handlerGeneratorClass == null) {
            return null;
        }

        try {
            PropertyConditionHandlerGenerator handlerGenerator = handlerGeneratorClass.newInstance();
            return handlerGenerator.generateHandler(c);
        } catch (Exception e) {
            throw new RuntimeException("error in property condition handler generation.", e);
        }
    }

    public abstract PropertyConditionHandler generateHandler(C c);

    /**
     * This generator need to generate the proper string condition checker for stringpropertycondition
     * <p>
     * If this is stringcontains condition, generate the StringContainsConditionChecker.
     * <p>
     * The actual cheker will do meetingCondition compute.
     */
    public static class StringConditionHandlerGenerator extends PropertyConditionHandlerGenerator<StringPropertyCondition> {

        @Override
        public PropertyConditionHandler generateHandler(StringPropertyCondition c) {
            if (c instanceof StringPropertyCondition.StringContainsPropertyCondition) {
                return new StringPropertyConditionHandler.StringContainsPropertyConditionHandler(c);
            } else if (c instanceof StringPropertyCondition.StringEqualsPropertyCondition) {
                return new StringPropertyConditionHandler.StringEqualsPropertyConditionHandler(c);
            } else if (c instanceof StringPropertyCondition.StringNotEqualsPropertyCondition) {
                return new StringPropertyConditionHandler.StringNotEqualsPropertyConditionHandler(c);
            } else {
                throw new NotSupportException("String condition is not supported");
            }
        }
    }

    public static class LongConditionHandlerGenerator extends PropertyConditionHandlerGenerator<LongPropertyCondition> {

        @Override
        public PropertyConditionHandler generateHandler(LongPropertyCondition c) {
            if (c instanceof LongPropertyCondition.LongBiggerThanPropertyCondition) {
                return new LongPropertyConditionHandler.LongBiggerThanPropertyConditionHandler(c);
            }
            if (c instanceof LongPropertyCondition.LongBiggerEqualsPropertyCondition) {
                return new LongPropertyConditionHandler.LongBiggerEqualsPropertyConditionHandler(c);
            }
            if (c instanceof LongPropertyCondition.LongSmallerThanPropertyCondition) {
                return new LongPropertyConditionHandler.LongSmallerThanPropertyConditionHandler(c);
            }
            if (c instanceof LongPropertyCondition.LongEqualsPropertyCondition) {
                return new LongPropertyConditionHandler.LongEqualsPropertyConditionHandler(c);
            } else {
                throw new NotSupportException("long condition is not supported");
            }
        }
    }

    public static class CompoundConditionHandlerGenerator extends PropertyConditionHandlerGenerator<CompoundCondition> {

        @Override
        public PropertyConditionHandler generateHandler(CompoundCondition c) {
            CompoundConditionHandler handler = new CompoundConditionHandler(c);
            if (c.getType().equals("and")) {
                handler.setLogicalOperator(new LogicalOperator.AndLogicalOperator());
            }
            if (c.getType().equals("or")) {
                handler.setLogicalOperator(new LogicalOperator.OrLogicalOperator());
            }
            if (c.getType().equals("not")) {
                handler.setLogicalOperator(new LogicalOperator.NotLogicalOperator());
            }
            List<PropertyConditionHandler> subHandlers = new ArrayList<>();
            for (PropertyCondition condition : c.getConditions()) {
                subHandlers.add(generateConditionHandler(condition));
            }
            handler.setSubConditionHandlers(subHandlers);
            return handler;
        }
    }

    public static class DoubleConditionHandlerGenerator extends PropertyConditionHandlerGenerator<DoublePropertyCondition> {

        @Override
        public PropertyConditionHandler generateHandler(DoublePropertyCondition c) {
            if (c instanceof DoublePropertyCondition.DoubleSmallerThanPropertyCondition) {
                return new DoublePropertyConditionHandler.DoubleSmallerThanPropertyConditionHandler(c);
            }
            throw new NotSupportException("double property is not supported");
        }
    }

    public static class GeneralConditionHandlerGenerator extends PropertyConditionHandlerGenerator<GeneralPropertyCondition> {

        @Override
        public PropertyConditionHandler generateHandler(GeneralPropertyCondition c) {
            return null;
        }
    }

    public static class IPConditionHandlerGenerator extends PropertyConditionHandlerGenerator<IPPropertyCondition> {

        @Override
        public PropertyConditionHandler generateHandler(IPPropertyCondition c) {
            return null;
        }
    }
}
