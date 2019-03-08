package com.threathunter.bordercollie.slot.compute.graph.node.propertyhandler.mapping;

import com.threathunter.bordercollie.slot.compute.VariableDataContext;
import com.threathunter.bordercollie.slot.util.ClassBasedRegistry;
import com.threathunter.bordercollie.slot.util.LocationHelper;
import com.threathunter.common.Identifier;
import com.threathunter.model.Property;
import com.threathunter.model.PropertyMapping;
import com.threathunter.variable.mapping.*;

/**
 * Created by daisy on 16/5/16.
 */
public abstract class PropertyMappingHandlerGenerator<M extends PropertyMapping> {
    private static final ClassBasedRegistry<PropertyMapping, PropertyMappingHandlerGenerator> registry =
            new ClassBasedRegistry<>(PropertyMapping.class);

    static {
        registerMapping(DirectPropertyMapping.class, DirectMappingHandlerGenerator.class);
        registerMapping(StringValuePropertyMapping.class, StringValueMappingHandlerGenerator.class);
        registerMapping(LocationPropertyMapping.class, LocationMappingHandlerGenerator.class);
        registerMapping(DoubleValuePropertyMapping.class, DoubleValueMappingHandlerGenerator.class);
        registerMapping(LongValuePropertyMapping.class, LongValueMappingHandlerGenerator.class);
    }

    public static void registerMapping(Class<? extends PropertyMapping> c, Class<? extends PropertyMappingHandlerGenerator> g) {
        registry.register(c, g);
    }

    public static PropertyMappingHandler generateMappingHandler(final PropertyMapping m) {
        Class<? extends PropertyMappingHandlerGenerator> handlerClass = registry.get(m.getClass());
        if (handlerClass == null) {
            return null;
        }

        try {
            // TODO no need to get a new instance every time, consider singleton
            PropertyMappingHandlerGenerator handlerGenerator = handlerClass.newInstance();
            return handlerGenerator.generateHandler(m);
        } catch (Exception e) {
            throw new RuntimeException("error in property mapping handler generation.", e);
        }
    }

    public abstract PropertyMappingHandler generateHandler(M m);

    public static class DirectMappingHandlerGenerator extends PropertyMappingHandlerGenerator<DirectPropertyMapping> {

        @Override
        public PropertyMappingHandler generateHandler(final DirectPropertyMapping directPropertyMapping) {
            return new PropertyMappingHandler() {
                Property srcProperty = directPropertyMapping.getSrcProperties().get(0);
                Property destProperty = directPropertyMapping.getDestProperty();

                @Override
                public String getPropertyName() {
                    return destProperty.getName();
                }

                @Override
                public Object getMappedPropertyValue(final VariableDataContext context) {
                    return context.getFromContext(srcProperty.getIdentifier(), srcProperty.getName());
                }
            };
        }
    }

    public static class StringValueMappingHandlerGenerator extends PropertyMappingHandlerGenerator<StringValuePropertyMapping> {

        @Override
        public PropertyMappingHandler generateHandler(final StringValuePropertyMapping stringValuePropertyMapping) {
            return new PropertyMappingHandler() {
                String param = stringValuePropertyMapping.getParam();
                Property destProperty = stringValuePropertyMapping.getDestProperty();

                @Override
                public String getPropertyName() {
                    return destProperty.getName();
                }

                @Override
                public Object getMappedPropertyValue(final VariableDataContext context) {
                    return param;
                }
            };
        }
    }

    public static class LocationMappingHandlerGenerator extends PropertyMappingHandlerGenerator<LocationPropertyMapping> {

        @Override
        public PropertyMappingHandler generateHandler(final LocationPropertyMapping locationPropertyMapping) {
            return new PropertyMappingHandler() {
                String field = locationPropertyMapping.getSrcProperty().getName();
                Identifier srcId = locationPropertyMapping.getSrcProperty().getIdentifier();
                String geoType = locationPropertyMapping.getGeoType();
                Property destProperty = locationPropertyMapping.getDestProperty();

                @Override
                public String getPropertyName() {
                    return destProperty.getName();
                }

                @Override
                public Object getMappedPropertyValue(final VariableDataContext context) {
                    String location = LocationHelper.getLocation(context.getFromContext(srcId, field).toString(), geoType);
                    return location == null ? "unknown" : location;
                }
            };
        }
    }

    public static class DoubleValueMappingHandlerGenerator extends PropertyMappingHandlerGenerator<DoubleValuePropertyMapping> {
        @Override
        public PropertyMappingHandler generateHandler(final DoubleValuePropertyMapping doubleValuePropertyMapping) {
            return new PropertyMappingHandler() {
                Double param = doubleValuePropertyMapping.getParam();
                Property destProperty = doubleValuePropertyMapping.getDestProperty();

                @Override
                public String getPropertyName() {
                    return destProperty.getName();
                }

                @Override
                public Object getMappedPropertyValue(final VariableDataContext context) {
                    return param;
                }
            };
        }
    }

    public static class LongValueMappingHandlerGenerator extends PropertyMappingHandlerGenerator<LongValuePropertyMapping> {

        @Override
        public PropertyMappingHandler generateHandler(final LongValuePropertyMapping longValuePropertyMapping) {
            return new PropertyMappingHandler() {
                Long param = longValuePropertyMapping.getParam();
                Property destProperty = longValuePropertyMapping.getDestProperty();

                @Override
                public String getPropertyName() {
                    return destProperty.getName();
                }

                @Override
                public Object getMappedPropertyValue(final VariableDataContext context) {
                    return param;
                }
            };
        }
    }
}
