package com.threathunter.bordercollie.slot.compute.graph.extension.incident;

import com.threathunter.common.Identifier;
import com.threathunter.common.NamedType;
import com.threathunter.model.*;

import java.util.List;
import java.util.Map;

/**
 * 
 */
public class IncidentVariableMeta extends VariableMeta {
    public static final String TYPE = "incident";

    private String app;
    private String name;
    private boolean topValue;
    private boolean keyTopValue;
    private String dimension;
    private List<Property> groupbyKeys;

    public IncidentVariableMeta(final String dimension, final String app, final String name, boolean isTopValue, boolean isKeyTopValue, List<Property> groupbyKeys) {
        this.dimension = dimension;
        this.app = app;
        this.name = name;
        this.topValue = isTopValue;
        this.keyTopValue = isKeyTopValue;
        this.groupbyKeys = groupbyKeys;
    }

    @Override
    public Identifier getId() {
        return null;
    }

    @Override
    public String getApp() {
        return app;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getType() {
        if (topValue || keyTopValue) {
            return "top";
        }
        return "aggregate";
    }

    @Override
    public String getStatus() {
        return null;
    }

    @Override
    public boolean isDerived() {
        return false;
    }

    @Override
    public List<Identifier> getSrcVariableMetasID() {
        return null;
    }

    @Override
    public Identifier getSrcEventMetaID() {
        return null;
    }

    @Override
    public boolean isInternal() {
        return false;
    }

    @Override
    public int getPriority() {
        return 0;
    }

    @Override
    public List<Property> getProperties() {
        return null;
    }

    @Override
    public boolean hasProperty(Property property) {
        return false;
    }

    @Override
    public Map<String, NamedType> getDataSchema() {
        return null;
    }

    @Override
    public long getExpireDate() {
        return 0;
    }

    @Override
    public long getTtl() {
        return 0;
    }

    @Override
    public Property findPropertyByName(String s) {
        return null;
    }

    @Override
    public List<PropertyMapping> getPropertyMappings() {
        return null;
    }

    @Override
    public PropertyCondition getPropertyCondition() {
        return null;
    }

    @Override
    public PropertyReduction getPropertyReduction() {
        return null;
    }


    @Override
    public String getRemark() {
        return null;
    }

    @Override
    public String getVisibleName() {
        return null;
    }


    @Override
    public String getDimension() {
        return dimension;
    }

    @Override
    public String getModule() {
        return null;
    }

    @Override
    public String getValueType() {
        return null;
    }

    @Override
    public Object to_json_object() {
        return null;
    }

    @Override
    public List<Property> getGroupKeys() {
        return this.groupbyKeys;
    }
}
