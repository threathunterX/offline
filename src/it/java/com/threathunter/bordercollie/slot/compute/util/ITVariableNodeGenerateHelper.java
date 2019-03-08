package com.threathunter.bordercollie.slot.compute.util;

import com.threathunter.bordercollie.slot.compute.cache.StorageType;
import com.threathunter.bordercollie.slot.compute.graph.node.VariableNode;
import com.threathunter.bordercollie.slot.compute.graph.nodegenerator.VariableNodeGenerator;
import com.threathunter.config.CommonDynamicConfig;
import com.threathunter.model.PropertyCondition;
import com.threathunter.model.PropertyMapping;
import com.threathunter.model.PropertyReduction;
import com.threathunter.model.VariableMeta;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class ITVariableNodeGenerateHelper {

    static {
        CommonDynamicConfig.getInstance().addConfigFile("nebula.conf");
        CommonDynamicConfig.getInstance().addConfigFile("online.conf");
        PropertyCondition.init();
        PropertyMapping.init();
        PropertyReduction.init();
        VariableMeta.init();
    }

    public List<VariableMeta> getVariableMeta(String jsonFileName) throws IOException {
        String variableFilePath = String.format("%s/src/test/resources/%s",
                System.getProperty("user.dir"), jsonFileName);
        InputStream is = new FileInputStream(new File(variableFilePath));
        ObjectMapper mapper = new ObjectMapper();
        List<Object> variableObjects = mapper.reader(List.class).readValue(is);
        List<VariableMeta> variableMetas = new ArrayList<>();
        if (variableObjects != null) {
            for (Object o : variableObjects) {
                variableMetas.add(VariableMeta.from_json_object(o));
            }
        }
        return variableMetas;
    }

    public List<VariableNode> getVariableNode(String jsonFile, StorageType storageType) throws IOException {
        List<VariableMeta> variableMetas = getVariableMeta(jsonFile);
        List<VariableNode> nodes = new ArrayList<>();
        for (VariableMeta variableMeta : variableMetas) {
            nodes.addAll(VariableNodeGenerator.generateNode(variableMeta, storageType));
        }
        return nodes;
    }
}