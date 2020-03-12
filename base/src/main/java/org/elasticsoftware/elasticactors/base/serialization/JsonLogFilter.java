package org.elasticsoftware.elasticactors.base.serialization;

import com.fasterxml.jackson.databind.ser.PropertyWriter;
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;

import java.util.Arrays;

final class JsonLogFilter extends SimpleBeanPropertyFilter {

    final static String FILTER_NAME = "elasticactors.jsonLog";

    @Override
    protected boolean include(PropertyWriter writer) {
        if (super.include(writer)) {
            JsonLog jsonLog = writer.findAnnotation(JsonLog.class);
            if (jsonLog != null) {
                return jsonLog.value();
            }
            JsonLogProperties jsonLogProperties = writer.findAnnotation(JsonLogProperties.class);
            if (jsonLogProperties != null) {
                return Arrays.asList(jsonLogProperties.value()).contains(writer.getName());
            }
        }
        return false;
    }
}
