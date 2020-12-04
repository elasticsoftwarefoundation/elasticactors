package org.elasticsoftware.elasticactors.base.serialization;

import com.fasterxml.jackson.databind.ser.BeanPropertyWriter;
import com.fasterxml.jackson.databind.ser.PropertyWriter;
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;

import java.util.Objects;

final class JsonLogIgnoreFilter extends SimpleBeanPropertyFilter {

    final static String FILTER_NAME = "elasticactors.jsonLogIgnore";

    @Override
    protected boolean include(BeanPropertyWriter writer) {
        return this.include((PropertyWriter) writer);
    }

    @Override
    protected boolean include(PropertyWriter writer) {
        if (writer.findAnnotation(JsonLogIgnore.class) != null) {
            return false;
        }
        JsonLogIgnoreProperties jsonLogIgnoreProperties =
                writer.findAnnotation(JsonLogIgnoreProperties.class);
        if (jsonLogIgnoreProperties != null) {
            for (String ignoredName : jsonLogIgnoreProperties.value()) {
                if (Objects.equals(ignoredName, writer.getName())) {
                    return false;
                }
            }
        }
        return super.include(writer);
    }
}
