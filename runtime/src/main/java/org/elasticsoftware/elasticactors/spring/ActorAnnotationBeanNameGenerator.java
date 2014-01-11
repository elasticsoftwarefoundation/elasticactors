package org.elasticsoftware.elasticactors.spring;

import org.springframework.context.annotation.AnnotationBeanNameGenerator;

import java.util.Map;
import java.util.Set;

/**
 * @author Joost van de Wijgerd
 */
public class ActorAnnotationBeanNameGenerator extends AnnotationBeanNameGenerator {
    @Override
    protected boolean isStereotypeWithNameValue(String annotationType, Set<String> metaAnnotationTypes, Map<String, Object> attributes) {
        if(annotationType.equals("org.elasticsoftware.elasticactors.ServiceActor")) {
            return true;
        } else {
            return super.isStereotypeWithNameValue(annotationType,metaAnnotationTypes,attributes);
        }
    }
}
