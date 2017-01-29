/*
 * Copyright 2013 - 2017 The Original Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsoftware.elasticactors.runtime;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsoftware.elasticactors.serialization.Message;
import org.elasticsoftware.elasticactors.serialization.SerializationFramework;
import org.reflections.Reflections;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.springframework.context.ApplicationContext;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Named;
import java.util.Set;

/**
 * Find all classes annotated with {@link org.elasticsoftware.elasticactors.serialization.Message} and
 * register them with the {@link org.elasticsoftware.elasticactors.serialization.SerializationFramework#register(Class)}
 *
 * @author Joost van de Wijgerd
 */
@Named
public final class MessagesScanner {
    private static final Logger logger = LogManager.getLogger(MessagesScanner.class);
    @Inject
    private ApplicationContext applicationContext;

    @PostConstruct
    public void init() {
        String[] basePackages = ScannerHelper.findBasePackagesOnClasspath(applicationContext.getClassLoader());
        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();

        for (String basePackage : basePackages) {
            configurationBuilder.addUrls(ClasspathHelper.forPackage(basePackage));
        }

        Reflections reflections = new Reflections(configurationBuilder);

        Set<Class<?>> messageClasses = reflections.getTypesAnnotatedWith(Message.class);

        for (Class<?> messageClass : messageClasses) {
            Message messageAnnotation = messageClass.getAnnotation(Message.class);
            // get the serialization framework
            SerializationFramework framework = applicationContext.getBean(messageAnnotation.serializationFramework());
            if(framework != null) {
                framework.register(messageClass);
            } else {
                logger.error(String.format("Could not find framework %s for message class %s",
                                           messageAnnotation.serializationFramework().getSimpleName(),
                                           messageClass.getName()));
            }
        }
    }
}
