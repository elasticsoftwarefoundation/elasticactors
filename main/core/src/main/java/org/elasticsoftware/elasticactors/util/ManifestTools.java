/*
 * Copyright 2013 - 2025 The Original Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package org.elasticsoftware.elasticactors.util;

import org.elasticsoftware.elasticactors.Actor;
import org.elasticsoftware.elasticactors.ActorState;
import org.elasticsoftware.elasticactors.ElasticActor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

import static java.lang.String.format;

/**
 * @author Joost van de Wijgerd
 */
public final class ManifestTools {
    private static final Logger logger = LoggerFactory.getLogger(ManifestTools.class);
    public static final String UNKNOWN_VERSION = "UNKNOWN";

    private ManifestTools() {}

    public static String extractActorStateVersion(Class<? extends ElasticActor> actorClass) {
        Actor actorAnnotation = actorClass.getAnnotation(Actor.class);
        if(actorAnnotation != null) {
            return extractVersion(actorAnnotation.stateClass());
        } else {
            return UNKNOWN_VERSION;
        }
    }

    public static String extractVersion(Class<? extends ActorState> stateClass) {
        String className = (stateClass.getEnclosingClass() == null) ? stateClass.getName() : stateClass.getEnclosingClass().getName();
        className = format("/%s.class",className.replace('.','/'));
        URL resource = stateClass.getResource(className);
        if(resource != null) {
            String classPath = resource.toString();
            if (!classPath.startsWith("jar")) {
                // Class not from JAR, cannot determine version
                return UNKNOWN_VERSION;
            }

            String manifestPath = classPath.substring(0, classPath.lastIndexOf("!") + 1) +
                    "/META-INF/MANIFEST.MF";
            try {
                Manifest manifest = new Manifest(new URL(manifestPath).openStream());
                Attributes attr = manifest.getMainAttributes();
                String value = attr.getValue("Implementation-Version");
                logger.debug("stateClass [{}] has version [{}]",className,(value != null) ? value : UNKNOWN_VERSION);
                return (value != null) ? value : UNKNOWN_VERSION;
            } catch(Exception e) {
                logger.warn("Cannot find Manifest for stateClass [{}] setting version to {}",stateClass.getName(),UNKNOWN_VERSION);
                return UNKNOWN_VERSION;
            }
        } else {
            logger.warn("Unable to determine version for stateClass [{}], cannot load class resource?",stateClass.getName());
            return UNKNOWN_VERSION;
        }
    }
}
