package org.elasticsoftware.elasticactors.util;

import org.apache.log4j.Logger;
import org.elasticsoftware.elasticactors.Actor;
import org.elasticsoftware.elasticactors.ActorState;
import org.elasticsoftware.elasticactors.ElasticActor;

import java.net.URL;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

import static java.lang.String.format;

/**
 * @author Joost van de Wijgerd
 */
public final class ManifestTools {
    private static final Logger logger = Logger.getLogger(ManifestTools.class);
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
                logger.debug(format("stateClass [%s] has version [%s]",className,(value != null) ? value : UNKNOWN_VERSION));
                return (value != null) ? value : UNKNOWN_VERSION;
            } catch(Exception e) {
                logger.warn(format("Cannot find Manifest for stateClass [%s] setting version to %s",stateClass.getName(),UNKNOWN_VERSION));
                return UNKNOWN_VERSION;
            }
        } else {
            logger.warn(format("Unable to determine version for stateClass [%s], cannot load class resource?",stateClass.getName()));
            return UNKNOWN_VERSION;
        }
    }
}
