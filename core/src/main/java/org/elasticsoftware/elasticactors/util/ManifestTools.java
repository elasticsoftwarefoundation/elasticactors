package org.elasticsoftware.elasticactors.util;

import org.elasticsoftware.elasticactors.Actor;
import org.elasticsoftware.elasticactors.ActorState;
import org.elasticsoftware.elasticactors.ElasticActor;

import java.io.IOException;
import java.net.URL;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

import static java.lang.String.format;

/**
 * @author Joost van de Wijgerd
 */
public final class ManifestTools {

    public static final String UNKNOWN_VERSION = "UNKNOWN";

    private ManifestTools() {}

    public static String extractActorStateVersion(Class<? extends ElasticActor> actorClass) throws IOException {
        Actor actorAnnotation = actorClass.getAnnotation(Actor.class);
        if(actorAnnotation != null) {
            return extractVersion(actorAnnotation.stateClass());
        } else {
            return UNKNOWN_VERSION;
        }
    }

    public static String extractVersion(Class<? extends ActorState> stateClass) throws IOException {
        String className = format("%s.class",stateClass.getSimpleName());
        String classPath = stateClass.getResource(className).toString();
        if (!classPath.startsWith("jar")) {
            // Class not from JAR, cannot determine version
            return UNKNOWN_VERSION;
        }
        String manifestPath = classPath.substring(0, classPath.lastIndexOf("!") + 1) +
                "/META-INF/MANIFEST.MF";
        Manifest manifest = new Manifest(new URL(manifestPath).openStream());
        Attributes attr = manifest.getMainAttributes();
        String value = attr.getValue("Implementation-Version");
        return (value != null ? value : "UNKNOWN");
    }
}
