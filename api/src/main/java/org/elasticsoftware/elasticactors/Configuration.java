package org.elasticsoftware.elasticactors;

/**
 * @author Joost van de Wijgerd
 */
public interface Configuration {
    int getAsInt(String component, String property, int defaultValue);
}
