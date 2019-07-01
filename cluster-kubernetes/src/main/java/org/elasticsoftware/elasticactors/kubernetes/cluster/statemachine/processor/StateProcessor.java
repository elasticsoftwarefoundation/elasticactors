package org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.processor;

import io.fabric8.kubernetes.api.model.apps.StatefulSet;

public interface StateProcessor {

    /**
     * Processes the current resource
     *
     * @param resource The current state of the Stateful Set
     * @return true if the resource should be processed by another processor after this one is done
     */
    boolean process(StatefulSet resource);
}
