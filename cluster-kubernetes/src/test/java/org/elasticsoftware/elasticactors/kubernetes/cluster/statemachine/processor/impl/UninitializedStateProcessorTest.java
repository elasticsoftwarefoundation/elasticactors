package org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.processor.impl;

import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.KubernetesStateMachineData;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.KubernetesStateMachineListener;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.KubernetesClusterState.STABLE;
import static org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.StateMachineTestUtil.resourceWith;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class UninitializedStateProcessorTest {

    private UninitializedStateProcessor processor;
    private KubernetesStateMachineData data;
    private KubernetesStateMachineListener listener;

    @BeforeMethod
    public void setUp() {
        listener = mock(KubernetesStateMachineListener.class);
        data = new KubernetesStateMachineData();
        processor = new UninitializedStateProcessor(data);
        data.getStateMachineListeners().add(listener);
    }

    @Test
    public void testProcess() {
        StatefulSet newStableState = resourceWith(2, 2, 2);
        assertFalse(processor.process(newStableState));

        assertEquals(data.getCurrentState().get(), STABLE);
        assertEquals(data.getCurrentTopology().get(), 2);
        assertEquals(data.getLatestStableState().get(), newStableState);
        then(listener).should().onTopologyChange(2);
    }

}