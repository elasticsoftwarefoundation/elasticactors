package org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.processor.impl;

import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.KubernetesStateMachineData;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.KubernetesStateMachineListener;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.KubernetesClusterState.SCALING_DOWN;
import static org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.KubernetesClusterState.SCALING_UP;
import static org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.KubernetesClusterState.STABLE;
import static org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.StateMachineTestUtil.initialize;
import static org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.StateMachineTestUtil.resourceWith;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class StableStateProcessorTest {

    private StableStateProcessor processor;
    private KubernetesStateMachineData data;
    private KubernetesStateMachineListener listener;
    private StatefulSet originalStableState;

    @BeforeMethod
    public void setUp() {
        listener = mock(KubernetesStateMachineListener.class);
        data = new KubernetesStateMachineData();
        processor = new StableStateProcessor(data);
        originalStableState = resourceWith(2, 2, 2);
        initialize(data, originalStableState, STABLE, listener);
    }

    @Test
    public void testProcess_shouldNotDoAnything() {
        assertFalse(processor.process(resourceWith(2, 2, 2)));

        assertEquals(data.getCurrentState().get(), STABLE);
        assertEquals(data.getCurrentTopology().get(), 2);
        assertEquals(data.getLatestStableState().get(), originalStableState);
        then(listener).should(never()).onTopologyChange(anyInt());
    }

    @Test
    public void testProcess_shouldSwitchToScalingUp() {
        assertTrue(processor.process(resourceWith(3, 2, 2)));

        assertEquals(data.getCurrentState().get(), SCALING_UP);
        assertEquals(data.getCurrentTopology().get(), 2);
        assertEquals(data.getLatestStableState().get(), originalStableState);
        then(listener).should(never()).onTopologyChange(anyInt());
    }

    @Test
    public void testProcess_shouldSwitchToScalingDown() {
        assertTrue(processor.process(resourceWith(1, 2, 2)));

        assertEquals(data.getCurrentState().get(), SCALING_DOWN);
        assertEquals(data.getCurrentTopology().get(), 2);
        assertEquals(data.getLatestStableState().get(), originalStableState);
        then(listener).should(never()).onTopologyChange(anyInt());
    }

}