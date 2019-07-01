package org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.processor.impl;

import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import org.elasticsoftware.elasticactors.kubernetes.cluster.TaskScheduler;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.KubernetesStateMachineListener;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.data.KubernetesStateMachineData;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.StateMachineTestUtil.initialize;
import static org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.StateMachineTestUtil.resourceWith;
import static org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.data.KubernetesClusterState.SCALING_DOWN;
import static org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.data.KubernetesClusterState.SCALING_UP;
import static org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.data.KubernetesClusterState.SCALING_UP_STARTED;
import static org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.data.KubernetesClusterState.STABLE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class ScalingUpStateProcessorTest {

    private ScalingUpStateProcessor processor;
    private KubernetesStateMachineData data;
    private TaskScheduler taskScheduler;
    private KubernetesStateMachineListener listener;
    private StatefulSet originalStableState;

    @BeforeMethod
    public void setUp() {
        taskScheduler = mock(TaskScheduler.class);
        listener = mock(KubernetesStateMachineListener.class);
        data = new KubernetesStateMachineData();
        processor = new ScalingUpStateProcessor(data, taskScheduler);
        originalStableState = resourceWith(2, 2, 2);
        initialize(data, originalStableState, SCALING_UP, listener);
    }

    @Test
    public void testProcess_shouldSwitchToStable_delayedProcessing_scaledDown() {
        StatefulSet newStableState = resourceWith(1, 1, 1);
        assertFalse(processor.process(newStableState));

        assertEquals(data.getCurrentState().get(), STABLE);
        assertEquals(data.getCurrentTopology().get(), 1);
        assertEquals(data.getLatestStableState().get(), newStableState);
        then(listener).should().onTopologyChange(1);
        then(taskScheduler).should().cancelScheduledTask();
        then(taskScheduler).should(never()).scheduleTask(any(), any(), any());
    }

    @Test
    public void testProcess_shouldSwitchToStable_delayedProcessing_scaledUp() {
        StatefulSet newStableState = resourceWith(3, 3, 3);
        assertFalse(processor.process(newStableState));

        assertEquals(data.getCurrentState().get(), STABLE);
        assertEquals(data.getCurrentTopology().get(), 3);
        assertEquals(data.getLatestStableState().get(), newStableState);
        then(listener).should().onTopologyChange(3);
        then(taskScheduler).should().cancelScheduledTask();
        then(taskScheduler).should(never()).scheduleTask(any(), any(), any());
    }

    @Test
    public void testProcess_shouldSwitchToStable_scaleUpCancelled() {
        StatefulSet newStableState = resourceWith(2, 2, 2);
        assertFalse(processor.process(newStableState));

        assertEquals(data.getCurrentState().get(), STABLE);
        assertEquals(data.getCurrentTopology().get(), 2);
        assertEquals(data.getLatestStableState().get(), newStableState);
        then(listener).should(never()).onTopologyChange(anyInt());
        then(taskScheduler).should().cancelScheduledTask();
        then(taskScheduler).should(never()).scheduleTask(any(), any(), any());
    }

    @Test
    public void testProcess_shouldSwitchToScalingDown() {
        assertTrue(processor.process(resourceWith(1, 2, 2)));

        assertEquals(data.getCurrentState().get(), SCALING_DOWN);
        assertEquals(data.getCurrentTopology().get(), 2);
        assertEquals(data.getLatestStableState().get(), originalStableState);
        then(listener).should(never()).onTopologyChange(anyInt());
        then(taskScheduler).should(never()).cancelScheduledTask();
        then(taskScheduler).should(never()).scheduleTask(any(), any(), any());
    }

    @Test
    public void testProcess_shouldKeepStateUntilScalingUpStarts() {
        assertFalse(processor.process(resourceWith(5, 2, 2)));

        assertEquals(data.getCurrentState().get(), SCALING_UP);
        assertEquals(data.getCurrentTopology().get(), 2);
        assertEquals(data.getLatestStableState().get(), originalStableState);
        then(listener).should(never()).onTopologyChange(anyInt());
        then(taskScheduler).should(never()).cancelScheduledTask();
        then(taskScheduler).should(never()).scheduleTask(any(), any(), any());
    }

    @Test
    public void testProcess_shouldSwitchToScalingUpStarted() {
        assertFalse(processor.process(resourceWith(5, 3, 2)));

        assertEquals(data.getCurrentState().get(), SCALING_UP_STARTED);
        assertEquals(data.getCurrentTopology().get(), 5);
        assertEquals(data.getLatestStableState().get(), originalStableState);
        then(listener).should().onTopologyChange(5);
        then(taskScheduler).should(never()).cancelScheduledTask();
        then(taskScheduler).should().scheduleTask(any(), eq(3), any());
    }

}