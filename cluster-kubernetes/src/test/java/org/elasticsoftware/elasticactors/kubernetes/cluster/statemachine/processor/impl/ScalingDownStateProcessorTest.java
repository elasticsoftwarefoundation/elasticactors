package org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.processor.impl;

import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import org.elasticsoftware.elasticactors.kubernetes.cluster.TaskScheduler;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.KubernetesStateMachineData;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.KubernetesStateMachineListener;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

import static org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.KubernetesClusterState.SCALING_DOWN;
import static org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.KubernetesClusterState.SCALING_UP;
import static org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.KubernetesClusterState.STABLE;
import static org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.StateMachineTestUtil.initialize;
import static org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.StateMachineTestUtil.resourceWith;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.never;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class ScalingDownStateProcessorTest {

    private ScalingDownStateProcessor processor;
    private KubernetesStateMachineData data;
    private TaskScheduler taskScheduler;
    private KubernetesStateMachineListener listener;
    private StatefulSet originalStableState;

    @BeforeMethod
    public void setUp() {
        taskScheduler = Mockito.mock(TaskScheduler.class);
        listener = Mockito.mock(KubernetesStateMachineListener.class);
        data = new KubernetesStateMachineData();
        processor = new ScalingDownStateProcessor(data, taskScheduler);
        originalStableState = resourceWith(5, 5, 5);
        initialize(data, originalStableState, SCALING_DOWN, listener);
    }

    @Test
    public void testProcess_shouldSwitchToStable() {
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
    public void testProcess_shouldSwitchToScaleUp() {
        assertTrue(processor.process(resourceWith(6, 3, 3)));

        assertEquals(data.getCurrentState().get(), SCALING_UP);
        assertEquals(data.getCurrentTopology().get(), 5);
        assertEquals(data.getLatestStableState().get(), originalStableState);
        then(listener).should(never()).onTopologyChange(anyInt());
        then(taskScheduler).should(never()).cancelScheduledTask();
        then(taskScheduler).should(never()).scheduleTask(any(), any(), any());
    }

    @DataProvider(name = "unstableStates")
    public Object[][] getUnstableStates() {
        return new Object[][]{
                {resourceWith(3, 5, 5)},
                {resourceWith(3, 5, 4)},
                {resourceWith(3, 4, 4)},
                {resourceWith(3, 4, 3)}
        };
    }

    @Test(dataProvider = "unstableStates")
    public void testProcess_shouldNotSwitchToStable(StatefulSet resource) {
        assertFalse(processor.process(resource));

        assertEquals(data.getCurrentState().get(), SCALING_DOWN);
        assertEquals(data.getCurrentTopology().get(), 5);
        assertEquals(data.getLatestStableState().get(), originalStableState);
        then(listener).should(never()).onTopologyChange(anyInt());
        then(taskScheduler).should(never()).cancelScheduledTask();
        then(taskScheduler).should(never()).scheduleTask(any(), any(), any());
    }

}