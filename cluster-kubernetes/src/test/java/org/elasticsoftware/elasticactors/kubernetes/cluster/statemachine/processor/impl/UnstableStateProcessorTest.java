package org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.processor.impl;

import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import org.elasticsoftware.elasticactors.kubernetes.cluster.TaskScheduler;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.KubernetesStateMachineData;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.KubernetesStateMachineListener;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.KubernetesClusterState.STABLE;
import static org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.KubernetesClusterState.UNSTABLE;
import static org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.StateMachineTestUtil.initialize;
import static org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.StateMachineTestUtil.resourceWith;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class UnstableStateProcessorTest {

    private UnstableStateProcessor processor;
    private KubernetesStateMachineData data;
    private TaskScheduler taskScheduler;
    private StatefulSet originalStableState;
    private KubernetesStateMachineListener listener;

    @BeforeMethod
    public void setUp() {
        taskScheduler = mock(TaskScheduler.class);
        listener = mock(KubernetesStateMachineListener.class);
        data = new KubernetesStateMachineData();
        processor = new UnstableStateProcessor(data, taskScheduler);
        originalStableState = resourceWith(2, 2, 2);
        initialize(data, originalStableState, UNSTABLE, listener);
    }

    @DataProvider(name = "unstableStates")
    public Object[][] getUnstableStates() {
        return new Object[][] {
                {resourceWith(1, 1, 1)},
                {resourceWith(1, 1, 2)},
                {resourceWith(1, 1, 3)},
                {resourceWith(1, 2, 1)},
                {resourceWith(1, 2, 2)},
                {resourceWith(1, 2, 3)},
                {resourceWith(1, 3, 1)},
                {resourceWith(1, 3, 2)},
                {resourceWith(1, 3, 3)},
                {resourceWith(2, 1, 1)},
                {resourceWith(2, 1, 2)},
                {resourceWith(2, 1, 3)},
                {resourceWith(2, 2, 1)},
                //{resourceWith(2, 2, 2)}, This is the stable state
                {resourceWith(2, 2, 3)},
                {resourceWith(2, 3, 1)},
                {resourceWith(2, 3, 2)},
                {resourceWith(2, 3, 3)},
                {resourceWith(3, 1, 1)},
                {resourceWith(3, 1, 2)},
                {resourceWith(3, 1, 3)},
                {resourceWith(3, 2, 1)},
                {resourceWith(3, 2, 2)},
                {resourceWith(3, 2, 3)},
                {resourceWith(3, 3, 1)},
                {resourceWith(3, 3, 2)},
                {resourceWith(3, 3, 3)}
        };
    }

    @Test(dataProvider = "unstableStates")
    public void testProcess_shouldRemainInUnstableState(StatefulSet resource) {
        assertFalse(processor.process(resource));

        assertEquals(data.getCurrentState().get(), UNSTABLE);
        assertEquals(data.getCurrentTopology().get(), 2);
        assertEquals(data.getLatestStableState().get(), originalStableState);
        then(listener).should(never()).onTopologyChange(anyInt());
        then(taskScheduler).should(never()).cancelScheduledTask();
        then(taskScheduler).should(never()).scheduleTask(any(), any(), any());
    }

    @Test
    public void testProcess_shouldSwitchToStable() {
        StatefulSet newStableState = resourceWith(2, 2, 2);
        assertFalse(processor.process(newStableState));

        assertEquals(data.getCurrentState().get(), STABLE);
        assertEquals(data.getCurrentTopology().get(), 2);
        assertEquals(data.getLatestStableState().get(), newStableState);
        then(listener).should(never()).onTopologyChange(anyInt());
        then(taskScheduler).should().cancelScheduledTask();
        then(taskScheduler).should(never()).scheduleTask(any(), any(), any());
    }

}