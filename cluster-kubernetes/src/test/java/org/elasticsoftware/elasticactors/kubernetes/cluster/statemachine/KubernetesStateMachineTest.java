package org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine;

import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import org.elasticsoftware.elasticactors.kubernetes.cluster.TaskScheduler;
import org.springframework.util.ReflectionUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.StateMachineTestUtil.resourceWith;
import static org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.StateMachineTestUtil.scale;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.util.ReflectionUtils.findField;
import static org.springframework.util.ReflectionUtils.getField;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public class KubernetesStateMachineTest {

    private KubernetesStateMachine stateMachine;
    private KubernetesStateMachineData stateMachineData;
    private AtomicReference scheduledTimeoutTask;
    private StatefulSet originalStable;

    @SuppressWarnings("unchecked")
    @BeforeMethod
    public void setUp() {
        ScheduledExecutorService executorService = mock(ScheduledExecutorService.class);
        ScheduledFuture scheduledFuture = mock(ScheduledFuture.class);
        when(executorService.schedule(any(Runnable.class), anyLong(), any())).thenReturn(scheduledFuture);
        TaskScheduler taskScheduler = new TaskScheduler(executorService, 60);
        stateMachine = new KubernetesStateMachine(taskScheduler);
        Field scheduledTaskField = findField(TaskScheduler.class, "scheduledTask");
        ReflectionUtils.makeAccessible(scheduledTaskField);
        scheduledTimeoutTask = (AtomicReference) getField(scheduledTaskField, taskScheduler);
        Field kubernetesStateMachineDataField = findField(KubernetesStateMachine.class, "kubernetesStateMachineData");
        ReflectionUtils.makeAccessible(kubernetesStateMachineDataField);
        stateMachineData = (KubernetesStateMachineData) getField(kubernetesStateMachineDataField, stateMachine);
        originalStable = resourceWith(3, 3, 3);
    }

    @Test
    public void shouldInitialize() {
        stateMachine.processStateUpdate(originalStable);

        assertNull(scheduledTimeoutTask.get());
        assertEquals(stateMachineData.getCurrentTopology().get(), 3);
        assertEquals(stateMachineData.getLatestStableState().get(), originalStable);
        assertEquals(stateMachineData.getCurrentState().get(), KubernetesClusterState.STABLE);
    }

    @Test
    public void shouldScaleUp() {
        stateMachine.processStateUpdate(originalStable);
        stateMachine.processStateUpdate(resourceWith(4, 3, 3));

        assertNull(scheduledTimeoutTask.get());
        assertEquals(stateMachineData.getCurrentTopology().get(), 3);
        assertEquals(stateMachineData.getLatestStableState().get(), originalStable);
        assertEquals(stateMachineData.getCurrentState().get(), KubernetesClusterState.SCALING_UP);
    }

    @Test
    public void shouldScaleUpAndThenStartScalingUp() {
        stateMachine.processStateUpdate(originalStable);
        stateMachine.processStateUpdate(resourceWith(4, 3, 3));
        stateMachine.processStateUpdate(resourceWith(4, 4, 3));

        assertNotNull(scheduledTimeoutTask.get());
        assertEquals(stateMachineData.getCurrentTopology().get(), 4);
        assertEquals(stateMachineData.getLatestStableState().get(), originalStable);
        assertEquals(stateMachineData.getCurrentState().get(), KubernetesClusterState.SCALING_UP_STARTED);
    }

    @Test
    public void shouldGoStraightToScalingUpStarted() {
        stateMachine.processStateUpdate(originalStable);
        stateMachine.processStateUpdate(resourceWith(4, 4, 3));

        assertNotNull(scheduledTimeoutTask.get());
        assertEquals(stateMachineData.getCurrentTopology().get(), 4);
        assertEquals(stateMachineData.getLatestStableState().get(), originalStable);
        assertEquals(stateMachineData.getCurrentState().get(), KubernetesClusterState.SCALING_UP_STARTED);
    }

    @Test
    public void shouldScaleUpAndCancel() {
        StatefulSet newStable = resourceWith(3, 3, 3);
        stateMachine.processStateUpdate(originalStable);
        stateMachine.processStateUpdate(resourceWith(4, 3, 3));
        stateMachine.processStateUpdate(newStable);

        assertNull(scheduledTimeoutTask.get());
        assertEquals(stateMachineData.getCurrentTopology().get(), 3);
        assertEquals(stateMachineData.getLatestStableState().get(), newStable);
        assertEquals(stateMachineData.getCurrentState().get(), KubernetesClusterState.STABLE);
    }

    @Test
    public void shouldGoStraightToScalingUpStartedAndCancel() {
        StatefulSet newStable = resourceWith(3, 3, 3);
        stateMachine.processStateUpdate(originalStable);
        stateMachine.processStateUpdate(resourceWith(4, 4, 3));
        stateMachine.processStateUpdate(newStable);

        assertNull(scheduledTimeoutTask.get());
        assertEquals(stateMachineData.getCurrentTopology().get(), 3);
        assertEquals(stateMachineData.getLatestStableState().get(), newStable);
        assertEquals(stateMachineData.getCurrentState().get(), KubernetesClusterState.STABLE);
    }

    @Test
    public void shouldScaleUpAndDown() {
        stateMachine.processStateUpdate(originalStable);
        stateMachine.processStateUpdate(resourceWith(4, 3, 3));
        stateMachine.processStateUpdate(resourceWith(2, 3, 3));

        assertNull(scheduledTimeoutTask.get());
        assertEquals(stateMachineData.getCurrentTopology().get(), 3);
        assertEquals(stateMachineData.getLatestStableState().get(), originalStable);
        assertEquals(stateMachineData.getCurrentState().get(), KubernetesClusterState.SCALING_DOWN);
    }

    @Test
    public void shouldGoStraightToScalingUpStartedAndScaleDown() {
        stateMachine.processStateUpdate(originalStable);
        stateMachine.processStateUpdate(resourceWith(4, 4, 3));
        stateMachine.processStateUpdate(resourceWith(2, 3, 3));

        assertNull(scheduledTimeoutTask.get());
        assertEquals(stateMachineData.getCurrentTopology().get(), 4);
        assertEquals(stateMachineData.getLatestStableState().get(), originalStable);
        assertEquals(stateMachineData.getCurrentState().get(), KubernetesClusterState.SCALING_DOWN);
    }

    @Test
    public void shouldScaleUpSuccessfully() {
        stateMachine.processStateUpdate(originalStable);

        List<StatefulSet> scale = scale(3, 6);
        scale.forEach(stateMachine::processStateUpdate);

        assertNull(scheduledTimeoutTask.get());
        assertEquals(stateMachineData.getCurrentTopology().get(), 6);
        assertEquals(stateMachineData.getLatestStableState().get(), scale.get(scale.size() - 1));
        assertEquals(stateMachineData.getCurrentState().get(), KubernetesClusterState.STABLE);
    }

    @Test
    public void shouldScaleUpSuccessfully_newNodes() {
        List<StatefulSet> scale = scale(3, 6);
        scale.forEach(stateMachine::processStateUpdate);

        assertNull(scheduledTimeoutTask.get());
        assertEquals(stateMachineData.getCurrentTopology().get(), 6);
        assertEquals(stateMachineData.getLatestStableState().get(), scale.get(scale.size() - 1));
        assertEquals(stateMachineData.getCurrentState().get(), KubernetesClusterState.STABLE);
    }

    @Test
    public void shouldScaleUpAndFail() {
        stateMachine.processStateUpdate(originalStable);

        List<StatefulSet> scale = scale(3, 6);
        scale.subList(0, scale.size() - 1).forEach(stateMachine::processStateUpdate);

        assertNotNull(scheduledTimeoutTask.get());
        assertEquals(stateMachineData.getCurrentTopology().get(), 6);
        assertEquals(stateMachineData.getLatestStableState().get(), originalStable);
        assertEquals(stateMachineData.getCurrentState().get(), KubernetesClusterState.SCALING_UP_STARTED);
    }

    @Test
    public void shouldScaleUpAndFail_newNodes() {
        List<StatefulSet> scale = scale(3, 6);
        scale.subList(0, scale.size() - 1).forEach(stateMachine::processStateUpdate);

        assertNotNull(scheduledTimeoutTask.get());
        assertEquals(stateMachineData.getCurrentTopology().get(), 6);
        assertEquals(stateMachineData.getLatestStableState().get(), scale.get(0));
        assertEquals(stateMachineData.getCurrentState().get(), KubernetesClusterState.SCALING_UP_STARTED);
    }

    @Test
    public void shouldNotDoAnything_rollingUpdate() {
        stateMachine.processStateUpdate(originalStable);
        stateMachine.processStateUpdate(resourceWith(3, 3, 2));

        assertNull(scheduledTimeoutTask.get());
        assertEquals(stateMachineData.getCurrentTopology().get(), 3);
        assertEquals(stateMachineData.getLatestStableState().get(), originalStable);
        assertEquals(stateMachineData.getCurrentState().get(), KubernetesClusterState.STABLE);
    }

    @Test
    public void shouldScaleDownSuccessfully() {
        stateMachine.processStateUpdate(originalStable);

        List<StatefulSet> scale = scale(3, 1);
        scale.forEach(stateMachine::processStateUpdate);

        assertNull(scheduledTimeoutTask.get());
        assertEquals(stateMachineData.getCurrentTopology().get(), 1);
        assertEquals(stateMachineData.getLatestStableState().get(), scale.get(scale.size() - 1));
        assertEquals(stateMachineData.getCurrentState().get(), KubernetesClusterState.STABLE);
    }

    @Test
    public void shouldScaleUpAndDownSuccessfully() {
        stateMachine.processStateUpdate(originalStable);

        List<StatefulSet> scaleUp = scale(3, 6);
        List<StatefulSet> scaleDown = scale(6, 1);
        scaleUp.forEach(stateMachine::processStateUpdate);
        scaleDown.forEach(stateMachine::processStateUpdate);

        assertNull(scheduledTimeoutTask.get());
        assertEquals(stateMachineData.getCurrentTopology().get(), 1);
        assertEquals(stateMachineData.getLatestStableState().get(), scaleDown.get(scaleDown.size() - 1));
        assertEquals(stateMachineData.getCurrentState().get(), KubernetesClusterState.STABLE);
    }

    @Test
    public void shouldScaleDownAndUpSuccessfully() {
        stateMachine.processStateUpdate(originalStable);

        List<StatefulSet> scaleDown = scale(3, 1);
        List<StatefulSet> scaleUp = scale(1, 6);
        scaleDown.forEach(stateMachine::processStateUpdate);
        scaleUp.forEach(stateMachine::processStateUpdate);

        assertNull(scheduledTimeoutTask.get());
        assertEquals(stateMachineData.getCurrentTopology().get(), 6);
        assertEquals(stateMachineData.getLatestStableState().get(), scaleUp.get(scaleUp.size() - 1));
        assertEquals(stateMachineData.getCurrentState().get(), KubernetesClusterState.STABLE);
    }

    @Test
    public void shouldScaleUpAndDown_failure() {
        stateMachine.processStateUpdate(originalStable);

        List<StatefulSet> scaleUp = scale(3, 6);
        List<StatefulSet> scaleDown = scale(6, 1);
        scaleUp.subList(0, scaleUp.size() - 1).forEach(stateMachine::processStateUpdate);
        scaleDown.subList(0, scaleDown.size() - 1).forEach(stateMachine::processStateUpdate);

        assertNull(scheduledTimeoutTask.get());
        assertEquals(stateMachineData.getCurrentTopology().get(), 6);
        assertEquals(stateMachineData.getLatestStableState().get(), originalStable);
        assertEquals(stateMachineData.getCurrentState().get(), KubernetesClusterState.SCALING_DOWN);
    }

    @Test
    public void shouldScaleDownAndUp_failure() {
        stateMachine.processStateUpdate(originalStable);

        List<StatefulSet> scaleDown = scale(3, 1);
        List<StatefulSet> scaleUp = scale(1, 6);
        scaleDown.subList(0, scaleDown.size() - 1).forEach(stateMachine::processStateUpdate);
        scaleUp.subList(0, scaleUp.size() - 1).forEach(stateMachine::processStateUpdate);

        assertNotNull(scheduledTimeoutTask.get());
        assertEquals(stateMachineData.getCurrentTopology().get(), 6);
        assertEquals(stateMachineData.getLatestStableState().get(), originalStable);
        assertEquals(stateMachineData.getCurrentState().get(), KubernetesClusterState.SCALING_UP_STARTED);
    }

    @Test
    public void shouldScaleUpStopAndDownSuccessfully() {
        stateMachine.processStateUpdate(originalStable);

        List<StatefulSet> scaleUp = scale(3, 6);
        scaleUp.subList(0, scaleUp.size() - 1).forEach(stateMachine::processStateUpdate);

        assertNotNull(scheduledTimeoutTask.get());
        assertEquals(stateMachineData.getCurrentTopology().get(), 6);
        assertEquals(stateMachineData.getLatestStableState().get(), originalStable);
        assertEquals(stateMachineData.getCurrentState().get(), KubernetesClusterState.SCALING_UP_STARTED);

        List<StatefulSet> scaleDown = scale(6, 1);
        scaleDown.forEach(stateMachine::processStateUpdate);

        assertNull(scheduledTimeoutTask.get());
        assertEquals(stateMachineData.getCurrentTopology().get(), 1);
        assertEquals(stateMachineData.getLatestStableState().get(), scaleDown.get(scaleDown.size() - 1));
        assertEquals(stateMachineData.getCurrentState().get(), KubernetesClusterState.STABLE);
    }

    @Test
    public void shouldScaleDownStopAndUpSuccessfully() {
        stateMachine.processStateUpdate(originalStable);

        List<StatefulSet> scaleDown = scale(3, 1);
        scaleDown.subList(0, scaleDown.size() - 1).forEach(stateMachine::processStateUpdate);

        assertNull(scheduledTimeoutTask.get());
        assertEquals(stateMachineData.getCurrentTopology().get(), 3);
        assertEquals(stateMachineData.getLatestStableState().get(), originalStable);
        assertEquals(stateMachineData.getCurrentState().get(), KubernetesClusterState.SCALING_DOWN);

        List<StatefulSet> scaleUp = scale(1, 6);
        scaleUp.forEach(stateMachine::processStateUpdate);

        assertNull(scheduledTimeoutTask.get());
        assertEquals(stateMachineData.getCurrentTopology().get(), 6);
        assertEquals(stateMachineData.getLatestStableState().get(), scaleUp.get(scaleUp.size() - 1));
        assertEquals(stateMachineData.getCurrentState().get(), KubernetesClusterState.STABLE);
    }

    @Test
    public void shouldScaleUpAndUpAgainSuccessfully() {
        stateMachine.processStateUpdate(originalStable);

        List<StatefulSet> firstScaleUp = scale(3, 5);
        firstScaleUp.subList(0, firstScaleUp.size() - 1).forEach(stateMachine::processStateUpdate);

        assertNotNull(scheduledTimeoutTask.get());
        assertEquals(stateMachineData.getCurrentTopology().get(), 5);
        assertEquals(stateMachineData.getLatestStableState().get(), originalStable);
        assertEquals(stateMachineData.getCurrentState().get(), KubernetesClusterState.SCALING_UP_STARTED);

        List<StatefulSet> secondScaleUp = scale(5, 7);
        secondScaleUp.forEach(stateMachine::processStateUpdate);

        assertNull(scheduledTimeoutTask.get());
        assertEquals(stateMachineData.getCurrentTopology().get(), 7);
        assertEquals(stateMachineData.getLatestStableState().get(), secondScaleUp.get(secondScaleUp.size() - 1));
        assertEquals(stateMachineData.getCurrentState().get(), KubernetesClusterState.STABLE);
    }

    @Test
    public void shouldScaleUpAndUpAgainSuccessfully_newNode() {

        List<StatefulSet> firstScaleUp = scale(3, 5);
        firstScaleUp.subList(2, firstScaleUp.size() - 1).forEach(stateMachine::processStateUpdate);

        assertNotNull(scheduledTimeoutTask.get());
        assertEquals(stateMachineData.getCurrentTopology().get(), 5);
        assertEquals(stateMachineData.getLatestStableState().get(), firstScaleUp.get(2));
        assertEquals(stateMachineData.getCurrentState().get(), KubernetesClusterState.SCALING_UP_STARTED);

        List<StatefulSet> secondScaleUp = scale(5, 7);
        secondScaleUp.forEach(stateMachine::processStateUpdate);

        assertNull(scheduledTimeoutTask.get());
        assertEquals(stateMachineData.getCurrentTopology().get(), 7);
        assertEquals(stateMachineData.getLatestStableState().get(), secondScaleUp.get(secondScaleUp.size() - 1));
        assertEquals(stateMachineData.getCurrentState().get(), KubernetesClusterState.STABLE);
    }
}