package org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine;

import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import org.elasticsoftware.elasticactors.kubernetes.cluster.TaskScheduler;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.data.KubernetesClusterState;
import org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.data.KubernetesStateMachineData;
import org.springframework.util.ReflectionUtils;
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.util.ReflectionUtils.findField;
import static org.springframework.util.ReflectionUtils.getField;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import static java.util.stream.Collectors.toList;

public class AbstractKubernetesStateMachineTest {

    private KubernetesStateMachine stateMachine;
    private KubernetesStateMachineData stateMachineData;
    private AtomicReference scheduledTimeoutTask;
    private StatefulSet originalStable;

    private static class DefaultKubernetesStateMachine extends AbstractKubernetesStateMachine {

        private DefaultKubernetesStateMachine(TaskScheduler taskScheduler) {
            super(taskScheduler);
        }

        @Override
        public void handleStateUpdate(StatefulSet resource) {
            processStateUpdate(resource);
        }
    }

    @SuppressWarnings("unchecked")
    @BeforeMethod
    public void setUp() {
        ScheduledExecutorService executorService = mock(ScheduledExecutorService.class);
        ScheduledFuture scheduledFuture = mock(ScheduledFuture.class);
        when(executorService.schedule(any(Runnable.class), anyLong(), any())).thenReturn(scheduledFuture);
        TaskScheduler taskScheduler = new TaskScheduler(executorService, 60);
        stateMachine = new DefaultKubernetesStateMachine(taskScheduler);
        Field scheduledTaskField = findField(TaskScheduler.class, "scheduledTask");
        ReflectionUtils.makeAccessible(scheduledTaskField);
        scheduledTimeoutTask = (AtomicReference) getField(scheduledTaskField, taskScheduler);
        Field kubernetesStateMachineDataField = findField(AbstractKubernetesStateMachine.class, "kubernetesStateMachineData");
        ReflectionUtils.makeAccessible(kubernetesStateMachineDataField);
        stateMachineData = (KubernetesStateMachineData) getField(kubernetesStateMachineDataField, stateMachine);
        originalStable = resourceWith(3, 3, 3);
    }

    @Test
    public void shouldInitialize() {
        stateMachine.handleStateUpdate(originalStable);

        assertNull(scheduledTimeoutTask.get());
        assertEquals(stateMachineData.getCurrentTopology().get(), 3);
        assertEquals(stateMachineData.getLatestStableState().get(), originalStable);
        assertEquals(stateMachineData.getCurrentState().get(), KubernetesClusterState.STABLE);
    }

    @Test
    public void shouldScaleUp() {
        stateMachine.handleStateUpdate(originalStable);
        stateMachine.handleStateUpdate(resourceWith(4, 3, 3));

        assertNull(scheduledTimeoutTask.get());
        assertEquals(stateMachineData.getCurrentTopology().get(), 3);
        assertEquals(stateMachineData.getLatestStableState().get(), originalStable);
        assertEquals(stateMachineData.getCurrentState().get(), KubernetesClusterState.SCALING_UP);
    }

    @Test
    public void shouldScaleUpAndThenStartScalingUp() {
        stateMachine.handleStateUpdate(originalStable);
        stateMachine.handleStateUpdate(resourceWith(4, 3, 3));
        stateMachine.handleStateUpdate(resourceWith(4, 4, 3));

        assertNotNull(scheduledTimeoutTask.get());
        assertEquals(stateMachineData.getCurrentTopology().get(), 4);
        assertEquals(stateMachineData.getLatestStableState().get(), originalStable);
        assertEquals(stateMachineData.getCurrentState().get(), KubernetesClusterState.SCALING_UP_STARTED);
    }

    @Test
    public void shouldGoStraightToScalingUpStarted() {
        stateMachine.handleStateUpdate(originalStable);
        stateMachine.handleStateUpdate(resourceWith(4, 4, 3));

        assertNotNull(scheduledTimeoutTask.get());
        assertEquals(stateMachineData.getCurrentTopology().get(), 4);
        assertEquals(stateMachineData.getLatestStableState().get(), originalStable);
        assertEquals(stateMachineData.getCurrentState().get(), KubernetesClusterState.SCALING_UP_STARTED);
    }

    @Test
    public void shouldScaleUpAndCancel() {
        StatefulSet newStable = resourceWith(3, 3, 3);
        stateMachine.handleStateUpdate(originalStable);
        stateMachine.handleStateUpdate(resourceWith(4, 3, 3));
        stateMachine.handleStateUpdate(newStable);

        assertNull(scheduledTimeoutTask.get());
        assertEquals(stateMachineData.getCurrentTopology().get(), 3);
        assertEquals(stateMachineData.getLatestStableState().get(), newStable);
        assertEquals(stateMachineData.getCurrentState().get(), KubernetesClusterState.STABLE);
    }

    @Test
    public void shouldGoStraightToScalingUpStartedAndCancel() {
        StatefulSet newStable = resourceWith(3, 3, 3);
        stateMachine.handleStateUpdate(originalStable);
        stateMachine.handleStateUpdate(resourceWith(4, 4, 3));
        stateMachine.handleStateUpdate(newStable);

        assertNull(scheduledTimeoutTask.get());
        assertEquals(stateMachineData.getCurrentTopology().get(), 3);
        assertEquals(stateMachineData.getLatestStableState().get(), newStable);
        assertEquals(stateMachineData.getCurrentState().get(), KubernetesClusterState.STABLE);
    }

    @Test
    public void shouldScaleUpAndDown() {
        stateMachine.handleStateUpdate(originalStable);
        stateMachine.handleStateUpdate(resourceWith(4, 3, 3));
        stateMachine.handleStateUpdate(resourceWith(2, 3, 3));

        assertNull(scheduledTimeoutTask.get());
        assertEquals(stateMachineData.getCurrentTopology().get(), 3);
        assertEquals(stateMachineData.getLatestStableState().get(), originalStable);
        assertEquals(stateMachineData.getCurrentState().get(), KubernetesClusterState.SCALING_DOWN);
    }

    @Test
    public void shouldGoStraightToScalingUpStartedAndScaleDown() {
        stateMachine.handleStateUpdate(originalStable);
        stateMachine.handleStateUpdate(resourceWith(4, 4, 3));
        stateMachine.handleStateUpdate(resourceWith(2, 3, 3));

        assertNull(scheduledTimeoutTask.get());
        assertEquals(stateMachineData.getCurrentTopology().get(), 4);
        assertEquals(stateMachineData.getLatestStableState().get(), originalStable);
        assertEquals(stateMachineData.getCurrentState().get(), KubernetesClusterState.SCALING_DOWN);
    }

    @Test
    public void shouldScaleUpSuccessfully() {
        stateMachine.handleStateUpdate(originalStable);

        List<StatefulSet> scale = scale(3, 6);
        scale.forEach(stateMachine::handleStateUpdate);

        assertNull(scheduledTimeoutTask.get());
        assertEquals(stateMachineData.getCurrentTopology().get(), 6);
        assertEquals(stateMachineData.getLatestStableState().get(), scale.get(scale.size() - 1));
        assertEquals(stateMachineData.getCurrentState().get(), KubernetesClusterState.STABLE);
    }

    @Test
    public void shouldScaleUpSuccessfully_newNodes() {
        List<StatefulSet> scale = scale(3, 6);
        scale.forEach(stateMachine::handleStateUpdate);

        assertNull(scheduledTimeoutTask.get());
        assertEquals(stateMachineData.getCurrentTopology().get(), 6);
        assertEquals(stateMachineData.getLatestStableState().get(), scale.get(scale.size() - 1));
        assertEquals(stateMachineData.getCurrentState().get(), KubernetesClusterState.STABLE);
    }

    @Test
    public void shouldScaleUpAndFail() {
        stateMachine.handleStateUpdate(originalStable);

        List<StatefulSet> scale = scale(3, 6);
        scale.subList(0, scale.size() - 1).forEach(stateMachine::handleStateUpdate);

        assertNotNull(scheduledTimeoutTask.get());
        assertEquals(stateMachineData.getCurrentTopology().get(), 6);
        assertEquals(stateMachineData.getLatestStableState().get(), originalStable);
        assertEquals(stateMachineData.getCurrentState().get(), KubernetesClusterState.SCALING_UP_STARTED);
    }

    @Test
    public void shouldScaleUpAndFail_newNodes() {
        List<StatefulSet> scale = scale(3, 6);
        scale.subList(0, scale.size() - 1).forEach(stateMachine::handleStateUpdate);

        assertNotNull(scheduledTimeoutTask.get());
        assertEquals(stateMachineData.getCurrentTopology().get(), 6);
        assertEquals(stateMachineData.getLatestStableState().get(), scale.get(0));
        assertEquals(stateMachineData.getCurrentState().get(), KubernetesClusterState.SCALING_UP_STARTED);
    }

    @Test
    public void shouldNotDoAnything_rollingUpdate() {
        stateMachine.handleStateUpdate(originalStable);
        stateMachine.handleStateUpdate(resourceWith(3, 3, 2));

        assertNull(scheduledTimeoutTask.get());
        assertEquals(stateMachineData.getCurrentTopology().get(), 3);
        assertEquals(stateMachineData.getLatestStableState().get(), originalStable);
        assertEquals(stateMachineData.getCurrentState().get(), KubernetesClusterState.STABLE);
    }

    @Test
    public void shouldScaleDownSuccessfully() {
        stateMachine.handleStateUpdate(originalStable);

        List<StatefulSet> scale = scale(3, 1);
        scale.forEach(stateMachine::handleStateUpdate);

        assertNull(scheduledTimeoutTask.get());
        assertEquals(stateMachineData.getCurrentTopology().get(), 1);
        assertEquals(stateMachineData.getLatestStableState().get(), scale.get(scale.size() - 1));
        assertEquals(stateMachineData.getCurrentState().get(), KubernetesClusterState.STABLE);
    }

    @Test
    public void shouldScaleUpAndDownSuccessfully() {
        stateMachine.handleStateUpdate(originalStable);

        List<StatefulSet> scaleUp = scale(3, 6);
        List<StatefulSet> scaleDown = scale(6, 1);
        scaleUp.forEach(stateMachine::handleStateUpdate);
        scaleDown.forEach(stateMachine::handleStateUpdate);

        assertNull(scheduledTimeoutTask.get());
        assertEquals(stateMachineData.getCurrentTopology().get(), 1);
        assertEquals(stateMachineData.getLatestStableState().get(), scaleDown.get(scaleDown.size() - 1));
        assertEquals(stateMachineData.getCurrentState().get(), KubernetesClusterState.STABLE);
    }

    @Test
    public void shouldScaleDownAndUpSuccessfully() {
        stateMachine.handleStateUpdate(originalStable);

        List<StatefulSet> scaleDown = scale(3, 1);
        List<StatefulSet> scaleUp = scale(1, 6);
        scaleDown.forEach(stateMachine::handleStateUpdate);
        scaleUp.forEach(stateMachine::handleStateUpdate);

        assertNull(scheduledTimeoutTask.get());
        assertEquals(stateMachineData.getCurrentTopology().get(), 6);
        assertEquals(stateMachineData.getLatestStableState().get(), scaleUp.get(scaleUp.size() - 1));
        assertEquals(stateMachineData.getCurrentState().get(), KubernetesClusterState.STABLE);
    }

    @Test
    public void shouldScaleUpAndDown_failure() {
        stateMachine.handleStateUpdate(originalStable);

        List<StatefulSet> scaleUp = scale(3, 6);
        List<StatefulSet> scaleDown = scale(6, 1);
        scaleUp.subList(0, scaleUp.size() - 1).forEach(stateMachine::handleStateUpdate);
        scaleDown.subList(0, scaleDown.size() - 1).forEach(stateMachine::handleStateUpdate);

        assertNull(scheduledTimeoutTask.get());
        assertEquals(stateMachineData.getCurrentTopology().get(), 6);
        assertEquals(stateMachineData.getLatestStableState().get(), originalStable);
        assertEquals(stateMachineData.getCurrentState().get(), KubernetesClusterState.SCALING_DOWN);
    }

    @Test
    public void shouldScaleDownAndUp_failure() {
        stateMachine.handleStateUpdate(originalStable);

        List<StatefulSet> scaleDown = scale(3, 1);
        List<StatefulSet> scaleUp = scale(1, 6);
        scaleDown.subList(0, scaleDown.size() - 1).forEach(stateMachine::handleStateUpdate);
        scaleUp.subList(0, scaleUp.size() - 1).forEach(stateMachine::handleStateUpdate);

        assertNotNull(scheduledTimeoutTask.get());
        assertEquals(stateMachineData.getCurrentTopology().get(), 6);
        assertEquals(stateMachineData.getLatestStableState().get(), originalStable);
        assertEquals(stateMachineData.getCurrentState().get(), KubernetesClusterState.SCALING_UP_STARTED);
    }

    @Test
    public void shouldScaleUpStopAndDownSuccessfully() {
        stateMachine.handleStateUpdate(originalStable);

        List<StatefulSet> scaleUp = scale(3, 6);
        scaleUp.subList(0, scaleUp.size() - 1).forEach(stateMachine::handleStateUpdate);

        assertNotNull(scheduledTimeoutTask.get());
        assertEquals(stateMachineData.getCurrentTopology().get(), 6);
        assertEquals(stateMachineData.getLatestStableState().get(), originalStable);
        assertEquals(stateMachineData.getCurrentState().get(), KubernetesClusterState.SCALING_UP_STARTED);

        List<StatefulSet> scaleDown = scale(6, 1);
        scaleDown.forEach(stateMachine::handleStateUpdate);

        assertNull(scheduledTimeoutTask.get());
        assertEquals(stateMachineData.getCurrentTopology().get(), 1);
        assertEquals(stateMachineData.getLatestStableState().get(), scaleDown.get(scaleDown.size() - 1));
        assertEquals(stateMachineData.getCurrentState().get(), KubernetesClusterState.STABLE);
    }

    @Test
    public void shouldScaleDownStopAndUpSuccessfully() {
        stateMachine.handleStateUpdate(originalStable);

        List<StatefulSet> scaleDown = scale(3, 1);
        scaleDown.subList(0, scaleDown.size() - 1).forEach(stateMachine::handleStateUpdate);

        assertNull(scheduledTimeoutTask.get());
        assertEquals(stateMachineData.getCurrentTopology().get(), 3);
        assertEquals(stateMachineData.getLatestStableState().get(), originalStable);
        assertEquals(stateMachineData.getCurrentState().get(), KubernetesClusterState.SCALING_DOWN);

        List<StatefulSet> scaleUp = scale(1, 6);
        scaleUp.forEach(stateMachine::handleStateUpdate);

        assertNull(scheduledTimeoutTask.get());
        assertEquals(stateMachineData.getCurrentTopology().get(), 6);
        assertEquals(stateMachineData.getLatestStableState().get(), scaleUp.get(scaleUp.size() - 1));
        assertEquals(stateMachineData.getCurrentState().get(), KubernetesClusterState.STABLE);
    }

    @Test
    public void shouldScaleUpAndUpAgainSuccessfully() {
        stateMachine.handleStateUpdate(originalStable);

        List<StatefulSet> firstScaleUp = scale(3, 5);
        firstScaleUp.subList(0, firstScaleUp.size() - 1).forEach(stateMachine::handleStateUpdate);

        assertNotNull(scheduledTimeoutTask.get());
        assertEquals(stateMachineData.getCurrentTopology().get(), 5);
        assertEquals(stateMachineData.getLatestStableState().get(), originalStable);
        assertEquals(stateMachineData.getCurrentState().get(), KubernetesClusterState.SCALING_UP_STARTED);

        List<StatefulSet> secondScaleUp = scale(5, 7);
        secondScaleUp.forEach(stateMachine::handleStateUpdate);

        assertNull(scheduledTimeoutTask.get());
        assertEquals(stateMachineData.getCurrentTopology().get(), 7);
        assertEquals(stateMachineData.getLatestStableState().get(), secondScaleUp.get(secondScaleUp.size() - 1));
        assertEquals(stateMachineData.getCurrentState().get(), KubernetesClusterState.STABLE);
    }

    @Test
    public void shouldScaleUpAndUpAgainSuccessfully_newNode() {

        List<StatefulSet> firstScaleUp = scale(3, 5);
        firstScaleUp.subList(2, firstScaleUp.size() - 1).forEach(stateMachine::handleStateUpdate);

        assertNotNull(scheduledTimeoutTask.get());
        assertEquals(stateMachineData.getCurrentTopology().get(), 5);
        assertEquals(stateMachineData.getLatestStableState().get(), firstScaleUp.get(2));
        assertEquals(stateMachineData.getCurrentState().get(), KubernetesClusterState.SCALING_UP_STARTED);

        List<StatefulSet> secondScaleUp = scale(5, 7);
        secondScaleUp.forEach(stateMachine::handleStateUpdate);

        assertNull(scheduledTimeoutTask.get());
        assertEquals(stateMachineData.getCurrentTopology().get(), 7);
        assertEquals(stateMachineData.getLatestStableState().get(), secondScaleUp.get(secondScaleUp.size() - 1));
        assertEquals(stateMachineData.getCurrentState().get(), KubernetesClusterState.STABLE);
    }

    @Test
    public void shouldScaleUpAndUpReduceScaleUp_newNode() {

        List<StatefulSet> firstScaleUp = scale(3, 8).stream()
                .filter(s -> s.getStatus().getReplicas() < 7)
                .collect(toList());
        firstScaleUp.forEach(stateMachine::handleStateUpdate);

        assertNotNull(scheduledTimeoutTask.get());
        assertEquals(stateMachineData.getCurrentTopology().get(), 8);
        assertEquals(stateMachineData.getLatestStableState().get(), firstScaleUp.get(0));
        assertEquals(stateMachineData.getCurrentState().get(), KubernetesClusterState.SCALING_UP_STARTED);

        List<StatefulSet> secondScaleUp = scale(5, 7);

        stateMachine.handleStateUpdate(secondScaleUp.get(0));

        assertNotNull(scheduledTimeoutTask.get());
        assertEquals(stateMachineData.getCurrentTopology().get(), 8);
        assertEquals(stateMachineData.getLatestStableState().get(), firstScaleUp.get(0));
        assertEquals(stateMachineData.getCurrentState().get(), KubernetesClusterState.SCALING_UP_STARTED);

        secondScaleUp.subList(1, secondScaleUp.size()).forEach(stateMachine::handleStateUpdate);

        assertNull(scheduledTimeoutTask.get());
        assertEquals(stateMachineData.getCurrentTopology().get(), 7);
        assertEquals(stateMachineData.getLatestStableState().get(), secondScaleUp.get(secondScaleUp.size() - 1));
        assertEquals(stateMachineData.getCurrentState().get(), KubernetesClusterState.STABLE);
    }

    @Test
    public void shouldScaleDownAndUpReduceScaleDown() {
        originalStable = resourceWith(7, 7, 7);
        stateMachine.handleStateUpdate(originalStable);

        List<StatefulSet> firstScaleDown = scale(7, 2).stream()
                .filter(s -> s.getStatus().getReplicas() > 5)
                .collect(toList());
        firstScaleDown.forEach(stateMachine::handleStateUpdate);

        assertNull(scheduledTimeoutTask.get());
        assertEquals(stateMachineData.getCurrentTopology().get(), 7);
        assertEquals(stateMachineData.getLatestStableState().get(), originalStable);
        assertEquals(stateMachineData.getCurrentState().get(), KubernetesClusterState.SCALING_DOWN);

        List<StatefulSet> secondScaleDown = scale(5, 3);

        stateMachine.handleStateUpdate(secondScaleDown.get(0));

        assertNull(scheduledTimeoutTask.get());
        assertEquals(stateMachineData.getCurrentTopology().get(), 7);
        assertEquals(stateMachineData.getLatestStableState().get(), originalStable);
        assertEquals(stateMachineData.getCurrentState().get(), KubernetesClusterState.SCALING_DOWN);

        secondScaleDown.subList(1, secondScaleDown.size()).forEach(stateMachine::handleStateUpdate);

        assertNull(scheduledTimeoutTask.get());
        assertEquals(stateMachineData.getCurrentTopology().get(), 3);
        assertEquals(stateMachineData.getLatestStableState().get(), secondScaleDown.get(secondScaleDown.size() - 1));
        assertEquals(stateMachineData.getCurrentState().get(), KubernetesClusterState.STABLE);
    }

}