/*
 *   Copyright 2013 - 2022 The Original Authors
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.elasticsoftware.elasticactors.cluster.tasks.app;

import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ElasticActor;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.cluster.logging.LoggingSettings;
import org.elasticsoftware.elasticactors.cluster.metrics.MetricsSettings;
import org.elasticsoftware.elasticactors.cluster.tasks.ActorLifecycleTask;
import org.elasticsoftware.elasticactors.cluster.tasks.ProtocolFactory;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.MessageHandlerEventListener;
import org.elasticsoftware.elasticactors.state.ActorStateUpdateProcessor;
import org.elasticsoftware.elasticactors.state.PersistentActor;
import org.elasticsoftware.elasticactors.state.PersistentActorRepository;

/**
 * @author Joost van de Wijgerd
 */
public final class ApplicationProtocolFactory implements ProtocolFactory {

    @Override
    public ActorLifecycleTask createHandleMessageTask(
        InternalActorSystem actorSystem,
        ElasticActor receiver,
        ActorRef receiverRef,
        InternalMessage internalMessage,
        PersistentActor persistentActor,
        PersistentActorRepository persistentActorRepository,
        ActorStateUpdateProcessor actorStateUpdateProcessor,
        MessageHandlerEventListener messageHandlerEventListener,
        MetricsSettings metricsSettings,
        LoggingSettings loggingSettings)
    {
        return new HandleMessageTask(
            actorSystem,
            receiver,
            receiverRef,
            internalMessage,
            persistentActor,
            persistentActorRepository,
            actorStateUpdateProcessor,
            messageHandlerEventListener,
            metricsSettings,
            loggingSettings
        );
    }

    @Override
    public ActorLifecycleTask createHandleUndeliverableMessageTask(
        InternalActorSystem actorSystem,
        ElasticActor receiver,
        ActorRef receiverRef,
        InternalMessage internalMessage,
        PersistentActor persistentActor,
        PersistentActorRepository persistentActorRepository,
        MessageHandlerEventListener messageHandlerEventListener,
        MetricsSettings metricsSettings,
        LoggingSettings loggingSettings)
    {
        return new HandleUndeliverableMessageTask(
            actorSystem,
            receiver,
            receiverRef,
            internalMessage,
            persistentActor,
            persistentActorRepository,
            messageHandlerEventListener,
            metricsSettings,
            loggingSettings
        );
    }
}
