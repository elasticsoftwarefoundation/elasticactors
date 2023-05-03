package org.elasticsoftware.elasticactors.test.common;

import org.elasticsoftware.elasticactors.Actor;
import org.elasticsoftware.elasticactors.MethodActor;
import org.elasticsoftware.elasticactors.base.serialization.JacksonSerializationFramework;

@Actor(stateClass = NameActorState.class, serializationFramework = JacksonSerializationFramework.class)
public class NameActor extends MethodActor {
}
