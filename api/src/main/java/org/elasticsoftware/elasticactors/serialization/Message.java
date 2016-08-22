/*
 * Copyright 2013 - 2014 The Original Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsoftware.elasticactors.serialization;

import java.lang.annotation.*;

import static org.elasticsoftware.elasticactors.serialization.MessageDeliveryMode.SYSTEM_DEFAULT;

/**
 * @author Joost van de Wijgerd
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Message {
    public static final int NO_TIMEOUT = -1;

    /**
     * Determines which framework will be used to serialize and deserialize this message
     *
     * @return
     */
    Class<? extends SerializationFramework> serializationFramework();

    /**
     * If a message is durable it will always be put on the Message layer. Depending on the implementation of the underlying
     * Messaging fabric the message will also be made durable in the queue. If a message is not durable, messages sent to
     * {@link org.elasticsoftware.elasticactors.ElasticActor}s that live in the same JVM are not send to to Messaging layer
     * but are delivered directly (on a seperate thread).
     *
     * Non durable messages for {@link org.elasticsoftware.elasticactors.ElasticActor}s that reside in a remote JVM are
     * not persisted to disc in the Message layer and thus take up fewer resources.
     *
     * @return  whether this message is durable (defaults to true)
     */
    boolean durable() default true;

    /**
     * If a message is marked as immutable, the framework can implement several optimizations. Most notably: messages that
     * are forwarded or messages that are send to multiple {@link org.elasticsoftware.elasticactors.ElasticActor}s are not
     * only serialized once.
     *
     * @return  whether this message is immutable (defaults to false)
     */
    boolean immutable() default false;

    /**
     * Determines how long a message stays queued up in the underlying messaging service. If it isn't consumed within
     * {@link #timeout()} seconds it will not be delivered to the receiver.
     *
     * This is useful for cases where the ActorSystem gets overloaded and can't keep up. Defaults to -1 (NO_TIMEOUT)
     *
     * @return  the timeout value (in milliseconds) for the message pojo annotation with this annotation
     */
    int timeout() default NO_TIMEOUT;

    /**
     * With the default delivery mode ({@link org.elasticsoftware.elasticactors.serialization.MessageDeliveryMode#STRICT_ORDER})
     * all messages (durable and non-durable) between 2 actors are always guaranteed to arrive in the same order as they were sent.
     * This is achieved by sending all messages via the messaging layer.
     *
     * If you know what you are doing it is also possible to use the {@link org.elasticsoftware.elasticactors.serialization.MessageDeliveryMode#LOCAL_NON_DURABLE_OPTIMIZED}
     * mode. In this case the message will be sent via the in-jvm queues when the actors are co-located on the same jvm.
     * Bare in mind however that this can lead to out-of-order delivery when mixing durable and non-durable messages. So if your
     * logic relies on message order the make all your messages in that flow either durable or non-durable or use
     * {@link org.elasticsoftware.elasticactors.serialization.MessageDeliveryMode#STRICT_ORDER} mode.
     *
     * By default this is set to ({@link org.elasticsoftware.elasticactors.serialization.MessageDeliveryMode#SYSTEM_DEFAULT})
     * which means it will use the globally set value (which defaults to STRICT_ORDER)
     *
     * @return  the {@link org.elasticsoftware.elasticactors.serialization.MessageDeliveryMode} to use for this message
     */
    MessageDeliveryMode deliveryMode() default SYSTEM_DEFAULT;
}
