/*
 *   Copyright 2013 - 2019 The Original Authors
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

package org.elasticsoftware.elasticactors.serialization;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author Joost van de Wijgerd
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Message {
    int NO_TIMEOUT = -1;

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
     * Determines whether or not a message's contents can be included in the logs. The format
     * depends on the {@link MessageToStringSerializer} implementation used by the serialization
     * framework (unless the {@value MessageToStringSerializer#LOGGING_USE_TO_STRING_PROPERTY}
     * property is set to {@code true}, in which case the {@link Object#toString()} method is used).
     *
     * <br/><br/>
     * The maximum length of the message as a String can be controlled using the {@value
     * MessageToStringSerializer#LOGGING_MAXIMUM_LENGTH_PROPERTY} property
     * (default: {@value MessageToStringSerializer#DEFAULT_MAX_LENGTH}).
     * Content that's too big will be trimmed and prefixed with the
     * string {@value MessageToStringSerializer#CONTENT_TOO_BIG_PREFIX}.
     *
     * <br/><br/>
     * <strong>IMPORTANT:</strong> due to the fact messages can potentially contain sensitive data,
     * think very carefully about which messages should be logged and which data should be exposed.
     *
     * @return true if the message can be logged, false otherwise.
     */
    boolean loggable() default false;
}
