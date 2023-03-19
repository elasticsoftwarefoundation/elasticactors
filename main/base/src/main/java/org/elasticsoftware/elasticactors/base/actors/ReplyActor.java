/*
 * Copyright 2013 - 2023 The Original Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package org.elasticsoftware.elasticactors.base.actors;

import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.TempActor;
import org.elasticsoftware.elasticactors.TypedActor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.annotation.Nullable;

/**
 * @author Joost van de Wijgerd
 */
@TempActor(stateClass = ActorDelegate.class)
public final class ReplyActor<T> extends TypedActor<T> {

    private final static Logger staticLogger = LoggerFactory.getLogger(ReplyActor.class);

    @Override
    protected Logger initLogger() {
        return staticLogger;
    }

    @Override
    public void postCreate(ActorRef creator) throws Exception {
        final ActorDelegate delegate = getState(ActorDelegate.class);
        delegate.postCreate(creator);
    }

    @Override
    public void preDestroy(ActorRef destroyer) throws Exception {
        final ActorDelegate delegate = getState(ActorDelegate.class);
        delegate.preDestroy(destroyer);
    }

    @Override
    public void onUndeliverable(ActorRef receiver, Object message) throws Exception {
        final ActorDelegate delegate = getState(ActorDelegate.class);
        try {
            delegate.onUndeliverable(receiver, message);
        } catch (Exception e) {
            if (logger.isErrorEnabled()) {
                logExceptionDuringOnUndeliverable(receiver, message, delegate, e);
            }
            throw e;
        } finally {
            if (delegate.isDeleteAfterReceive()) {
                getSystem().stop(getSelf());
            }
        }
    }

    @Override
    public void onReceive(ActorRef sender, T message) throws Exception {
        final ActorDelegate delegate = getState(ActorDelegate.class);
        try {
            delegate.onReceive(sender, message);
        } catch (Exception e) {
            if (logger.isErrorEnabled()) {
                logExceptionDuringOnReceive(sender, message, delegate, e);
            }
            throw e;
        } finally {
            if (delegate.isDeleteAfterReceive()) {
                getSystem().stop(getSelf());
            }
        }
    }

    private void logExceptionDuringOnUndeliverable(
        ActorRef receiver,
        Object message,
        ActorDelegate delegate,
        Exception thrownException)
    {
        logExceptionThrownDuringOnUndeliverable(
            receiver,
            delegate,
            message,
            thrownException
        );
        if (!delegate.isDeleteAfterReceive()) {
            logExceptionThrownButNotDeletingActor();
        }
    }

    private void logExceptionDuringOnReceive(
        ActorRef sender,
        T message,
        ActorDelegate delegate,
        Exception thrownException)
    {
        logExceptionThrownDuringOnReceive(sender, message, delegate, thrownException);
        if (delegate.isDeleteAfterReceive()) {
            logExceptionThrownButNotDeletingActor();
        }
    }

    private void logExceptionThrownDuringOnReceive(
        ActorRef sender,
        T message,
        ActorDelegate delegate,
        Exception thrownException)
    {
        logger.error(
            "Exception while handling message of type [{}] for a Temporary Actor. "
                + "Sender: [{}]. "
                + "Receiver: [{}]. "
                + "{}{}{}",
            message.getClass().getTypeName(),
            sender,
            getSelf(),
            (delegate.getCreationContext() != null || delegate.getTraceContext() != null)
                ? "When ask was called, the following contexts were in scope:"
                : "",
            toLoggableString(delegate.getCreationContext()),
            toLoggableString(delegate.getTraceContext()),
            thrownException
        );
    }

    private void logExceptionThrownButNotDeletingActor() {
        logger.error(
            "Actor [{}] is not being destroyed right now, but an exception was caught. "
                + "This can cause it to not be destroyed until evicted from the cache.",
            getSelf()
        );
    }

    private void logExceptionThrownDuringOnUndeliverable(
        ActorRef receiver,
        ActorDelegate delegate,
        Object message,
        Exception thrownException)
    {
        logger.error(
            "Exception while handling undeliverable message of type [{}] for a Temporary Actor. "
                + "Sender: [{}]. "
                + "Original receiver: [{}]."
                + "{}"
                + "{}"
                + "{}",
            message.getClass().getTypeName(),
            getSelf(),
            receiver,
            (delegate.getCreationContext() != null || delegate.getTraceContext() != null)
                ? " When the Temporary Actor was created, the following contexts were in scope:"
                : "",
            toLoggableString(delegate.getCreationContext()),
            toLoggableString(delegate.getTraceContext()),
            thrownException
        );
    }

    private static String toLoggableString(@Nullable Object object) {
        return object != null
            ? " " + object + "."
            : "";
    }
}
