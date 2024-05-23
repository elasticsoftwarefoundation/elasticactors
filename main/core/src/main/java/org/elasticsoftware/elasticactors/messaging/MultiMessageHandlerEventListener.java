/*
 * Copyright 2013 - 2024 The Original Authors
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

package org.elasticsoftware.elasticactors.messaging;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Joost van de Wijgerd
 */
public final class MultiMessageHandlerEventListener implements MessageHandlerEventListener {
    private final MessageHandlerEventListener delegate;
    private final AtomicInteger waitLatch;

    public MultiMessageHandlerEventListener(MessageHandlerEventListener delegate, int numberOfMessages) {
        this.delegate = delegate;
        this.waitLatch = new AtomicInteger(numberOfMessages);
    }

    @Override
    public void onError(InternalMessage message, Throwable exception) {
        if(waitLatch.decrementAndGet() == 0) {
            delegate.onError(message, exception);
        }
    }

    @Override
    public void onDone(InternalMessage message) {
        if(waitLatch.decrementAndGet() == 0) {
            delegate.onDone(message);
        }
    }
}
