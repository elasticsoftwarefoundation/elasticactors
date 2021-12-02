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

package org.elasticsoftware.elasticactors.test.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.elasticsoftware.elasticactors.base.serialization.JacksonSerializationFramework;
import org.elasticsoftware.elasticactors.serialization.Message;

/**
 * @author Joost van de Wijgerd
 */
@Message(
    serializationFramework = JacksonSerializationFramework.class,
    durable = true,
    logBodyOnError = true)
public class Greeting {
    private final String who;
    private final boolean checkTraceData;
    private final long delayResponseTime;

    public Greeting(String who) {
        this(who, Boolean.TRUE, 0L);
    }

    public Greeting(String who, Boolean checkTraceData) {
        this(who, checkTraceData, 0L);
    }

    @JsonCreator
    public Greeting(
        @JsonProperty("who") String who,
        @JsonProperty("checkTraceData") Boolean checkTraceData,
        @JsonProperty("delayResponseTime") Long delayResponseTime)
    {
        this.who = who;
        this.checkTraceData = checkTraceData == null || checkTraceData;
        this.delayResponseTime = delayResponseTime != null ? delayResponseTime : 0L;
    }

    public String getWho() {
        return who;
    }

    public boolean isCheckTraceData() {
        return checkTraceData;
    }

    public long getDelayResponseTime() {
        return delayResponseTime;
    }
}
