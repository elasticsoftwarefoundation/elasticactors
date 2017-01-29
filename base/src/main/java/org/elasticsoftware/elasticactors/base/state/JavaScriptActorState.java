/*
 * Copyright 2013 - 2017 The Original Authors
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

package org.elasticsoftware.elasticactors.base.state;

import com.fasterxml.jackson.annotation.JsonIgnore;

import javax.script.CompiledScript;
import java.util.Map;

/**
 * @author Joost van de Wijgerd
 */
public final class JavaScriptActorState extends JacksonActorState<JavaScriptActorState> {
    private String script;
    private Map<String, Object> storedState;
    private transient CompiledScript compiledScript;

    public JavaScriptActorState() {}

    public JavaScriptActorState(String script) {
        this.script = script;
    }

    public JavaScriptActorState(String script, Map<String, Object> storedState) {
        this.script = script;
        this.storedState = storedState;
    }

    public Map<String, Object> getStoredState() {
        return storedState;
    }

    public void setStoredState(Map<String, Object> storedState) {
        this.storedState = storedState;
    }

    public String getScript() {
        return script;
    }

    public void setScript(String script) {
        this.script = script;
    }

    @JsonIgnore
    public CompiledScript getCompiledScript() {
        return compiledScript;
    }

    public void setCompiledScript(CompiledScript compiledScript) {
        this.compiledScript = compiledScript;
    }

    @Override
    public JavaScriptActorState getBody() {
        return this;
    }
}
