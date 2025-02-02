/*
 * Copyright 2013 - 2025 The Original Authors
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

import org.elasticsoftware.elasticactors.Actor;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.UntypedActor;
import org.elasticsoftware.elasticactors.base.serialization.JacksonSerializationFramework;
import org.elasticsoftware.elasticactors.base.state.JavaScriptActorState;
import org.elasticsoftware.elasticactors.serialization.Message;
import org.elasticsoftware.elasticactors.state.PersistenceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.*;

/**
 * @author Joost van de Wijgerd
 */
@Actor(stateClass = JavaScriptActorState.class, serializationFramework = JacksonSerializationFramework.class)
@PersistenceConfig(persistOnMessages = true)
public final class JavaScriptActor extends UntypedActor {

    private final static Logger staticLogger = LoggerFactory.getLogger(JavaScriptActor.class);

    @Override
    protected Logger initLogger() {
        return staticLogger;
    }

    @Override
    public void postActivate(String previousVersion) throws Exception {
        // need to compile the source (if not already done)
        JavaScriptActorState state = getState(JavaScriptActorState.class);
        if(state.getCompiledScript() == null && state.getScript() != null) {
            ScriptEngine scriptEngine = new ScriptEngineManager().getEngineByName("graal.js");
            if(scriptEngine != null) {
                // nashorn is compilable
                try {
                    CompiledScript compiledScript = ((Compilable) scriptEngine).compile(state.getScript());
                    // store the script for later evaluation
                    state.setCompiledScript(compiledScript);
                    // eval so it's ready for invocations
                    compiledScript.eval();
                } catch(ScriptException e) {
                    logger.error("Problem compiling script for actor with id [{}]", getSelf().getActorId(), e);
                }
            } else {
                logger.error("GraalVM JavaScript ScriptEngine not found. Make sure you have the GraalVM JavaScript engine dependencies on the classpath");
                throw new IllegalStateException("GraalVM JavaScript ScriptEngine not found. Make sure you have the GraalVM JavaScript engine dependencies on the classpath");
            }
        }
    }

    @Override
    public void onReceive(ActorRef sender, Object message) throws Exception {
        JavaScriptActorState state = getState(JavaScriptActorState.class);
        if(state.getCompiledScript() != null) {
            CompiledScript compiledScript = state.getCompiledScript();
            Object result = ((Invocable)compiledScript.getEngine()).invokeFunction("onReceive", sender, message);
            // check if the object is a message
            if(result != null && result.getClass().getAnnotation(Message.class) != null) {
                // assume it's a result for the sender
                sender.tell(result, getSelf());
            }
            // otherwise we assume that no reply is needed or that the reply has been handled inside the script
        }
    }
}
