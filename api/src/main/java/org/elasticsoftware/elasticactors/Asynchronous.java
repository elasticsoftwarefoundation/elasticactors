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

package org.elasticsoftware.elasticactors;

import java.lang.annotation.*;

/**
 * Use this annotation to annotate methods of {@link ServiceActor} actors to make these methods run on a
 * different thread than the actor thread. Should be used when accessing resources such as URLs or Databases
 * that don't have an asynchronous API.
 *
 * @author Joost van de Wijgerd
 */
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Asynchronous {
    /**
     * Can be used to qualify the executor used, by default set to asyncExecutor
     *
     * @return
     */
    String value() default "asyncExecutor";
}
