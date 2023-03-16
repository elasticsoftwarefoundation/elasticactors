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

package org.elasticsoftware.elasticactors.logging;

import org.slf4j.Logger;

import java.util.function.Function;

public enum LogLevel {

    TRACE(l -> l::trace, Logger::isTraceEnabled),
    DEBUG(l -> l::debug, Logger::isDebugEnabled),
    INFO(l -> l::info, Logger::isInfoEnabled),
    WARN(l -> l::warn, Logger::isWarnEnabled),
    ERROR(l -> l::error, Logger::isErrorEnabled);

    public interface LogMethod {
        void log(String format, Object... arguments);
    }

    private final Function<Logger, LogMethod> logMethod;
    private final Function<Logger, Boolean> isEnabledMethod;

    LogLevel(Function<Logger, LogMethod> logMethod, Function<Logger, Boolean> isEnabledMethod) {
        this.logMethod = logMethod;
        this.isEnabledMethod = isEnabledMethod;
    }

    public LogMethod prepare(Logger logger) {
        return logMethod.apply(logger);
    }

    public boolean isEnabled(Logger logger) {
        return isEnabledMethod.apply(logger);
    }
}
