package org.elasticsoftware.elasticactors.tracing.service;

import org.apache.logging.log4j.ThreadContext;
import org.apache.logging.log4j.util.Constants;
import org.apache.logging.log4j.util.PropertiesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class Log4j2DiagnosticContext implements DiagnosticContext {

    private static final Logger logger = LoggerFactory.getLogger(Log4j2DiagnosticContext.class);

    private static final String GC_FREE_THREAD_CONTEXT_KEY = "log4j2.garbagefree.threadContextMap";

    private final boolean isGarbageFree;
    private final ThreadLocal<Map<String, String>> threadLocalMap;
    private final ThreadLocal<List<String>> threadLocalRemovalList;
    private final ThreadLocal<Boolean> initialized;

    public Log4j2DiagnosticContext() {
        this.isGarbageFree = Constants.ENABLE_THREADLOCALS
                && PropertiesUtil.getProperties().getBooleanProperty(GC_FREE_THREAD_CONTEXT_KEY);
        if (this.isGarbageFree) {
            logger.info("Starting up with garbage-free mode ON");
            this.threadLocalMap = null;
            this.threadLocalRemovalList = null;
            this.initialized = null;
        } else {
            logger.info("Starting up with garbage-free mode OFF");
            this.threadLocalMap = ThreadLocal.withInitial(HashMap::new);
            this.threadLocalRemovalList = ThreadLocal.withInitial(ArrayList::new);
            this.initialized = ThreadLocal.withInitial(() -> Boolean.FALSE);
        }
    }

    @Override
    public void init() {
        if (!isGarbageFree) {
            Map<String, String> map = threadLocalMap.get();
            if (!map.isEmpty()) {
                logger.warn("Initialized diagnostic context, but it already had values: {}", map);
                map.clear();
            }
            List<String> list = threadLocalRemovalList.get();
            if (!list.isEmpty()) {
                logger.warn(
                        "Initialized diagnostic context, but it already had keys for removal: {}",
                        list);
                list.clear();
            }
            initialized.set(true);
        }
    }

    @Override
    public final void put(@Nonnull String key, @Nullable String value) {
        if (isGarbageFree) {
            if (value != null) {
                ThreadContext.put(key, value);
            } else {
                ThreadContext.remove(key);
            }
        } else {
            if (value != null) {
                threadLocalMap.get().put(key, value);
                threadLocalRemovalList.get().remove(key);
            } else {
                threadLocalMap.get().remove(key);
                if (ThreadContext.containsKey(key)) {
                    threadLocalRemovalList.get().add(key);
                }
            }
        }
    }

    @Override
    public void finish() {
        if (!isGarbageFree) {
            Map<String, String> map = threadLocalMap.get();
            if (!map.isEmpty()) {
                ThreadContext.putAll(map);
            }
            threadLocalMap.remove();
            List<String> list = threadLocalRemovalList.get();
            if (!list.isEmpty()) {
                ThreadContext.removeAll(list);
            }
            threadLocalRemovalList.remove();
            if (!initialized.get()) {
                logger.warn("Finishing uninitialized diagnostic context");
            }
            initialized.remove();
        }
    }
}
