package org.elasticsoftware.elasticactors.tracing;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.reflect.Method;
import java.time.Clock;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class TracingUtils {

    private static final ConcurrentMap<Class<?>, String> classCache = new ConcurrentHashMap<>();
    private static final ConcurrentMap<Method, String> methodCache = new ConcurrentHashMap<>();

    private TracingUtils() {
    }

    /**
     * See https://github.com/openzipkin/b3-propagation/issues/6
     */
    @Nonnull
    public static String nextTraceIdHigh(Clock clock, Random prng) {
        long epochSeconds = clock.millis() / 1000;
        int random = prng.nextInt();
        long traceIdHigh = ((epochSeconds & 0xffffffffL) << 32) | (random & 0xffffffffL);
        return toHexString(traceIdHigh);
    }

    @Nonnull
    public static String toHexString(long number) {
        String numberHex = Long.toHexString(number);
        int zeroes = 16 - numberHex.length();
        if (zeroes == 0) {
            return numberHex;
        }
        StringBuilder sb = new StringBuilder(16);
        for (int i = 0; i < zeroes; i++) {
            sb.append('0');
        }
        sb.append(numberHex);
        return sb.toString();
    }

    @Nullable
    public static String safeToString(@Nullable Object o) {
        return o != null ? o.toString() : null;
    }

    @Nullable
    public static String shorten(@Nullable String s) {
        int lastIndex;
        if (s == null || s.isEmpty() || (lastIndex = s.lastIndexOf('.')) == -1) {
            return s;
        }
        StringBuilder sb = new StringBuilder();
        int i = 0;
        char c;
        if ((c = s.charAt(0)) != '.') {
            sb.append(c);
            i += 1;
        }
        for (; (i = s.indexOf('.', i)) > -1 && i < lastIndex; i++) {
            if ((c = s.charAt(i + 1)) != '.') {
                if (sb.length() > 0) {
                    sb.append('.');
                }
                sb.append(c);
            }
        }
        if (lastIndex < s.length() - 1) {
            if (sb.length() > 0) {
                sb.append('.');
            }
            sb.append(s, lastIndex + 1, s.length());
        }
        return sb.toString();
    }

    @Nullable
    public static String shorten(@Nullable Class<?> aClass) {
        if (aClass != null) {
            return classCache.computeIfAbsent(aClass, c -> {
                if (c.isArray()) {
                    return shorten(c.getComponentType().getName()) + "[]";
                } else {
                    return shorten(c.getName());
                }
            });
        }
        return null;
    }

    @Nonnull
    public static String shorten(@Nonnull Class<?>[] classes) {
        if (classes.length == 0) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (Class<?> aClass : classes) {
            if (sb.length() > 0) {
                sb.append(',');
            }
            sb.append(shorten(aClass));
        }
        return sb.toString();
    }

    @Nullable
    public static String shorten(@Nullable Method method) {
        if (method != null) {
            return methodCache.computeIfAbsent(
                    method,
                m -> shorten(m.getDeclaringClass())
                    + '.'
                    + m.getName()
                    + '('
                    + shorten(m.getParameterTypes())
                    + ')'
            );
        }
        return null;
    }

}
