package org.elasticsoftware.elasticactors.tracing;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.reflect.Method;

public final class TracingUtils {

    private TracingUtils() {
    }

    @Nullable
    public static String safeToString(@Nullable Object o) {
        return o != null ? o.toString() : null;
    }

    @Nullable
    public static String shorten(@Nullable String s) {
        if (s != null) {
            String[] parts = s.split("\\.");
            for (int i = 0; i < parts.length - 1; i++) {
                if (parts[i].length() > 0) {
                    parts[i] = parts[i].substring(0, 1);
                }
            }
            return String.join(".", parts);
        }
        return null;
    }

    @Nullable
    public static String shorten(@Nullable Class<?> c) {
        if (c != null) {
            return shorten(c.getName());
        }
        return null;
    }

    @Nonnull
    public static String shorten(@Nonnull Class<?>[] p) {
        String[] s = new String[p.length];
        for (int i = 0; i < p.length; i++) {
            s[i] = shorten(p[i]);
        }
        return String.join(",", s);
    }

    @Nullable
    public static String shorten(@Nullable Method m) {
        if (m != null) {
            return shorten(m.getDeclaringClass().getName()) + "." + m.getName() +
                    "(" + shorten(m.getParameterTypes()) + ")";
        }
        return null;
    }

}
