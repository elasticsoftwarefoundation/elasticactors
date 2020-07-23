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
            int lastIndex = s.lastIndexOf('.');
            if (lastIndex == -1) {
                return s;
            }
            StringBuilder sb = new StringBuilder();
            char c;
            int index = 0;
            if ((c = s.charAt(0)) != '.') {
                sb.append(c);
                index += 1;
            }
            while ((index = s.indexOf('.', index)) > -1 && index < lastIndex) {
                if ((c = s.charAt(index + 1)) != '.') {
                    if (sb.length() > 0) {
                        sb.append('.');
                    }
                    sb.append(c);
                }
                index += 1;
            }
            if (lastIndex < s.length() - 1) {
                if (sb.length() > 0) {
                    sb.append('.');
                }
                sb.append(s, lastIndex + 1, s.length());
            }
            return sb.toString();
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
