package org.elasticsoftware.elasticactors.util;

public final class ArrayUtils {

    private ArrayUtils() {
    }

    public static <T> boolean contains(T[] array, T object) {
        for (T currentObject : array) {
            if (currentObject.equals(object)) {
                return true;
            }
        }
        return false;
    }

}
