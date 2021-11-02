package org.elasticsoftware.elasticactors.messaging;

import org.elasticsoftware.elasticactors.ActorRef;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public final class SplittableUtils {

    private SplittableUtils() {
    }

    public static Map<Integer, List<ActorRef>> groupByHashValue(
        List<ActorRef> actorRefs,
        Function<String, Integer> hashFunction)
    {
        Map<Integer, List<ActorRef>> map = new HashMap<>();
        for (ActorRef ref : actorRefs) {
            map.computeIfAbsent(calculateHash(ref, hashFunction), k -> new ArrayList<>()).add(ref);
        }
        return map;
    }

    public static int calculateHash(List<ActorRef> actorRefs, Function<String, Integer> hashFunction) {
        return actorRefs.isEmpty() ? 0 : calculateHash(actorRefs.get(0), hashFunction);
    }

    public static int calculateHash(ActorRef actorRef, Function<String, Integer> hashFunction) {
        String actorId = actorRef.getActorId();
        return actorId == null ? 0 : hashFunction.apply(actorId);
    }
}
