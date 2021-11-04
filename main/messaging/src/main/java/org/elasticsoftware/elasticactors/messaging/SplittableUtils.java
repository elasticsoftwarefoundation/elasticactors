package org.elasticsoftware.elasticactors.messaging;

import com.google.common.collect.ImmutableMap;
import org.elasticsoftware.elasticactors.ActorRef;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public final class SplittableUtils {

    private SplittableUtils() {
    }

    public static ImmutableMap<Integer, InternalMessage> groupByBucket(
        List<ActorRef> receivers,
        Hasher hasher,
        int buckets,
        Function<List<ActorRef>, InternalMessage> messageCopyBuilder)
    {
        /*
         Optimization for cases in which building a small array is less expensive than the map
        */
        if (buckets <= 16) {
            List<ActorRef>[] grouped =
                SplittableUtils.groupByBucketAsArray(receivers, hasher, buckets);
            ImmutableMap.Builder<Integer, InternalMessage> builder =
                ImmutableMap.builderWithExpectedSize(Math.min(receivers.size(), buckets));
            for (int i = 0; i < grouped.length; i++) {
                List<ActorRef> refs = grouped[i];
                if (refs != null) {
                    builder.put(i, messageCopyBuilder.apply(refs));
                }
            }
            return builder.build();
        } else {
            Map<Integer, List<ActorRef>> grouped =
                SplittableUtils.groupByBucket(receivers, hasher, buckets);
            ImmutableMap.Builder<Integer, InternalMessage> builder =
                ImmutableMap.builderWithExpectedSize(grouped.size());
            grouped.forEach((key, refs) -> builder.put(key, messageCopyBuilder.apply(refs)));
            return builder.build();
        }
    }

    public static List<ActorRef>[] groupByBucketAsArray(
        List<ActorRef> actorRefs,
        Hasher hasher,
        int buckets)
    {
        List<ActorRef>[] refs = new List[buckets];
        for (ActorRef ref : actorRefs) {
            int bucket = calculateBucket(ref, hasher, buckets);
            List<ActorRef> refList = refs[bucket];
            if (refList == null) {
                refList = new ArrayList<>();
                refs[bucket] = refList;
            }
            refList.add(ref);
        }
        return refs;
    }

    public static Map<Integer, List<ActorRef>> groupByBucket(
        List<ActorRef> actorRefs,
        Hasher hasher,
        int buckets)
    {
        Map<Integer, List<ActorRef>> map = new HashMap<>();
        for (ActorRef ref : actorRefs) {
            map.computeIfAbsent(
                bucketForHash(calculateHash(ref, hasher), buckets),
                k -> new ArrayList<>()
            ).add(ref);
        }
        return map;
    }

    private static int bucketForHash(int hash, int buckets) {
        return Math.abs(hash) % buckets;
    }

    public static int calculateBucketForEmptyOrSingleActor(
        List<ActorRef> actorRefs,
        Hasher hasher,
        int buckets)
    {
        return bucketForHash(
            calculateHashForEmptyOrSingleActor(actorRefs, hasher),
            buckets
        );
    }

    public static int calculateBucket(
        ActorRef actorRef,
        Hasher hasher,
        int buckets)
    {
        return bucketForHash(calculateHash(actorRef, hasher), buckets);
    }

    public static int calculateHashForEmptyOrSingleActor(List<ActorRef> actorRefs, Hasher hasher) {
        return actorRefs.isEmpty() ? 0 : calculateHash(actorRefs.get(0), hasher);
    }

    public static int calculateHash(ActorRef actorRef, Hasher hasher) {
        String actorId = actorRef.getActorId();
        return actorId == null ? 0 : hasher.hashStringToInt(actorId);
    }
}
