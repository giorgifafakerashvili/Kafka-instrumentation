package org.apache.kafka;

import edu.brown.cs.systems.baggage.DetachedBaggage;

import java.util.HashMap;
import java.util.Map;

public class TracingStorage {
    public static Map<Long, DetachedBaggage> timestampToDetachedBaggage =
            new HashMap<>();

    public static void addTrace(long timestamp, DetachedBaggage detachedBaggage) {
        timestampToDetachedBaggage.put(timestamp, detachedBaggage);
    }

    public static void removeTrace(long timestamp) {
        timestampToDetachedBaggage.remove(timestamp);
    }

    public static boolean hasTrace(long timestamp) {
        return timestampToDetachedBaggage.containsKey(timestamp);
    }

    public static DetachedBaggage getTrace(long timestamp) {
        return timestampToDetachedBaggage.get(timestamp);
    }
}
