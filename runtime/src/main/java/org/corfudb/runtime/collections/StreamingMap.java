package org.corfudb.runtime.collections;

import java.util.Map;
import java.util.stream.Stream;

/**
 *
 * This interface provides additional functionality not provided by the standard {@link Map}.
 * In cases when the actual data is not being backed by the heap, {@link Map#values()},
 * {@link Map#keySet()} or {@link Map#entrySet()} will not suffice, since we cannot guarantee
 * that the data-set will fit in the memory.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface StreamingMap<K, V> extends Map<K, V>, AutoCloseable {
    /**
     * sdfasdfdsf.
     * @return asdfsdf
     */
    Stream<Map.Entry<K, V>> entryStream();

    /**
     * dsfasdfads.
     */
    default void close() {
    }
}