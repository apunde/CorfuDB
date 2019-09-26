package org.corfudb.runtime.collections;

import static org.corfudb.runtime.collections.QueryOptions.DEFAULT_OPTIONS;

import com.google.protobuf.Message;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.TreeSet;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuStoreMetadata.Timestamp;
import org.corfudb.runtime.object.transactions.Transaction.TransactionBuilder;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.runtime.view.TableRegistry;

/**
 * Created by zlokhandwala on 2019-08-09.
 */
public class Query {

    private final TableRegistry tableRegistry;

    private final ObjectsView objectsView;

    private final String namespace;

    /**
     * Creates a Query interface.
     *
     * @param tableRegistry Table registry from the corfu client.
     * @param objectsView   Objects View from the corfu client.
     * @param namespace     Namespace to perform the queries within.
     */
    public Query(final TableRegistry tableRegistry, final ObjectsView objectsView, final String namespace) {
        this.tableRegistry = tableRegistry;
        this.objectsView = objectsView;
        this.namespace = namespace;
    }

    private Token getToken(Timestamp timestamp) {
        return new Token(timestamp.getEpoch(), timestamp.getSequence());
    }

    private void txBegin(Timestamp timestamp) {
        TransactionBuilder transactionBuilder = objectsView.TXBuild()
                .type(TransactionType.SNAPSHOT);

        if (timestamp != null) {
            transactionBuilder.snapshot(getToken(timestamp));
        }

        transactionBuilder.build().begin();
    }

    private void txEnd() {
        objectsView.TXEnd();
    }

    private <K extends Message, V extends Message, M extends Message>
    Table<K, V, M> getTable(@Nonnull final String tableName) {
        return tableRegistry.getTable(this.namespace, tableName);
    }

    /**
     * Fetch the Value payload for the specified key.
     *
     * @param tableName Table name.
     * @param key       Key.
     * @param <K>       Type of key.
     * @param <V>       Type of value.
     * @return Value.
     */
    @Nullable
    public <K extends Message, V extends Message, M extends Message>
    CorfuRecord<V, M> get(@Nonnull final String tableName,
                          @Nonnull final K key) {
        return get(tableName, null, key);
    }

    /**
     * Fetch the Value payload for the specified key at the specified snapshot.
     *
     * @param tableName Table name.
     * @param timestamp Timestamp to perform the query.
     * @param key       Key.
     * @param <K>       Type of key.
     * @param <V>       Type of value.
     * @return Value.
     */
    @Nullable
    public <K extends Message, V extends Message, M extends Message>
    CorfuRecord<V, M> get(@Nonnull final String tableName,
                          @Nullable final Timestamp timestamp,
                          @Nonnull final K key) {
        try {
            txBegin(timestamp);
            return ((Table<K, V, M>) getTable(tableName)).get(key);
        } finally {
            txEnd();
        }
    }

    /**
     * Fetch the Value payload with the metadata for the specified key.
     *
     * @param tableName Table name.
     * @param key       Key.
     * @param <K>       Type of key.
     * @param <V>       Type of value.
     * @return Corfu Record.
     */
    @Nullable
    public <K extends Message, V extends Message, M extends Message>
    CorfuRecord<V, M> getRecord(@Nonnull final String tableName,
                                @Nonnull final K key) {
        return get(tableName, null, key);
    }

    /**
     * Fetch the Value payload with the metadata for the specified key at the specified timestamp.
     *
     * @param tableName Table name.
     * @param timestamp Timestamp to perform the query on.
     * @param key       Key.
     * @param <K>       Type of key.
     * @param <V>       Type of value.
     * @return Corfu Record.
     */
    @Nullable
    public <K extends Message, V extends Message, M extends Message>
    CorfuRecord<V, M> getRecord(@Nonnull final String tableName,
                                @Nullable final Timestamp timestamp,
                                @Nonnull final K key) {
        try {
            txBegin(timestamp);
            return ((Table<K, V, M>) getTable(tableName)).get(key);
        } finally {
            txEnd();
        }
    }

    /**
     * Gets the count of records in the table.
     *
     * @param tableName Table name.
     * @return Count of records.
     */
    public int count(@Nonnull final String tableName) {
        return count(tableName, null);
    }

    /**
     * Gets the count of records in the table at a particular timestamp.
     *
     * @param tableName Table name.
     * @param timestamp Timestamp to perform the query on.
     * @return Count of records.
     */
    public int count(@Nonnull final String tableName,
                     @Nullable final Timestamp timestamp) {
        try {
            txBegin(timestamp);
            return getTable(tableName).count();
        } finally {
            txEnd();
        }
    }

    @Nonnull
    private <K extends Message, V extends Message, M extends Message>
    Collection<CorfuRecord<V, M>> scanAndFilter(@Nonnull final String tableName,
                                                @Nullable Timestamp timestamp,
                                                @Nonnull final Predicate<CorfuRecord<V, M>> p) {
        try {
            txBegin(timestamp);
            return ((Table<K, V, M>) getTable(tableName)).scanAndFilter(p);
        } finally {
            txEnd();
        }
    }

    /**
     * Query by a secondary index.
     *
     * @param tableName Table name.
     * @param indexName Index name. In case of protobuf-defined secondary index it is the field name.
     * @param indexKey  Key to query.
     * @param <K>       Type of Key.
     * @param <V>       Type of Value.
     * @param <I>       Type of index/secondary key.
     * @return Result of the query.
     */
    @Nonnull
    public <K extends Message, V extends Message, M extends Message, I extends Comparable<I>>
    QueryResult<Entry<K, V>> getByIndex(@Nonnull final String tableName,
                                        @Nonnull final String indexName,
                                        @Nonnull final I indexKey) {
        return new QueryResult<>(((Table<K, V, M>) getTable(tableName)).getByIndex(indexName, indexKey));
    }

    private <V extends Message, M extends Message, R>
    Collection<R> initializeResultCollection(QueryOptions<V, M, R> queryOptions) {
        if (!queryOptions.isDistinct()) {
            return new ArrayList<>();
        }
        if (queryOptions.getComparator() != null) {
            return new TreeSet<>(queryOptions.getComparator());
        }
        return new HashSet<>();
    }

    private <V extends Message, M extends Message, R>
    Collection<R> transform(Collection<CorfuRecord<V, M>> queryResult,
                            Collection<R> resultCollection,
                            Function<CorfuRecord<V, M>, R> projection) {
        return queryResult.stream()
                .map(v -> Optional.ofNullable(projection)
                        .map(function -> function.apply(v))
                        .orElse((R) v))
                .collect(Collectors.toCollection(() -> resultCollection));
    }

    /**
     * Execute a scan and filter query.
     *
     * @param tableName Table name.
     * @param query     Predicate to filter the values.
     * @param <V>       Type of Value.
     * @param <R>       Type of returned projected values.
     * @return Result of the query.
     */
    @Nonnull
    public <V extends Message, M extends Message, R>
    QueryResult<R> exectuteQuery(@Nonnull final String tableName,
                                 @Nonnull final Predicate<CorfuRecord<V, M>> query) {
        return exectuteQuery(tableName, query, DEFAULT_OPTIONS);
    }

    /**
     * Execute a scan and filter query.
     * The query options enables to define:
     * a projection function,
     * timestamp at which the table needs to be queried,
     * Flag to return only distinct results.
     *
     * @param tableName    Table name.
     * @param query        Predicate to filter the values.
     * @param queryOptions Query options.
     * @param <V>          Type of Value.
     * @param <R>          Type of returned projected values.
     * @return Result of the query.
     */
    @Nonnull
    public <V extends Message, M extends Message, R>
    QueryResult<R> exectuteQuery(@Nonnull final String tableName,
                                 @Nonnull final Predicate<CorfuRecord<V, M>> query,
                                 @Nonnull final QueryOptions<V, M, R> queryOptions) {

        Collection<CorfuRecord<V, M>> filterResult = scanAndFilter(tableName, queryOptions.getTimestamp(), query);
        Collection<R> result = initializeResultCollection(queryOptions);
        return new QueryResult<>(transform(filterResult, result, queryOptions.getProjection()));
    }

    /**
     * Execute a join of 2 tables.
     *
     * @param tableName1     Table name 1.
     * @param tableName2     Table name 2.
     * @param query1         Predicate to filter entries in table 1.
     * @param query2         Predicate to filter entries in table 2.
     * @param joinPredicate  Predicate to filter entries during the join.
     * @param joinFunction   Function to merge entries.
     * @param joinProjection Project the merged entries.
     * @param <Val1>         Type of Value in table 1.
     * @param <Val2>         Type of Value in table 2.
     * @param <T>            Type of resultant value after merging type V and type W.
     * @param <U>            Type of value projected from T.
     * @return Result of query.
     */
    @Nonnull
    public <Val1 extends Message, Val2 extends Message, Meta1 extends Message, Meta2 extends Message, T, U>
    QueryResult<U> executeJoinQuery(
            @Nonnull final String tableName1,
            @Nonnull final String tableName2,
            @Nonnull final Predicate<CorfuRecord<Val1, Meta1>> query1,
            @Nonnull final Predicate<CorfuRecord<Val2, Meta2>> query2,
            @Nonnull final BiPredicate<Val1, Val2> joinPredicate,
            @Nonnull final BiFunction<Val1, Val2, T> joinFunction,
            final Function<T, U> joinProjection) {
        return executeJoinQuery(
                tableName1,
                tableName2,
                query1,
                query2,
                DEFAULT_OPTIONS,
                DEFAULT_OPTIONS,
                joinPredicate,
                joinFunction,
                joinProjection);
    }

    /**
     * Execute a join of 2 tables.
     *
     * @param tableName1     Table name 1.
     * @param tableName2     Table name 2.
     * @param query1         Predicate to filter entries in table 1.
     * @param query2         Predicate to filter entries in table 2.
     * @param queryOptions1  Query options to transform table 1 filtered values.
     * @param queryOptions2  Query options to transform table 2 filtered values.
     * @param joinPredicate  Predicate to filter entries during the join.
     * @param joinFunction   Function to merge entries.
     * @param joinProjection Project the merged entries.
     * @param <Val1>         Type of Value in table 1.
     * @param <Val2>         Type of Value in table 2.
     * @param <R>            Type of projected values from table 1 from type V.
     * @param <S>            Type of projected values from table 2 from type W.
     * @param <T>            Type of resultant value after merging type R and type S.
     * @param <U>            Type of value projected from T.
     * @return Result of query.
     */
    @Nonnull
    public <Val1 extends Message, Val2 extends Message, Meta1 extends Message, Meta2 extends Message, R, S, T, U>
    QueryResult<U> executeJoinQuery(
            @Nonnull final String tableName1,
            @Nonnull final String tableName2,
            @Nonnull final Predicate<CorfuRecord<Val1, Meta1>> query1,
            @Nonnull final Predicate<CorfuRecord<Val2, Meta2>> query2,
            @Nonnull final QueryOptions<Val1, Meta1, R> queryOptions1,
            @Nonnull final QueryOptions<Val2, Meta2, S> queryOptions2,
            @Nonnull final BiPredicate<R, S> joinPredicate,
            @Nonnull final BiFunction<R, S, T> joinFunction,
            final Function<T, U> joinProjection) {

        Collection<CorfuRecord<Val1, Meta1>> filterResult1 = scanAndFilter(tableName1, queryOptions1.getTimestamp(), query1);
        Collection<R> queryResult1 = transform(
                filterResult1,
                initializeResultCollection(queryOptions1),
                queryOptions1.getProjection());

        Collection<CorfuRecord<Val2, Meta2>> filterResult2 = scanAndFilter(tableName2, queryOptions2.getTimestamp(), query2);
        Collection<S> queryResult2 = transform(
                filterResult2,
                initializeResultCollection(queryOptions2),
                queryOptions2.getProjection());

        Collection<T> joinResult = new ArrayList<>();

        for (R value1 : queryResult1) {
            for (S value2 : queryResult2) {
                if (!joinPredicate.test(value1, value2)) {
                    continue;
                }

                T resultValue = joinFunction.apply(value1, value2);
                joinResult.add(resultValue);
            }
        }

        return new QueryResult<>(joinResult.stream()
                .map(v -> Optional.ofNullable(joinProjection)
                        .map(function -> function.apply(v))
                        .orElse((U) v))
                .collect(Collectors.toList()));
    }

    /**
     * Merge Function which
     *
     * @param <R> Type of result.
     */
    @FunctionalInterface
    public interface MergeFunction<R> {

        /**
         * Merges the specified argument list.
         *
         * @param arguments Objects across tables to be merged.
         * @return Resultant object.
         */
        R merge(List<Object> arguments);
    }

    private <R> List<R> merge(@Nonnull List<Collection<?>> list,
                              @Nonnull List<Object> mergePayload,
                              @Nonnull MergeFunction<R> func,
                              int depth) {
        Collection<?> collection = list.get(depth);
        if (list.size() - 1 == depth) {
            List<R> result = new ArrayList<>();
            for (Object o : collection) {
                List<Object> finalMergeList = new ArrayList<>(mergePayload);
                finalMergeList.add(o);
                R mergeResult = func.merge(finalMergeList);
                if (mergeResult != null) {
                    result.add(mergeResult);
                }
            }
            return result;
        }

        List<R> result = new ArrayList<>();
        for (Object o : collection) {
            List<Object> mergeList = new ArrayList<>(mergePayload);
            mergeList.add(o);
            result.addAll(merge(list, mergeList, func, depth + 1));
        }
        return result;
    }

    /**
     * Performs join of multiple tables.
     *
     * @param tableNames   Collection of table names to be joined.
     * @param joinFunction MergeFunction to perform the join across the specified tables.
     * @param <R>          Type of resultant Object.
     * @return Result of the query.
     */
    @Nonnull
    public <R> QueryResult<R> executeMultiJoinQuery(@Nonnull final Collection<String> tableNames,
                                                    @Nonnull final MergeFunction<R> joinFunction) {

        List<Collection<? extends Object>> values = new ArrayList<>();
        for (String tableName : tableNames) {
            Collection<Message> messages = scanAndFilter(tableName, null, corfuRecord -> true)
                    .stream()
                    .map(corfuRecord -> corfuRecord.getPayload())
                    .collect(Collectors.toList());
            values.add(messages);
        }

        Collection<R> mergedResults = merge(values, new ArrayList<>(), joinFunction, 0);
        return new QueryResult<>(mergedResults);
    }
}
