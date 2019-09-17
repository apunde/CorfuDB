package org.corfudb.runtime.collections;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuOptions;
import org.corfudb.runtime.CorfuStoreMetadata.Timestamp;
import org.corfudb.runtime.object.transactions.Transaction;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.runtime.view.TableRegistry;

/**
 * Created by zlokhandwala on 2019-08-05.
 */
public class TxBuilder {

    private final ObjectsView objectsView;
    private final TableRegistry tableRegistry;
    private final String namespace;
    private Timestamp timestamp;
    private final List<Runnable> operations;

    Set<Descriptors.FieldDescriptor.Type> versionTypes = new HashSet<>(Arrays.asList(
            Descriptors.FieldDescriptor.Type.INT32,
            Descriptors.FieldDescriptor.Type.INT64,
            Descriptors.FieldDescriptor.Type.UINT32,
            Descriptors.FieldDescriptor.Type.UINT64,
            Descriptors.FieldDescriptor.Type.SFIXED32,
            Descriptors.FieldDescriptor.Type.SFIXED64
    ));

    /**
     * Creates a new TxBuilder.
     *
     * @param objectsView   ObjectsView from the Corfu client.
     * @param tableRegistry Table Registry.
     * @param namespace     Namespace boundary defined for the transaction.
     */
    @Nonnull
    TxBuilder(@Nonnull final ObjectsView objectsView,
              @Nonnull final TableRegistry tableRegistry,
              @Nonnull final String namespace) {
        this.objectsView = objectsView;
        this.tableRegistry = tableRegistry;
        this.namespace = namespace;
        this.operations = new ArrayList<>();
    }

    private void validateNamespace(Table table) {
        if (table.getNamespace().equals(this.namespace)) {
            return;
        }
        throw new IllegalArgumentException(
                String.format("Transaction namespace: %s. Attempted transaction on table in namespace: %s",
                        this.namespace, table.getNamespace()));
    }

    private <M extends Message> void validateVersion(@Nullable M previousMetadata,
                                                     @Nullable M userMetadata) {
        if (userMetadata == null) {
            return;
        }
        for (Descriptors.FieldDescriptor fieldDescriptor : userMetadata.getDescriptorForType().getFields()) {
            if (fieldDescriptor.getOptions().getExtension(CorfuOptions.schema).getVersion()) {
                if (!versionTypes.contains(fieldDescriptor.getType())) {
                    throw new IllegalArgumentException("Version field needs to be an Integer or Long type.");
                }
                long validatingVersion = (long) userMetadata.getField(fieldDescriptor);
                long previousVersion = Optional.ofNullable(previousMetadata)
                        .map(m -> (Long) m.getField(fieldDescriptor))
                        .orElse(-1L);
                if (validatingVersion != previousVersion) {
                    throw new RuntimeException("Stale object Exception");
                }
            }
        }
    }

    private <M extends Message> M getNewMetadata(@Nonnull M previousMetadata,
                                                 @Nullable M userMetadata) {
        M.Builder builder = previousMetadata.toBuilder();
        for (Descriptors.FieldDescriptor fieldDescriptor : previousMetadata.getDescriptorForType().getFields()) {
            if (fieldDescriptor.getOptions().getExtension(CorfuOptions.schema).getVersion()) {
                builder.setField(
                        fieldDescriptor,
                        Optional.ofNullable(previousMetadata.getField(fieldDescriptor))
                                .map(previousVersion -> ((Long) previousVersion) + 1)
                                .orElse(0L));
            } else if (userMetadata != null){
                builder.setField(fieldDescriptor, userMetadata.getField(fieldDescriptor));
            }
        }
        return (M) builder.build();
    }

    private <K extends Message, V extends Message, M extends Message>
    Table<K, V, M> getTable(@Nonnull final String tableName) {
        return this.tableRegistry.getTable(this.namespace, tableName);
    }

    /**
     * Creates a record for the specified key.
     *
     * @param tableName Table name to perform the create on.
     * @param key       Key.
     * @param value     Value to create.
     * @param <K>       Type of Key.
     * @param <V>       Type of Value.
     * @return TxBuilder instance.
     */
    @Nonnull
    public <K extends Message, V extends Message, M extends Message>
    TxBuilder create(@Nonnull final String tableName,
                     @Nonnull final K key,
                     @Nonnull final V value,
                     @Nullable final M metadata) {
        Table<K, V, M> table = getTable(tableName);
        validateNamespace(table);
        operations.add(() -> {
            CorfuRecord<V, M> previous = table.get(key);
            if (previous != null) {
                throw new RuntimeException("Cannot create a record on existing key.");
            }
            M newMetadata = null;
            if (table.getMetadataOptions().isMetadataEnabled()) {
                M metadataDefaultInstance = (M) table.getMetadataOptions().getDefaultMetadataInstance();
                newMetadata = getNewMetadata(metadataDefaultInstance, metadata);
            }
            table.create(key, value, newMetadata);
        });
        return this;
    }

    /**
     * Updates the value on the specified key.
     *
     * @param tableName Table name to perform the update on.
     * @param key       Key.
     * @param value     Value to update.
     * @param <K>       Type of Key.
     * @param <V>       Type of Value.
     * @return TxBuilder instance.
     */
    @Nonnull
    public <K extends Message, V extends Message, M extends Message>
    TxBuilder update(@Nonnull final String tableName,
                     @Nonnull final K key,
                     @Nonnull final V value,
                     @Nullable final M metadata) {
        Table<K, V, M> table = getTable(tableName);
        validateNamespace(table);
        operations.add(() -> {
            CorfuRecord<V, M> previous = table.get(key);
            M previousMetadata = Optional.ofNullable(previous)
                    .map(CorfuRecord::getMetadata)
                    .orElseGet(() -> {
                        if (table.getMetadataOptions().isMetadataEnabled()) {
                            return (M) table.getMetadataOptions().getDefaultMetadataInstance();
                        }
                        return null;
                    });
            M newMetadata = null;
            if (previousMetadata != null) {
                validateVersion(previousMetadata, metadata);
                newMetadata = getNewMetadata(previousMetadata, metadata);
            }
            table.update(key, value, newMetadata);
        });
        return this;
    }

    /**
     * Touches the specified key without mutating the version of the record.
     * This provides read after write conflict semantics.
     *
     * @param tableName Table name to perform the touch on.
     * @param key       Key to touch.
     * @param <K>       Type of Key.
     * @param <V>       Type of Value.
     * @return TxBuilder instance.
     */
    @Nonnull
    public <K extends Message, V extends Message, M extends Message>
    TxBuilder touch(@Nonnull final String tableName,
                    @Nonnull final K key) {
        Table<K, V, M> table = getTable(tableName);
        validateNamespace(table);
        operations.add(() -> {
            //TODO: Validate the get is executed.
            CorfuRecord<V, M> record = table.get(key);
        });
        return this;
    }

    /**
     * Deletes the specified key.
     *
     * @param tableName Table name to perform the delete on.
     * @param key       Key.
     * @param <K>       Type of Key.
     * @param <V>       Type of Value.
     * @return TxBuilder instance.
     */
    @Nonnull
    public <K extends Message, V extends Message, M extends Message>
    TxBuilder delete(@Nonnull final String tableName,
                     @Nonnull final K key) {
        Table<K, V, M> table = getTable(tableName);
        validateNamespace(table);
        operations.add(() -> table.delete(key));
        return this;
    }

    private void txBegin() {
        Transaction.TransactionBuilder transactionBuilder = this.objectsView
                .TXBuild()
                .type(TransactionType.OPTIMISTIC);
        if (timestamp != null) {
            transactionBuilder.snapshot(new Token(timestamp.getEpoch(), timestamp.getSequence()));
        }
        transactionBuilder.build().begin();
    }

    private void txEnd() {
        this.objectsView.TXEnd();
    }

    /**
     * Commit the transaction.
     * The commit call begins a Corfu transaction at the latest timestamp, applies all the updates and then
     * ends the Corfu transaction.
     * The commit returns successfully if the transaction was committed.
     * Otherwise this throws a TransactionAbortedException.
     */
    public void commit() {
        commit(null);
    }

    /**
     * Commit the transaction.
     * The commit call begins a Corfu transaction at the specified snapshot or at the latest snapshot
     * if no snapshot is specified, applies all the updates and then ends the Corfu transaction.
     * The commit returns successfully if the transaction was committed.
     * Otherwise this throws a TransactionAbortedException.
     *
     * @param timestamp Timestamp to commit the transaction on.
     */
    public void commit(final Timestamp timestamp) {
        this.timestamp = timestamp;
        try {
            txBegin();
            operations.forEach(Runnable::run);
        } finally {
            txEnd();
            operations.clear();
        }
    }
}
