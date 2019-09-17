package org.corfudb.integration;

import org.corfudb.runtime.collections.*;
import org.corfudb.test.SampleSchema;
import org.junit.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class CorfuStoreIT extends AbstractIT {

    @Test
    public void testTx() throws Exception {
        CorfuStore store = new CorfuStore(runtime);

        Table<SampleSchema.Uuid, SampleSchema.Uuid, SampleSchema.Uuid> table = store.createTable(
                "namespace", "table", SampleSchema.Uuid.class,
                SampleSchema.Uuid.class, SampleSchema.Uuid.class,
                TableOptions.builder().build()
        );
        UUID keyUuid = UUID.randomUUID();
        UUID valueUuid = UUID.randomUUID();
        UUID metadataUuid = UUID.randomUUID();
        SampleSchema.Uuid uuidKey = SampleSchema.Uuid.newBuilder()
                .setMsb(keyUuid.getMostSignificantBits())
                .setLsb(keyUuid.getLeastSignificantBits())
                .build();
        SampleSchema.Uuid uuidVal = SampleSchema.Uuid.newBuilder()
                .setMsb(valueUuid.getMostSignificantBits())
                .setLsb(valueUuid.getLeastSignificantBits())
                .build();
        SampleSchema.Uuid metadata = SampleSchema.Uuid.newBuilder()
                .setMsb(metadataUuid.getMostSignificantBits())
                .setLsb(metadataUuid.getLeastSignificantBits())
                .build();
        TxBuilder tx = store.tx("namespace");
        tx.create("table", uuidKey, uuidVal,metadata)
                .update("table", uuidKey, uuidVal, metadata)
                .commit();
        CorfuRecord record = table.get(uuidKey);
        assertThat(record.getPayload()).isEqualTo(uuidVal);
        assertThat(record.getMetadata()).isEqualTo(metadata);
    }
}
