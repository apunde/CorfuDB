package org.corfudb.integration;

import com.google.protobuf.UnknownFieldSet;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.corfudb.browser.CorfuBrowser;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.CorfuDynamicRecord;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxBuilder;
import org.corfudb.test.SampleAppliance;
import org.corfudb.test.SampleSchema;


public class CorfuBrowserIT extends AbstractIT {

    private static String corfuSingleNodeHost;

    private static int corfuStringNodePort;

    private static String singleNodeEndpoint;

    /* A helper method that takes host and port specification, start a single server and
     *  returns a process. */
    private Process runSinglePersistentServer(String host, int port) throws
        IOException {
        return new AbstractIT.CorfuServerRunner()
            .setHost(host)
            .setPort(port)
            .setLogPath(getCorfuServerLogPath(host, port))
            .setSingle(true)
            .runServer();
    }

    /**
     * Load properties for a single node corfu server before each test
     */
    @Before
    public void loadProperties() {
        corfuSingleNodeHost = PROPERTIES.getProperty("corfuSingleNodeHost");
        corfuStringNodePort = Integer.valueOf(PROPERTIES.getProperty(
            "corfuSingleNodePort"));
        singleNodeEndpoint = String.format(
            "%s:%d",
            corfuSingleNodeHost,
            corfuStringNodePort
        );
    }

    /**
     * Create a table and add data to it.  Verify that the browser tool is able
     * to read its contents accurately.
     * @throws IOException
     * @throws NoSuchMethodException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     */
    @Test
    public void browserTest() throws
        IOException,
        NoSuchMethodException,
        IllegalAccessException,
        InvocationTargetException {
        final String namespace = "namespace";
        final String tableName = "table";
        Process corfuServer = runSinglePersistentServer(corfuSingleNodeHost,
            corfuStringNodePort);

        // Start a Corfu runtime
        runtime = createRuntime(singleNodeEndpoint);

        CorfuStore store = new CorfuStore(runtime);

        store.openTable(
            namespace,
            tableName,
            SampleSchema.Uuid.class,
            SampleSchema.Uuid.class,
            SampleSchema.Uuid.class,
            TableOptions.builder().build());

        final long keyUuid = 1L;
        final long valueUuid = 3L;
        final long metadataUuid = 5L;

        SampleSchema.Uuid uuidKey = SampleSchema.Uuid.newBuilder()
            .setMsb(keyUuid)
            .setLsb(keyUuid)
            .build();
        SampleSchema.Uuid uuidVal = SampleSchema.Uuid.newBuilder()
            .setMsb(valueUuid)
            .setLsb(valueUuid)
            .build();
        SampleSchema.Uuid metadata = SampleSchema.Uuid.newBuilder()
            .setMsb(metadataUuid)
            .setLsb(metadataUuid)
            .build();
        TxBuilder tx = store.tx(namespace);
        tx.create(tableName, uuidKey, uuidVal, metadata)
            .update(tableName, uuidKey, uuidVal, metadata)
            .commit();
        runtime.shutdown();

        // Invoke the browser and go through each item
        CorfuBrowser browser = new CorfuBrowser(corfuSingleNodeHost,
            corfuStringNodePort, "", "", "", "", false);
        CorfuTable table = browser.getTable(namespace, tableName);
        browser.printTable(table);
        Assert.assertEquals(1, table.size());
        for(Object obj : table.values()) {
            CorfuDynamicRecord record = (CorfuDynamicRecord)obj;
            Assert.assertEquals(
                UnknownFieldSet.newBuilder().build(),
                record.getPayload().getUnknownFields());
        }
    }

    /**
     * Create a table and add nested protobufs as data to it.  Verify that the
     * browser tool is able to read the contents accurately.
     * @throws IOException
     * @throws NoSuchMethodException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     */
    @Test
    public void nestedProtoTest() throws
        IOException,
        NoSuchMethodException,
        IllegalAccessException,
        InvocationTargetException {
        final String namespace = "namespace";
        final String tableName = "table";
        runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);

        // Start a Corfu runtime
        runtime = createRuntime(singleNodeEndpoint);

        CorfuStore store = new CorfuStore(runtime);

        final long keyUuid = 10L;
        final long ruleIdVal = 50L;
        final long metaUuid = 100L;

        store.openTable(
            namespace,
            tableName,
            SampleSchema.Uuid.class,
            SampleSchema.FirewallRule.class,
            SampleSchema.Uuid.class,
            TableOptions.builder().build());

        SampleSchema.Uuid uuidKey = SampleSchema.Uuid.newBuilder().setLsb(keyUuid)
            .setMsb(keyUuid).build();
        SampleSchema.FirewallRule firewallRuleVal = SampleSchema.FirewallRule.newBuilder()
            .setRuleId(ruleIdVal).setRuleName("Test Rule")
            .setInput(
                SampleAppliance.Appliance.newBuilder().setEndpoint("localhost"))
            .setOutput(
                SampleAppliance.Appliance.newBuilder().setEndpoint("localhost"))
            .build();
        SampleSchema.Uuid uuidMeta = SampleSchema.Uuid.newBuilder().setLsb(metaUuid)
            .setMsb(metaUuid).build();
        TxBuilder tx = store.tx(namespace);
        tx.create(tableName, uuidKey, firewallRuleVal, uuidMeta)
            .update(tableName, uuidKey, firewallRuleVal, uuidMeta)
            .commit();
        runtime.shutdown();

        CorfuBrowser browser = new CorfuBrowser(corfuSingleNodeHost,
            corfuStringNodePort, "", "", "", "", false);
        CorfuTable table = browser.getTable(namespace, tableName);
        browser.printTable(table);
        Assert.assertEquals(1, table.size());

        for(Object obj : table.values()) {
            CorfuDynamicRecord record = (CorfuDynamicRecord)obj;
            Assert.assertEquals(
                UnknownFieldSet.newBuilder().build(),
                record.getPayload().getUnknownFields());
        }
    }
}
