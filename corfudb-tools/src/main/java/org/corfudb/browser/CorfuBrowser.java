package org.corfudb.browser;

import com.google.common.reflect.TypeToken;

import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.core.joran.spi.JoranException;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.CorfuDynamicKey;
import org.corfudb.runtime.collections.CorfuDynamicRecord;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.util.serializer.DynamicProtobufSerializer;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;

/**
 * This is the UFO Browser Tool which prints data in a given namespace and table.
 *
 * - Created by pmajmudar on 10/16/2019.
 */
@Slf4j
public class CorfuBrowser {
    private final CorfuRuntime runtime;
    private final String singleNodeEndpoint;
    private static final int SERIALIZER_TYPE = 25;
    private static final String LOG_LEVEL = "INFO";

    /**
     * Creates a CorfuBrowser which connects a runtime to the server.
     * @param host Corfu Server Host
     * @param port Corfu Server Port
     * @param keyStore Location of the KeyStore File
     * @param ksPasswordFile Location of the KeyStore Password File
     * @param trustStore Location of the Truststore File
     * @param tsPasswordFile Location of the Truststore Password File
     * @param tlsEnabled Tls Enabled Status
     */
    public CorfuBrowser(String host, int port, String keyStore,
        String ksPasswordFile, String trustStore, String tsPasswordFile,
        boolean tlsEnabled) {
        configureLogger();
        runtime = CorfuRuntime.fromParameters(
            CorfuRuntime.CorfuRuntimeParameters.builder().useFastLoader(false)
        .cacheDisabled(true)
        .tlsEnabled(tlsEnabled)
        .keyStore(keyStore)
        .ksPasswordFile(ksPasswordFile)
        .trustStore(trustStore)
        .tsPasswordFile(tsPasswordFile).build());
        singleNodeEndpoint = String.format("%s:%d", host, port);
        connectRuntime(singleNodeEndpoint);
    }

    private void connectRuntime(String endpoint) {
        runtime.parseConfigurationString(endpoint);
        runtime.connect();
        log.info("Successfully connected to {}", endpoint);
    }

    /**
     * Setup logback logger
     * - pick the correct logging level before outputting error messages
     * - add serverEndpoint information
     *
     * @throws JoranException logback exception
     */
    private static void configureLogger() {
        final Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        final Level level = Level.toLevel(LOG_LEVEL);
        root.setLevel(level);
    }

    /**
     * Fetches the table from the given namespace
     * @param namespace Namespace of the table
     * @param tableName Tablename
     * @return CorfuTable
     */
    public CorfuTable getTable(String namespace, String tableName) {
        ISerializer dynamicProtobufSerializer =
            new DynamicProtobufSerializer((byte)SERIALIZER_TYPE, runtime);
        Serializers.registerSerializer(dynamicProtobufSerializer);

        CorfuTable<CorfuDynamicKey, CorfuDynamicRecord> corfuTable =
            runtime.getObjectsView().build()
            .setTypeToken(new TypeToken<CorfuTable<CorfuDynamicKey, CorfuDynamicRecord>>() {
            })
            .setStreamName(
                TableRegistry.getFullyQualifiedTableName(namespace, tableName))
            .setSerializer(dynamicProtobufSerializer)
            .open();
        return corfuTable;
    }

    /**
     * Prints the payload and metadata in the given table
     * @param table
     */
    public void printTable(CorfuTable table) {
        for (Object obj : table.values()) {
            CorfuDynamicRecord record = (CorfuDynamicRecord)obj;
            log.info("Payload: {}", record.getPayload());
            log.info("Metadata: {}", record.getMetadata());
            log.info("===================");
        }
    }

}
