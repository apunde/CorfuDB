package org.corfudb.samples;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.util.GitRepositoryState;
import org.docopt.Docopt;
import samples.protobuf.PersonProfile.Name;
import samples.protobuf.Vehicle.Car;

import java.util.Map;

/**
 * Created by zlokhandwala on 10/12/19.
 */
public class CorfuStoreSample {
    private static final String USAGE = "Usage: HelloCorfu [-c <conf>]\n"
            + "Options:\n"
            + " -c <conf>     Set the configuration host and port  [default: localhost:9999]\n";

    /**
     * Internally, the corfuRuntime interacts with the CorfuDB service over TCP/IP sockets.
     *
     * @param configurationString specifies the IP:port of the CorfuService
     *                            The configuration string has format "hostname:port", for example, "localhost:9090".
     * @return a CorfuRuntime object, with which Corfu applications perform all Corfu operations
     */
    private static CorfuRuntime getRuntimeAndConnect(String configurationString) {

        return CorfuRuntime.fromParameters(CorfuRuntime.CorfuRuntimeParameters.builder()
                .build())
                .parseConfigurationString(configurationString)
                .connect();
    }

    @SuppressWarnings("checkstyle:printLine") // Sample code
    public static void main(String[] args) throws Exception {
        // Parse the options given, using docopt.
        Map<String, Object> opts =
                new Docopt(USAGE)
                        .withVersion(GitRepositoryState.getRepositoryState().describe)
                        .parse(args);
        String corfuConfigurationString = (String) opts.get("-c");

        /**
         * First, the application needs to instantiate a CorfuRuntime,
         * which is a Java object that contains all of the Corfu utilities exposed to applications.
         */
        CorfuRuntime runtime = getRuntimeAndConnect(corfuConfigurationString);

        CorfuStore corfuStore = new CorfuStore(runtime);

        String namespace = "aaa";
        String tableName = "profile";

        corfuStore.openTable(namespace,
                tableName,
                Name.class,
                Car.class,
                null,
                TableOptions.builder().build());

        corfuStore.tx(namespace)
                .create(tableName,
                        Name.newBuilder().setFirstName("a").setLastName("x").build(),
                        Car.newBuilder().setColor("red").build(),
                        null)
                .commit();

        CorfuRecord record = corfuStore.query(namespace)
                .getRecord(tableName,
                        Name.newBuilder().setFirstName("a").setLastName("x").build());
        System.out.println("Car = " + record.getPayload());

        corfuStore.tx(namespace)
                .update(tableName,
                        Name.newBuilder().setFirstName("a").setLastName("x").build(),
                        Car.newBuilder().setColor("silver").build(),
                        null)
                .commit();
        record = corfuStore.query(namespace)
                .getRecord(tableName,
                        Name.newBuilder().setFirstName("a").setLastName("x").build());
        System.out.println("Car = " + record.getPayload());
    }
}
