package org.corfudb.browser;

import java.util.Map;

import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.util.GitRepositoryState;
import org.docopt.Docopt;

/**
 * Main class for the UFO Browser Tool.
 * Command line options are documented in the USAGE variable.
 *
 * - Created by pmajmudar on 10/16/2019
 */
public class CorfuBrowserMain {
    private static final String USAGE = "Usage: corfu-browser --host=<host> " +
        "--port=<port> --namespace=<namespace> --tablename=<tablename> " +
        "[--keystore=<keystore_file>] [--ks_password=<keystore_password>] " +
        "[--truststore=<truststore_file>] [--truststore_password=<truststore_password>] " +
        "[--tlsEnabled=<tls_enabled>]\n"
        + "Options:\n"
        + "--host=<host>   Hostname\n"
        + "--port=<port>   Port\n"
        + "--namespace=<namespace>   Namespace\n"
        + "--tablename=<tablename>   Table Name\n"
        + "--keystore=<keystore_file> KeyStore File\n"
        + "--ks_password=<keystore_password> KeyStore Password\n"
        + "--truststore=<truststore_file> TrustStore File\n"
        + "--truststore_password=<truststore_password> Truststore Password\n"
        + "--tlsEnabled=<tls_enabled>";

    public static void main(String[] args) {
        // Parse the options given, using docopt.
        Map<String, Object> opts =
            new Docopt(USAGE)
                .withVersion(GitRepositoryState.getRepositoryState().describe)
                .parse(args);
        String host = opts.get("--host").toString();
        Integer port = Integer.parseInt(opts.get("--port").toString());
        String keystore = opts.get("--keystore") == null ? "" :
            opts.get("--keystore").toString();
        String ks_password = opts.get("--ks_password") == null? "" :
            opts.get("--ks_password").toString();
        String truststore = opts.get("--truststore") == null ? "" :
            opts.get("--truststore").toString();
        String truststore_password =
            opts.get("--truststore_password") == null? "" :
                opts.get("--truststore_password").toString();
        boolean tlsEnabled = opts.get("--tlsEnabled") == null ? false :
            Boolean.parseBoolean(opts.get("--tlsEnabled").toString());

        CorfuBrowser browser = new CorfuBrowser(host, port, keystore,
            ks_password, truststore, truststore_password, tlsEnabled);
        CorfuTable table = browser.getTable(opts.get("--namespace").toString(),
            opts.get("--tablename").toString());
        browser.printTable(table);
    }
}
