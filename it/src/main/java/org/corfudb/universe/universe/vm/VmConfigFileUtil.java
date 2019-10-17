package org.corfudb.universe.universe.vm;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

public class VmConfigFileUtil {

    private VmConfigFileUtil() {
        //prevent creating instances
    }

    public static final String VM_PROPERTIES_FILE = "vm.properties";
    public static final String VM_CREDENTIALS_PROPERTIES_FILE = "vm.credentials.properties";

    private static Properties loadPropertiesFile(String propertiesFile) {
        Properties credentials = new Properties();
        URL credentialsUrl = ClassLoader.getSystemResource(propertiesFile);
        try (InputStream is = credentialsUrl.openStream()) {
            credentials.load(is);
        } catch (IOException e) {
            throw new IllegalStateException("Can't load credentials", e);
        }
        return credentials;
    }

    public static Properties loadVmProperties() {
        return loadPropertiesFile(VM_PROPERTIES_FILE);
    }

    public static Properties loadVmCredentialsProperties() {
        return loadPropertiesFile(VM_CREDENTIALS_PROPERTIES_FILE);
    }
}
