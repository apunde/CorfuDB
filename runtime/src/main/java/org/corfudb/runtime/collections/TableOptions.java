package org.corfudb.runtime.collections;

import org.corfudb.runtime.collections.CorfuTable.IndexRegistry;
import lombok.Builder;

import java.io.File;
import java.util.Optional;

/**
 * Created by zlokhandwala on 2019-08-09.
 */
@Builder
public class TableOptions<K, V> {

    private final IndexRegistry<K, V> indexRegistry;

    private final File persistentDataPath;

    public Optional<File> getPersistentDataPath() {
        return Optional.ofNullable(persistentDataPath);
    }
}
