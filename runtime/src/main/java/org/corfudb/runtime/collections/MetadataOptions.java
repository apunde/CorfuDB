package org.corfudb.runtime.collections;

import com.google.protobuf.Message;
import lombok.Builder;
import lombok.Data;

import static lombok.Builder.Default;

/**
 * Created by zlokhandwala on 2019-09-04.
 */
@Data
@Builder
class MetadataOptions {

    @Default
    private final boolean metadataEnabled = false;

    @Default
    private final Message defaultMetadataInstance = null;
}
