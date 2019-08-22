package org.corfudb.infrastructure.log;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

@Slf4j
public final class FileReader {
    private final FileChannel channel;
    private final FileSender sender;

    FileReader(final FileSender sender, final String path) throws IOException {
        if (Objects.isNull(sender) || path.isEmpty()) {
            throw new IllegalArgumentException("sender and path required");
        }

        this.sender = sender;
        this.channel = FileChannel.open(Paths.get(path), StandardOpenOption.READ);
    }

    void read() throws IOException {
        try {
            transfer();
        } finally {
            log.info("Transfer finished, closing.");
            close();
        }
    }

    void close() throws IOException {
        this.sender.close();
        this.channel.close();
    }

    private void transfer() throws IOException {
        this.sender.transfer(this.channel);
    }
}
