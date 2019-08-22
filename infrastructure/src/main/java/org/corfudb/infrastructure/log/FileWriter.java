package org.corfudb.infrastructure.log;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

@Slf4j
public final class FileWriter {
    private final FileChannel channel;

    FileWriter(final String path) throws IOException {
        if (path.isEmpty()) {
            throw new IllegalArgumentException("path required");
        }
        File file = new File(path);
        if (file.exists()){
            file.delete();
        }
        this.channel = FileChannel.open(Paths.get(path), StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
    }

    void transfer(final SocketChannel channel) throws IOException {
        assert !Objects.isNull(channel);
        long position = 0L;

        this.channel.transferFrom(channel, position, Long.MAX_VALUE);
        System.out.println("Finished writing");
    }

    int write(final ByteBuffer buffer, long position) throws IOException {
        assert !Objects.isNull(buffer);

        int bytesWritten = 0;
        while(buffer.hasRemaining()) {
            bytesWritten += this.channel.write(buffer, position + bytesWritten);
        }

        return bytesWritten;
    }

    void close() throws IOException {
        this.channel.close();
    }
}
