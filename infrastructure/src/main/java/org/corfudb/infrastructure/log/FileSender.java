package org.corfudb.infrastructure.log;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.util.Objects;

@Slf4j
public final class FileSender {
    private final InetSocketAddress hostAddress;
    private SocketChannel client;
    FileSender(final int port, final String hostAddress) throws IOException {
        this.hostAddress = new InetSocketAddress(hostAddress, port);
        this.client = SocketChannel.open(this.hostAddress); // opens socket and connects to the remote address
    }

    void transfer(final FileChannel channel) throws IOException {
        assert !Objects.isNull(channel);
        log.info("Starting transfer: " + channel.size());
        System.out.println("Starting transfer: " + channel.size());
        long position = 0L;
        long finalSize = channel.size();
        while(position < finalSize){
            long left = channel.size() - position;
            System.out.println("Left: " + left);
            position += channel.transferTo(0L, left, this.client);
        }
        System.out.println(position);
    }

    SocketChannel getChannel() {
        return this.client;
    }

    void close() throws IOException {
        this.client.close();
    }
}
