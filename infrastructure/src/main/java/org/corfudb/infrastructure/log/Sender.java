package org.corfudb.infrastructure.log;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
public class Sender {
    public static void main(String[] args) {
        try {
            FileSender fileSender = new FileSender(8888, "localhost");
            FileReader fileReader = new FileReader(fileSender, "/tmp/0/log/0.log");
            fileReader.read();
        } catch (IOException e) {
            log.error("Transfer error: ", e);
        }
    }
}
