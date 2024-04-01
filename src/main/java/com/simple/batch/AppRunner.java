package com.simple.batch;

import com.simple.batch.service.BatchService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class AppRunner implements CommandLineRunner {

    @Autowired
    private BatchService batchService;

    @Override
    public void run(String... args) throws Exception {
        if (args.length > 0) {
            batchService.processFile(args[0]);
        }
    }
}