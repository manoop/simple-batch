package com.simple.batch.controller;

import com.simple.batch.service.BatchService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class BatchController {

    @Autowired
    private BatchService batchService;

    @GetMapping("/process-file")
    public String processFile(@RequestParam String filePath) {
        batchService.processFile(filePath);
        return "Batch processing started...";
    }
}

