package com.simple.batch.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.simple.batch.persistence.DataEntity;
import com.simple.batch.persistence.DataRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.BufferedReader;
import java.io.FileReader;
import java.time.LocalDateTime;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Service
public class BatchService {

    @Value("${app.batch.user}")
    private String user;

    @Autowired
    private DataRepository repository;

    @Autowired
    private TaskExecutor taskExecutor;

    public void processFile(String filePath) {
        ObjectMapper mapper = new ObjectMapper();

        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String finalLine = line;
                CompletableFuture.runAsync(() -> processLine(finalLine, mapper), taskExecutor);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void processLine(String line, ObjectMapper mapper) {
        try {
            DataEntity entity = mapper.readValue(line, DataEntity.class);
            saveEntity(entity);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Transactional
    public void saveEntity(DataEntity entity) {
        entity.setCreateDate(LocalDateTime.now());
        entity.setCreateUser(user);
        repository.save(entity);
    }
}

