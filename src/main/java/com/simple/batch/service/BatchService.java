package com.simple.batch.service;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.simple.batch.persistence.DataEntity;
import com.simple.batch.persistence.DataRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.BufferedReader;
import java.io.FileReader;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Service
public class BatchService {

    @Autowired
    private DataRepository repository;

    @Autowired
    private TaskExecutor taskExecutor;

    private static final int BATCH_SIZE = 50;

    // Thread-safe list for batch processing
    private final List<DataEntity> synchronizedList = Collections.synchronizedList(new ArrayList<>());

    public void processFile(String filePath) {
        ObjectMapper mapper = new ObjectMapper();

        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String finalLine = line;
                CompletableFuture.runAsync(() -> {
                    DataEntity entity = processLine(finalLine, mapper);
                    if (entity != null) {
                        addEntityToBatch(entity);
                    }
                }, taskExecutor).exceptionally(ex -> {
                    ex.printStackTrace();
                    return null;
                });
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        // After all lines are processed, check if there's any residual batch to save
        if (!synchronizedList.isEmpty()) {
            saveEntities(new ArrayList<>(synchronizedList));
            synchronizedList.clear();
        }
    }

    private DataEntity processLine(String line, ObjectMapper mapper) {
        try {
            return mapper.readValue(line, DataEntity.class);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private void addEntityToBatch(DataEntity entity) {
        synchronized (synchronizedList) {
            synchronizedList.add(entity);
            if (synchronizedList.size() >= BATCH_SIZE) {
                saveEntities(new ArrayList<DataEntity>(synchronizedList));
                synchronizedList.clear();
            }
        }
    }

    @Transactional
    public void saveEntities(List<DataEntity> entities) {
        entities.forEach(entity -> {
            entity.setCreateDate(LocalDateTime.now());
            entity.setCreateUser("batchUser");
        });
        repository.saveAll(entities);
    }
}
