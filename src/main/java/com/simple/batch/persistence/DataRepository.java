package com.simple.batch.persistence;

import org.springframework.data.jpa.repository.JpaRepository;

public interface DataRepository extends JpaRepository<DataEntity, Long> {
}
