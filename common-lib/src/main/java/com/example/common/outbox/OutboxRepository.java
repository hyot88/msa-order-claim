package com.example.common.outbox;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

public interface OutboxRepository extends JpaRepository<OutboxEvent, Long> {

    @Transactional
    @Query(value = """
        SELECT *
        FROM outbox_event
        WHERE published = false
        ORDER BY id
        FOR UPDATE SKIP LOCKED
        LIMIT :limit
        """, nativeQuery = true)
    List<OutboxEvent> lockAndFetchRaw(@Param("limit") int limit);

    @Modifying
    @Transactional
    @Query(value = "UPDATE outbox_event SET published = true WHERE id IN (:ids)", nativeQuery = true)
    int markPublishedBulk(@Param("ids") List<Long> ids);
}
