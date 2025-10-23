package com.example.outbox;

import org.springframework.data.jpa.repository.*;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, Long> {

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

    // (옵션) 벌크 업데이트가 필요할 때 사용 가능. 이번 구현은 엔티티 변경 flush로 처리하므로 필수는 아님.
    @Modifying
    @Transactional
    @Query(value = "UPDATE outbox_event SET published = true WHERE id IN (:ids)", nativeQuery = true)
    int markPublishedBulk(@Param("ids") List<Long> ids);
}
