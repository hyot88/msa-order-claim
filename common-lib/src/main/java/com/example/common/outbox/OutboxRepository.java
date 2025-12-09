package com.example.common.outbox;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * Outbox 이벤트 저장소
 *
 * outbox-relay 서비스가 미발행 이벤트를 조회하고
 * 발행 완료 후 상태를 업데이트하기 위해 사용합니다.
 */
public interface OutboxRepository extends JpaRepository<OutboxEvent, Long> {

    /**
     * 미발행 이벤트를 잠금과 함께 조회
     *
     * <h3>FOR UPDATE SKIP LOCKED의 의미:</h3>
     * - FOR UPDATE: 조회한 행에 배타적 잠금을 설정 (다른 트랜잭션의 수정 방지)
     * - SKIP LOCKED: 이미 다른 트랜잭션이 잠금한 행은 건너뜀
     *
     * <h3>왜 필요한가?</h3>
     * outbox-relay를 다중 인스턴스로 운영할 때, 각 인스턴스가 서로 다른 이벤트를
     * 처리하도록 보장합니다. 한 인스턴스가 특정 이벤트를 처리 중이면,
     * 다른 인스턴스는 그 이벤트를 건너뛰고 다른 이벤트를 처리합니다.
     *
     * <h3>처리 순서:</h3>
     * id 순서대로 처리하여 이벤트 발행 순서를 보장합니다.
     *
     * @param limit 한 번에 조회할 최대 이벤트 수
     * @return 미발행 이벤트 목록 (잠금 설정됨)
     */
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

    /**
     * 여러 이벤트를 일괄적으로 발행 완료 상태로 변경
     *
     * 이벤트를 Kafka로 발행한 후 호출하여 published 플래그를 true로 설정합니다.
     * 벌크 업데이트로 성능을 최적화합니다.
     *
     * @param ids 발행 완료할 이벤트 ID 목록
     * @return 업데이트된 행의 수
     */
    @Modifying
    @Transactional
    @Query(value = "UPDATE outbox_event SET published = true WHERE id IN (:ids)", nativeQuery = true)
    int markPublishedBulk(@Param("ids") List<Long> ids);
}
