package com.github.rkoji.kafka_event_test.consumer;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import com.github.rkoji.kafka_event_test.model.SecurityEvent;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import tools.jackson.databind.ObjectMapper;

@Slf4j
@Service
@RequiredArgsConstructor
public class EventConsumer {

	private final ObjectMapper objectMapper;
	private final KafkaTemplate<String, String> kafkaTemplate;

	// 처리 통계
	public static final AtomicLong totalReceived = new AtomicLong(0);
	public static final AtomicLong totalProcessed = new AtomicLong(0);

	// 멱등 처리 : 이미 처리한 id 저장 (인메모리)
	private final Set<String> processedIds = Collections.newSetFromMap(new ConcurrentHashMap<>());

	private static final String DLQ_TOPIC = "event.dlq";
	private static final int MAX_RETRY = 3;

	// CRITICAL - 동기처리
	@KafkaListener(topics = "${kafka.topics.critical}", groupId = "critical-group")
	public void consumeCritical(String payload, Acknowledgment ack) {
		process(payload, ack, "CRITICAL");
	}

	// HIGH - 동기 처리
	@KafkaListener(topics = "${kafka.topics.high}", groupId = "high-group")
	public void consumeHigh(String payload, Acknowledgment ack) {
		process(payload, ack, "HIGH");
	}

	// MEDIUM - 비동기 처리
	@KafkaListener(topics = "${kafka.topics.medium}", groupId = "medium-group")
	public void consumeMedium(String payload, Acknowledgment ack) {
		CompletableFuture.runAsync(() -> process(payload, ack, "MEDIUM"));
	}

	// LOW - 비동기 처리
	@KafkaListener(topics = "${kafka.topics.low}", groupId = "low-group")
	public void consumeLow(String payload, Acknowledgment ack) {
		CompletableFuture.runAsync(() -> process(payload, ack, "LOW"));
	}

	private void process(String payload, Acknowledgment ack, String grade) {
		totalReceived.incrementAndGet();
		int attempt = 0;

		while (attempt < MAX_RETRY) {
			try {
				SecurityEvent event = objectMapper.readValue(payload, SecurityEvent.class);

				// 멱등 처리 : 이미 처리한 메시지면 skip
				if (processedIds.contains(event.getId())) {
					log.warn("[SKIP] 중복 메시지 grade={} id={}", grade, event.getId());
					ack.acknowledge();
					return;
				}

				// 실제 처리
				simulate(event);

				// 처리 완료 후 id 저장 + 오프셋 커밋
				processedIds.add(event.getId());
				totalProcessed.incrementAndGet();
				ack.acknowledge();

				log.info("[DONE] grade={} id={} attempt={}", grade, event.getId(), attempt + 1);
				return;
			} catch (Exception e) {
				attempt++;
				log.warn("[RETRY] grade={} attempt={} error={}", grade, attempt, e.getMessage());

			}
		}

		// 재시도 3번 실패 -> DLQ로 이동
		log.error("[DLQ] grade={} 처리 실패, DLQ로 이동", grade);
		kafkaTemplate.send(DLQ_TOPIC, payload);
		ack.acknowledge(); // DLQ 보낸 후 커밋 (무한 루프 방지)
	}

	private void simulate(SecurityEvent event) throws InterruptedException {
		long delay = switch (event.getGrade()) {
			case "CRITICAL" -> 50L;
			case "HIGH" -> 100L;
			case "MEDIUM" -> 200L;
			default -> 300L;
		};
		Thread.sleep(delay);
	}

}
