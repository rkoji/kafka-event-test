package com.github.rkoji.kafka_event_test.consumer;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import com.github.rkoji.kafka_event_test.context.OrganizationContextHolder;
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
	private final Executor asyncExecutor;

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
		processSync(payload, ack, "CRITICAL");
	}

	// HIGH - 동기 처리
	@KafkaListener(topics = "${kafka.topics.high}", groupId = "high-group")
	public void consumeHigh(String payload, Acknowledgment ack) {
		processSync(payload, ack, "HIGH");
	}

	// MEDIUM - 비동기 처리
	@KafkaListener(topics = "${kafka.topics.medium}", groupId = "medium-group")
	public void consumeMedium(String payload, Acknowledgment ack) {
		try {
			SecurityEvent event = objectMapper.readValue(payload, SecurityEvent.class);
			OrganizationContextHolder.set(event.getOrgId()); // 리스너 스레드에서 먼저 심기
			CompletableFuture.runAsync(() -> processAsync(payload, ack, "MEDIUM"), asyncExecutor);
		} catch (Exception e) {
			log.error("[ERROR] MEDIUM 파싱 실패: {}", e.getMessage());
			ack.acknowledge();
		}
	}

	// LOW - 비동기 처리
	@KafkaListener(topics = "${kafka.topics.low}", groupId = "low-group")
	public void consumeLow(String payload, Acknowledgment ack) {
		try {
			SecurityEvent event = objectMapper.readValue(payload, SecurityEvent.class);
			OrganizationContextHolder.set(event.getOrgId()); // 리스너 스레드에서 먼저 심기
			CompletableFuture.runAsync(() -> processAsync(payload, ack, "LOW"), asyncExecutor);
		} catch (Exception e) {
			log.error("[ERROR] LOW 파싱 실패: {}", e.getMessage());
			ack.acknowledge();
		}
	}

	// 동기용 (CRITICAL/HIGH)
	private void processSync(String payload, Acknowledgment ack, String grade) {
		totalReceived.incrementAndGet();
		int attempt = 0;

		while (attempt < MAX_RETRY) {
			try {
				SecurityEvent event = objectMapper.readValue(payload, SecurityEvent.class);
				OrganizationContextHolder.set(event.getOrgId());

				if (processedIds.contains(event.getId())) {
					log.warn("[SKIP] 중복 메시지 grade={} id={}", grade, event.getId());
					ack.acknowledge();
					return;
				}

				simulate(event);
				processedIds.add(event.getId());
				totalProcessed.incrementAndGet();
				ack.acknowledge();
				log.info("[DONE] grade={} id={} orgId={} attempt={}",
					grade, event.getId(), OrganizationContextHolder.get(), attempt + 1);
				return;

			} catch (Exception e) {
				attempt++;
				log.warn("[RETRY] grade={} attempt={} error={}", grade, attempt, e.getMessage());
			} finally {
				OrganizationContextHolder.remove(); // 무조건 정리
			}
		}

		log.error("[DLQ] grade={} 처리 실패, DLQ로 이동", grade);
		kafkaTemplate.send(DLQ_TOPIC, payload);
		ack.acknowledge();
	}

	// 비동기용 (MEDIUM/LOW)
	private void processAsync(String payload, Acknowledgment ack, String grade) {
		totalReceived.incrementAndGet();
		int attempt = 0;

		while (attempt < MAX_RETRY) {
			try {
				SecurityEvent event = objectMapper.readValue(payload, SecurityEvent.class);
				// set/remove는 TaskDecorator가 담당, get()만 사용
				log.info("[PROCESS] orgId={}", OrganizationContextHolder.get());

				if (processedIds.contains(event.getId())) {
					log.warn("[SKIP] 중복 메시지 grade={} id={}", grade, event.getId());
					ack.acknowledge();
					return;
				}

				simulate(event);
				processedIds.add(event.getId());
				totalProcessed.incrementAndGet();
				ack.acknowledge();
				log.info("[DONE] grade={} id={} orgId={} attempt={}",
					grade, event.getId(), OrganizationContextHolder.get(), attempt + 1);
				return;

			} catch (Exception e) {
				attempt++;
				log.warn("[RETRY] grade={} attempt={} error={}", grade, attempt, e.getMessage());
			}
		}

		log.error("[DLQ] grade={} 처리 실패, DLQ로 이동", grade);
		kafkaTemplate.send(DLQ_TOPIC, payload);
		ack.acknowledge();
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
