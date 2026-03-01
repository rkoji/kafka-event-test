package com.github.rkoji.kafka_event_test.consumer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.kafka.annotation.KafkaListener;
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

	// 처리 통계 (나중에 유실률 측정에 사용)
	public static final AtomicLong totalReceived = new AtomicLong(0);
	public static final AtomicLong totalProcessed = new AtomicLong(0);

	// CRITICAL - 동기 처리 (즉시 처리, 순서 보장)
	@KafkaListener(topics = "${kafka.topics.critical}", groupId = "critical-group")
	public void consumeCritical(String payload) {
		totalReceived.incrementAndGet();
		try{
			SecurityEvent event = objectMapper.readValue(payload, SecurityEvent.class);
			process(event);
			totalProcessed.incrementAndGet();
		}catch (Exception e) {
			log.error("[CRITICAL] 처리 실패 payload={}", payload);
		}
	}

	// HIGH - 동기 처리
	@KafkaListener(topics = "${kafka.topics.high}", groupId = "high-group")
	public void consumeHigh(String payload) {
		totalReceived.incrementAndGet();
		try{
			SecurityEvent event = objectMapper.readValue(payload, SecurityEvent.class);
			process(event);
			totalProcessed.incrementAndGet();
		}catch (Exception e) {
			log.error("[HIGH] 처리 실패 payload={}", payload);
		}
	}

	// MEDIUM - 비동기 처리
	@KafkaListener(topics = "${kafka.topics.medium}", groupId = "medium-group")
	public void consumeMedium(String payload) {
		totalReceived.incrementAndGet();
		CompletableFuture.runAsync(() -> {
			try {
				SecurityEvent event = objectMapper.readValue(payload, SecurityEvent.class);
				process(event);
				totalProcessed.incrementAndGet();
			} catch (Exception e) {
				log.error("[MEDIUM] 처리 실패 payload={}", payload);
			}
		});
	}

	// LOW - 비동기 처리
	@KafkaListener(topics = "${kafka.topics.low}", groupId = "low-group")
	public void consumeLow(String payload) {
		totalReceived.incrementAndGet();
		CompletableFuture.runAsync(() -> {
			try {
				SecurityEvent event = objectMapper.readValue(payload, SecurityEvent.class);
				process(event);
				totalProcessed.incrementAndGet();
			} catch (Exception e) {
				log.error("[LOW] 처리 실패 payload={}", payload);
			}
		});
	}

	private void process(SecurityEvent event) {
		// 실제 처리 로직 시뮬레이션
		try {
			long delay = switch (event.getGrade()) {
				case "CRITICAL" -> 50L;
				case "HIGH" -> 100L;
				case "MEDIUM" -> 200L;
				default -> 300L;
			};
			Thread.sleep(delay);
			log.info("[PROCESS] grade={} id={}", event.getGrade(), event.getId());
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}
}
