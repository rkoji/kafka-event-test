package com.github.rkoji.kafka_event_test.controller;

import java.util.Map;
import java.util.UUID;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.github.rkoji.kafka_event_test.consumer.EventConsumer;
import com.github.rkoji.kafka_event_test.model.SecurityEvent;
import com.github.rkoji.kafka_event_test.producer.EventProducer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/events")
public class EventController {

	private final EventProducer eventProducer;

	// 단건 이벤트 발송
	@PostMapping
	public String send(
		@RequestParam String grade,
		@RequestParam String message,
		@RequestHeader("X-Org-Id") String orgId) {
		SecurityEvent event = SecurityEvent.of(UUID.randomUUID().toString(), grade, message, orgId);
		eventProducer.send(event);
		return "sent: " + event.getId();
	}

	// Kafka 비동기 방식 (After)
	@PostMapping("/bulk")
	public Map<String, Object> bulk(
		@RequestParam String grade,
		@RequestParam int count,
		@RequestHeader("X-Org-Id") String orgId) {
		EventConsumer.totalReceived.set(0);
		EventConsumer.totalProcessed.set(0);

		long start = System.currentTimeMillis();
		for (int i = 0; i < count; i++) {
			SecurityEvent event = SecurityEvent.of(
				UUID.randomUUID().toString(), grade, "bulk-test-" + i, orgId
			);
			eventProducer.send(event);
		}
		long elapsed = System.currentTimeMillis() - start;

		return Map.of(
			"mode", "kafka-async",
			"count", count,
			"sendTimeMs", elapsed
		);
	}

	// HTTP 동기 방식 (Before 시뮬레이션)
	@PostMapping("/bulk-sync")
	public Map<String, Object> bulkSync(@RequestParam String grade,
		@RequestParam int count) {
		long start = System.currentTimeMillis();
		for (int i = 0; i < count; i++) {
			try {
				long delay = switch (grade) {
					case "CRITICAL" -> 50L;
					case "HIGH" -> 100L;
					case "MEDIUM" -> 200L;
					default -> 300L;
				};
				Thread.sleep(delay);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
		long elapsed = System.currentTimeMillis() - start;

		return Map.of(
			"mode", "http-sync",
			"count", count,
			"sendTimeMs", elapsed
		);
	}

	// 유실률 확인
	@GetMapping("/stats")
	public Map<String, Long> stats() {
		long received = EventConsumer.totalReceived.get();
		long processed = EventConsumer.totalProcessed.get();
		return Map.of(
			"totalReceived", received,
			"totalProcessed", processed,
			"lost", received - processed
		);
	}
}