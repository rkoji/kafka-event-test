package com.github.rkoji.kafka_event_test.controller;

import java.util.Map;
import java.util.UUID;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;

import com.github.rkoji.kafka_event_test.consumer.EventConsumer;
import com.github.rkoji.kafka_event_test.model.SecurityEvent;
import com.github.rkoji.kafka_event_test.producer.EventProducer;

import lombok.RequiredArgsConstructor;

@RestController
@RequiredArgsConstructor
@RequestMapping("/events")
public class EventController {

	private final EventProducer eventProducer;

	// 단건 이벤트 발송
	@PostMapping
	public String send(
		@RequestParam String grade,
		@RequestParam String message
	) {
		SecurityEvent event = SecurityEvent.of(UUID.randomUUID().toString(), grade, message);
		eventProducer.send(event);
		return "sent: " + event.getId();
	}

	// 대량 이벤트 발송 (부하테스트용)
	@PostMapping("/bulk")
	public String bulk(
		@RequestParam String grade,
		@RequestParam int count
	) {
		for (int i = 0; i < count; i++) {
			SecurityEvent event = SecurityEvent.of(
				UUID.randomUUID().toString(), grade, "bulk-test-" + i
			);
			eventProducer.send(event);
		}
		return "sent " + count + " events / grade=" + grade;
	}

	// 유실률 확인
	@GetMapping("/stats")
	public Map<String,Long> stats() {
		long received = EventConsumer.totalReceived.get();
		long processed = EventConsumer.totalProcessed.get();

		return Map.of(
			"totalReceived", received,
			"totalProcessed", processed,
			"lost", received - processed
		);
	}
}
