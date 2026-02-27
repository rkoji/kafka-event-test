package com.github.rkoji.kafka_event_test.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SecurityEvent {

	private String id;
	private String grade;	// CRITICAL, HIGH, MEDIUM, LOW
	private String message;
	private long timestamp;

	public static SecurityEvent of(String id, String grade, String message) {
		return SecurityEvent.builder()
			.id(id)
			.grade(grade)
			.message(message)
			.timestamp(System.currentTimeMillis())
			.build();
	}
}
