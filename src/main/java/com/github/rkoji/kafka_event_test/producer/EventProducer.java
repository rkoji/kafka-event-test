package com.github.rkoji.kafka_event_test.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.rkoji.kafka_event_test.model.SecurityEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class EventProducer {

	private final KafkaTemplate<String, String> kafkaTemplate;
	private final ObjectMapper objectMapper;

	@Value("${kafka.topics.critical}") private String criticalTopic;
	@Value("${kafka.topics.high}")     private String highTopic;
	@Value("${kafka.topics.medium}")   private String mediumTopic;
	@Value("${kafka.topics.low}")      private String lowTopic;

	public void send(SecurityEvent event) {
		String topic = resolveTopic(event.getGrade());
		try {
			String payload = objectMapper.writeValueAsString(event);
			kafkaTemplate.send(topic, event.getId(), payload);
			log.info("[SEND] grade={} topic={} id={}", event.getGrade(), topic, event.getId());
		} catch (Exception e) {
			log.error("[SEND FAIL] id={} error={}", event.getId(), e.getMessage());
		}
	}

	private String resolveTopic(String grade) {
		return switch (grade) {
			case "CRITICAL" -> criticalTopic;
			case "HIGH"     -> highTopic;
			case "MEDIUM"   -> mediumTopic;
			default         -> lowTopic;
		};
	}
}