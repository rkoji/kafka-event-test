# Kafka 등급별 이벤트 처리 시스템

## 프로젝트 소개
위험도 등급(CRITICAL/HIGH/MEDIUM/LOW)별로 
Kafka 토픽을 분리하고 처리 방식을 차별화한 시스템

## 기술 스택
Java 17, Spring Boot 4.0.3, Apache Kafka, Docker

## 핵심 구현
- 위험도별 동기/비동기 처리 차별화
- OrganizationContextHolder + TaskDecorator로 
  멀티테넌시 비동기 컨텍스트 전파
- 수동 오프셋 커밋으로 유실률 0% 달성
