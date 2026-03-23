# Kafka 등급별 이벤트 처리 시스템

실무에서 설계한 보안 이벤트 처리 파이프라인을 개인 프로젝트로 구현했습니다.  
위험도 등급별 Kafka 토픽 분리, 멀티테넌시 컨텍스트 전파, 유실률 0% 달성을 목표로 합니다.

---

## 기술 스택

- Java 21
- Spring Boot 4.0.3
- Apache Kafka
- Docker / Docker Compose

---

## 핵심 구현

### 1. 위험도 등급별 처리 방식 차별화

보안 이벤트를 위험도(CRITICAL / HIGH / MEDIUM / LOW)에 따라 토픽을 분리하고 처리 방식을 다르게 적용했습니다.

| 등급 | 처리 방식 | 이유 |
|------|----------|------|
| CRITICAL / HIGH | 동기 처리 | 즉시 처리 및 순서 보장이 필요 |
| MEDIUM / LOW | 비동기 처리 (CompletableFuture) | 처리량 확보가 우선 |

### 2. 멀티테넌시 비동기 컨텍스트 전파

ThreadLocal 기반 `OrganizationContextHolder`로 요청마다 조직(orgId)을 식별합니다.  
비동기 처리 시 ThreadLocal이 새 스레드에 전파되지 않는 문제를 `TaskDecorator`로 해결했습니다.

```
Kafka 리스너 스레드 (orgId set)
        ↓
   runAsync() 호출
        ↓
TaskDecorator가 orgId를 asyncExecutor 스레드로 자동 전파
        ↓
  비동기 처리 (orgId 누락 없음)
```

- **CRITICAL/HIGH (동기)**: try/finally로 직접 set/remove 보장
- **MEDIUM/LOW (비동기)**: TaskDecorator가 set/remove 담당, Consumer는 get()만 사용

### 3. 유실률 0% 달성

- 수동 오프셋 커밋 (`enable-auto-commit: false`) — 처리 완료 후 커밋
- 멱등 처리 — 중복 메시지 skip
- DLQ(Dead Letter Queue) — 처리 실패 메시지 별도 보관

---

## 프로젝트 구조

```
src/main/java/com/github/rkoji/kafka_event_test/
├── config/
│   ├── AppConfig.java                  # 애플리케이션 설정
│   └── TaskDecoratorConfig.java        # ThreadPoolTaskExecutor + TaskDecorator
├── consumer/
│   └── EventConsumer.java              # 등급별 동기/비동기 처리
├── context/
│   └── OrganizationContextHolder.java  # ThreadLocal 기반 컨텍스트
├── controller/
│   └── EventController.java            # X-Org-Id 헤더로 orgId 수신
├── model/
│   └── SecurityEvent.java              # 이벤트 모델
├── producer/
│   └── EventProducer.java              # 등급별 토픽 발행
└── KafkaEventTestApplication.java
```

---

## 실행 방법

**1. Kafka 실행** (로컬 환경에 맞게 실행)

**2. 애플리케이션 실행**
```bash
./gradlew bootRun
```

**3. 이벤트 발행 테스트**
```bash
# CRITICAL 등급
curl -X POST http://localhost:8080/events \
  -H "X-Org-Id: org-001" \
  -H "Content-Type: application/json" \
  -d '{"grade": "CRITICAL", "message": "침입 탐지"}'

# MEDIUM 등급
curl -X POST http://localhost:8080/events \
  -H "X-Org-Id: org-001" \
  -H "Content-Type: application/json" \
  -d '{"grade": "MEDIUM", "message": "포트 스캔 감지"}'
```

---

## 검증 결과

```
[kafka-listener-1] [CONSUME] orgId=org-001 grade=CRITICAL
[kafka-listener-1] [PROCESS] orgId=org-001  ← 동기 처리

[kafka-listener-1] [CONSUME] orgId=org-001 grade=MEDIUM
[async-executor-1] [PROCESS] orgId=org-001  ← 비동기 처리, orgId 정상 전파
[async-executor-1] [DONE]    orgId=org-001 grade=MEDIUM
```

- 유실률 0% 확인
- 비동기 처리 시에도 orgId 누락 없음 검증
