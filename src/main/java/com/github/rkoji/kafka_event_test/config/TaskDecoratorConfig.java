package com.github.rkoji.kafka_event_test.config;

import com.github.rkoji.kafka_event_test.context.OrganizationContextHolder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskDecorator;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;

@Configuration
public class TaskDecoratorConfig {

	@Bean
	public Executor asyncExecutor() {
		ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
		executor.setCorePoolSize(5);
		executor.setMaxPoolSize(10);
		executor.setQueueCapacity(100);
		executor.setThreadNamePrefix("async-executor-");
		executor.setTaskDecorator(contextCopyingDecorator());
		executor.initialize();
		return executor;
	}

	@Bean
	public TaskDecorator contextCopyingDecorator() {
		return runnable -> {
			// 메인 스레드의 orgId 저장
			String orgId = OrganizationContextHolder.get();
			return () -> {
				try {
					// 새 스레드에 orgId 심기
					OrganizationContextHolder.set(orgId);
					runnable.run();
				} finally {
					// 작업 완료 후 반드시 정리
					OrganizationContextHolder.remove();
				}
			};
		};
	}
}
