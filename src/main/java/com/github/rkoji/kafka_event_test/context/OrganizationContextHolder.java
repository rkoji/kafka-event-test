package com.github.rkoji.kafka_event_test.context;

public class OrganizationContextHolder{

	private static final ThreadLocal<String> holder = new ThreadLocal<>();

	public static void set(String orgId) {
		holder.set(orgId);
	}

	public static String get() {
		return holder.get();
	}

	public static void remove() {
		holder.remove();
	}
}
