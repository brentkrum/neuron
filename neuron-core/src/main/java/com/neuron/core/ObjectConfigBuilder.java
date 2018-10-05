package com.neuron.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public final class ObjectConfigBuilder
{
	public static ObjectConfig config() {
		return new ObjectConfig();
	}
	
	public static class ObjectConfig {
		private final ObjectNode m_node = JSONSerializer.createNode();
		
		public ObjectConfig option(String key, String value) {
			m_node.put(key, value);
			return this;
		}
		
		public String getString(String key, String defaultValue) {
			final JsonNode n = m_node.get(key);
			return (n==null) ? defaultValue : n.asText(defaultValue);
		}
		
		public Integer getInteger(String key, Integer defaultValue) {
			final JsonNode n = m_node.get(key);
			return (n==null) ? defaultValue : n.asInt(defaultValue);
		}
	}
	
	private ObjectConfigBuilder() {
	}
}
