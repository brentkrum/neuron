package com.neuron.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.POJONode;
import com.neuron.core.serializer.json.JSONSerializer;

public final class ObjectConfigBuilder
{
	private static final ObjectConfig EMPTY_CONFIG = ObjectConfigBuilder.config().build();
	
	public static ObjectConfig emptyConfig() {
		return EMPTY_CONFIG;
	}
	
	public static ObjectConfigObjectBuilder config() {
		return new ObjectConfig();
	}
	
	public static class ObjectConfigArrayBuilder {
		private final ArrayNode m_array = JSONSerializer.createArrayNode();
		
		public ObjectConfigArrayBuilder option(int value) {
			m_array.add(value);
			return this;
		}
		
		public ObjectConfigArrayBuilder option(long value) {
			m_array.add(value);
			return this;
		}
		
		public ObjectConfigArrayBuilder option(String value) {
			m_array.add(value);
			return this;
		}

		public ObjectConfigArrayBuilder optionArray(ObjectConfigArrayBuilder array) {
			m_array.add(array.m_array);
			return this;
		}
		
		public ObjectConfigArrayBuilder optionObject(ObjectConfigObjectBuilder obj) {
			m_array.add(obj.m_obj);
			return this;
		}
	}
	
	public abstract static class ObjectConfigObjectBuilder {
		protected final ObjectNode m_obj = JSONSerializer.createNode();
		
		public ObjectConfigObjectBuilder option(String key, String value) {
			m_obj.put(key, value);
			return this;
		}
		public ObjectConfigObjectBuilder option(String key, boolean value) {
			m_obj.put(key, value);
			return this;
		}
		public ObjectConfigObjectBuilder option(String key, int value) {
			m_obj.put(key, value);
			return this;
		}
		public ObjectConfigObjectBuilder option(String key, long value) {
			m_obj.put(key, value);
			return this;
		}
		public ObjectConfigObjectBuilder pojoOption(String key, Object value) {
			m_obj.putPOJO(key, value);
			return this;
		}
		public ObjectConfigObjectBuilder optionArray(String key, ObjectConfigArrayBuilder array) {
			m_obj.set(key, array.m_array);
			return this;
		}
		
		public ObjectConfigObjectBuilder optionObject(String key, ObjectConfigObjectBuilder obj) {
			m_obj.set(key, obj.m_obj);
			return this;
		}
		
		public abstract ObjectConfig build();
	}

	public static class ObjectConfig extends ObjectConfigObjectBuilder {
		
		public String getString(String key, String defaultValue) {
			final JsonNode n = m_obj.get(key);
			return (n==null) ? defaultValue : n.asText(defaultValue);
		}
		
		public Object getPOJO(String key) {
			final JsonNode n = m_obj.get(key);
			if (n == null || n.isNull() || !n.isPojo()) {
				return null;
			}
			return ((POJONode)n).getPojo();
		}
		
		public boolean has(String key) {
			final JsonNode n = m_obj.get(key);
			if (n == null || n.isNull()) {
				return false;
			}
			return true;
		}
		
		public Boolean getBoolean(String key, Boolean defaultValue) {
			final JsonNode n = m_obj.get(key);
			if (n == null || n.isNull()) {
				return defaultValue;
			}
			return n.asBoolean(defaultValue);
		}
		
		public boolean getBoolean(String key) {
			final JsonNode n = m_obj.get(key);
			if (n == null || n.isNull()) {
				return false;
			}
			return n.asBoolean();
		}
		
		public Integer getInteger(String key, Integer defaultValue) {
			final JsonNode n = m_obj.get(key);
			if (n == null || n.isNull() || !n.canConvertToInt()) {
				return defaultValue;
			}
			return n.asInt();
		}
		
		public Long getLong(String key, Long defaultValue) {
			final JsonNode n = m_obj.get(key);
			if (n == null || n.isNull() || !n.canConvertToLong()) {
				return defaultValue;
			}
			return n.asLong();
		}
		
		public long getlong(String key, long defaultValue) {
			final JsonNode n = m_obj.get(key);
			if (n == null || n.isNull() || !n.canConvertToLong()) {
				return defaultValue;
			}
			return n.asLong();
		}

		@Override
		public ObjectConfig build() {
			return this;
		}
	}

	private ObjectConfigBuilder() {
	}
}
