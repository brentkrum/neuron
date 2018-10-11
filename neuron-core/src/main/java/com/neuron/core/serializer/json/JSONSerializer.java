package com.neuron.core.serializer.json;

import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.neuron.core.serializer.SerializerBase;

public abstract class JSONSerializer extends SerializerBase {
	private static final ObjectMapper m_jsonSerializer;
	
	static {
		m_jsonSerializer = new ObjectMapper();
		m_jsonSerializer.disable(SerializationFeature.CLOSE_CLOSEABLE);
		m_jsonSerializer.disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
		m_jsonSerializer.disable(JsonParser.Feature.AUTO_CLOSE_SOURCE);
		m_jsonSerializer.setVisibility(PropertyAccessor.GETTER, Visibility.NONE);
		m_jsonSerializer.setVisibility(PropertyAccessor.SETTER, Visibility.NONE);
		m_jsonSerializer.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
		addMixins(m_jsonSerializer);
	}
	public static ObjectMapper instance() {
		return m_jsonSerializer;
	}

	public static ObjectNode createNode() {
		return m_jsonSerializer.getNodeFactory().objectNode();
	}
	public static ArrayNode createArrayNode() {
		return m_jsonSerializer.getNodeFactory().arrayNode();
	}
}
