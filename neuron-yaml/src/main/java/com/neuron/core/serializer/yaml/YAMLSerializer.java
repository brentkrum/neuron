package com.neuron.core.serializer.yaml;

import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import com.neuron.core.serializer.SerializerBase;

public abstract class YAMLSerializer extends SerializerBase {
	private static final ObjectMapper m_yamlSerializer;
	
	static {
		m_yamlSerializer = new ObjectMapper(new YAMLFactory());
		m_yamlSerializer.disable(SerializationFeature.CLOSE_CLOSEABLE);
		m_yamlSerializer.disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
		m_yamlSerializer.disable(JsonParser.Feature.AUTO_CLOSE_SOURCE);
		m_yamlSerializer.setVisibility(PropertyAccessor.GETTER, Visibility.NONE);
		m_yamlSerializer.setVisibility(PropertyAccessor.SETTER, Visibility.NONE);
		m_yamlSerializer.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
		addMixins(m_yamlSerializer);
	}
	public static ObjectMapper instance() {
		return m_yamlSerializer;
	}

	public static ObjectNode createNode() {
		return m_yamlSerializer.getNodeFactory().objectNode();
	}
	public static ArrayNode createArrayNode() {
		return m_yamlSerializer.getNodeFactory().arrayNode();
	}
}
