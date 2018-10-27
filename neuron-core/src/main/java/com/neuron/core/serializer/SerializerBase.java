package com.neuron.core.serializer;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.neuron.utility.FastLinkedList;
import com.neuron.utility.FastLinkedList.LLNode;

import io.netty.util.AbstractReferenceCounted;

import com.neuron.utility.FastLinkedMap;

public abstract class SerializerBase {
	abstract class LLNodeMixin<T> {
		@JsonIgnore
		private volatile LLNode<T> m_next;
		@JsonIgnore
		private volatile LLNode<T> m_prev;
		@JsonIgnore
		private volatile boolean m_inList;
		
	}
	
	abstract class LLMapNodeMixin<T> {
		@JsonIgnore
		private volatile LLNode<T> m_next;
		@JsonIgnore
		private volatile LLNode<T> m_prev;
		@JsonIgnore
		private volatile boolean m_inList;
		
	}
	
	abstract class AbstractReferenceCountedMixin {
		@JsonIgnore
	    private volatile int refCnt;
	}

	public static void addMixins(ObjectMapper mapper) {
		mapper.addMixIn(FastLinkedList.LLNode.class, LLNodeMixin.class);
		mapper.addMixIn(FastLinkedMap.LLMapNode.class, LLMapNodeMixin.class);
		mapper.addMixIn(AbstractReferenceCounted.class, AbstractReferenceCountedMixin.class);
	}
}
