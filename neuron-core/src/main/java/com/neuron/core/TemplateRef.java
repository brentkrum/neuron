package com.neuron.core;

import java.util.List;

import org.apache.logging.log4j.Level;

import com.neuron.core.TemplateStateManager.TemplateState;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

public abstract class TemplateRef {
	private final int m_id;
	private final String m_name;
	private final int m_generationId;
	private final String m_logString;
	
	TemplateRef(int id, String name, int generationId) {
		m_id = id;
		m_name = name;
		m_generationId = generationId;
		m_logString = m_name + "(" + generationId + ")";
	}

	public final int id() {
		return m_id;
	}
	
	public final String name() {
		return m_name;
	}
	
	final int generation() {
		return m_generationId;
	}
	
	public final String logString() {
		return m_logString;
	}

	/**
	 * This method is helpful for unit tests
	 * 
	 * @param testString
	 * 
	 * @return
	 */
	public final boolean logContains(String testString) {
		for(NeuronLogEntry entry : getLog()) {
			if (entry.message.contains(testString)) {
				return true;
			}
		}
		return false;
	}

	abstract List<NeuronLogEntry> getLog();
	abstract void log(Level level, StringBuilder sb);
	public abstract ITemplateStateLock lockState();
	
	public interface ITemplateStateLock extends AutoCloseable, ITemplateStateLockInternal {
		Future<TemplateRef> getStateFuture(TemplateState forState);
		void addStateListener(TemplateState state, GenericFutureListener<? extends Future<? super TemplateRef>> listener);
		
		TemplateState currentState();
		boolean isStateOneOf(TemplateState... states);
		
		boolean takeOffline();
		void unlock();
		void close();
	}
}
