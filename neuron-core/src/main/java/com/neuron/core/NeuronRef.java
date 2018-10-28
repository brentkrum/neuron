package com.neuron.core;

import java.util.List;

import org.apache.logging.log4j.Level;

import com.neuron.core.NeuronStateSystem.NeuronState;

import io.netty.util.concurrent.Promise;

public abstract class NeuronRef {
	private final TemplateRef m_templateRef;
	private final int m_id;
	private final String m_name;
	private final int m_generationId;
	private final String m_logString;
 
	NeuronRef(TemplateRef templateRef, int id, String name, int generationId) {
		m_templateRef = templateRef;
		m_id = id;
		m_name = name;
		m_generationId = generationId;
		m_logString = m_name + "(" + generationId + ")";
	}

	public final TemplateRef templateRef() {
		return m_templateRef;
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
	
	public abstract List<NeuronLogEntry> getLog();
	public abstract void log(Level level, StringBuilder sb);
	public abstract INeuronStateLock lockState();
	
	public interface INeuronStateLock extends AutoCloseable {
		INeuronStateListenerRemoval addStateListener(NeuronState state, INeuronStateSyncListener listener);
		INeuronStateListenerRemoval addStateAsyncListener(NeuronState state, INeuronStateAsyncListener listener);
		
		NeuronState currentState();
		default boolean isStateOneOf(NeuronState... states) {
			final NeuronState cur = currentState();
			for(NeuronState s : states) {
				if (s == cur) {
					return true;
				}
			}
			return false;
		}
		
		boolean takeOffline();
		void unlock();
		void close();
	}
	
	public interface INeuronStateListener {
	}
	public interface INeuronStateSyncListener extends INeuronStateListener {
		void onStateReached(boolean successful);
	}
	
	public interface INeuronStateAsyncListener extends INeuronStateListener {
		void onStateReached(boolean successful, Promise<Void> promise);
	}
	
	public interface INeuronStateListenerRemoval {
		void remove();
	}
}
