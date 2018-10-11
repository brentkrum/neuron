package com.neuron.core;

import java.util.List;

import org.apache.logging.log4j.Level;

import com.neuron.core.TemplateStateManager.TemplateState;

import io.netty.util.concurrent.Promise;

public abstract class TemplateRef {
	private final GroupRef m_groupRef;
	private final int m_id;
	private final String m_name;
	private final int m_generationId;
	private final String m_logString;
	
	TemplateRef(GroupRef groupRef, int id, String name, int generationId) {
		m_groupRef = groupRef;
		m_id = id;
		m_name = name;
		m_generationId = generationId;
		m_logString = m_name + "(" + generationId + ")";
	}

	public final GroupRef groupRef() {
		return m_groupRef;
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
	public abstract ITemplateStateLock lockState();
	
	public interface ITemplateStateLock extends AutoCloseable, ITemplateStateLockInternal {
		ITemplateStateListenerRemoval addStateListener(TemplateState state, ITemplateStateSyncListener listener);
		ITemplateStateListenerRemoval addStateAsyncListener(TemplateState state, ITemplateStateAsyncListener listener);
		
		TemplateState currentState();
		default boolean isStateOneOf(TemplateState... states) {
			final TemplateState cur = currentState();
			for(TemplateState s : states) {
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
	
	public interface ITemplateStateListener {
	}
	public interface ITemplateStateSyncListener extends ITemplateStateListener {
		void onStateReached(boolean successful);
	}
	
	public interface ITemplateStateAsyncListener extends ITemplateStateListener {
		void onStateReached(boolean successful, Promise<Void> promise);
	}
	
	public interface ITemplateStateListenerRemoval {
		void remove();
	}
}
