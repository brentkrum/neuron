package com.neuron.core;

import com.neuron.core.GroupStateManager.GroupState;

import io.netty.util.concurrent.Promise;

public abstract class GroupRef {
	private final int m_id;
	private final String m_name;
	private final int m_generationId;
	private final String m_logString;
	
	GroupRef(int id, String name, int generationId) {
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

	public abstract IGroupStateLock lockState();
	
	public interface IGroupStateLock extends AutoCloseable, IGroupStateLockInternal {
		IGroupStateListenerRemoval addStateListener(GroupState state, IGroupStateSyncListener listener);
		IGroupStateListenerRemoval addStateAsyncListener(GroupState state, IGroupStateAsyncListener listener);
		
		GroupState currentState();
		default boolean isStateOneOf(GroupState... states) {
			final GroupState cur = currentState();
			for(GroupState s : states) {
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
	
	public interface IGroupStateListener {
	}
	public interface IGroupStateSyncListener extends IGroupStateListener {
		void onStateReached(boolean successful);
	}
	
	public interface IGroupStateAsyncListener extends IGroupStateListener {
		void onStateReached(boolean successful, Promise<Void> promise);
	}
	
	public interface IGroupStateListenerRemoval {
		void remove();
	}
}
