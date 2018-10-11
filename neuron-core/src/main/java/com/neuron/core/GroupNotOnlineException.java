package com.neuron.core;

public class GroupNotOnlineException extends Exception {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private final GroupRef m_ref;
	
	public GroupNotOnlineException(GroupRef ref) {
		m_ref = ref;
	}
	
	public GroupRef groupRef() {
		return m_ref;
	}
}
