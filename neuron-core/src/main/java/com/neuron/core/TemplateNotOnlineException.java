package com.neuron.core;

public class TemplateNotOnlineException extends Exception {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private final TemplateRef m_ref;
	
	public TemplateNotOnlineException(TemplateRef ref) {
		m_ref = ref;
	}
	
	public TemplateRef templateRef() {
		return m_ref;
	}
}
