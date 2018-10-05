package com.neuron.core;

public abstract class DefaultNeuronTemplateBase implements INeuronTemplate
{
	private final TemplateRef m_ref;
	
	public DefaultNeuronTemplateBase(TemplateRef ref) {
		m_ref = ref;
	}
	
	@Override
	public final TemplateRef ref() {
		return m_ref;
	}

}
