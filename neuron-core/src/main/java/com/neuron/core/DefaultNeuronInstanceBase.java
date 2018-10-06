package com.neuron.core;

import com.neuron.core.ObjectConfigBuilder.ObjectConfig;

public abstract class DefaultNeuronInstanceBase implements INeuronInitialization
{
	private final NeuronRef m_instanceRef;
	
	public DefaultNeuronInstanceBase(NeuronRef instanceRef)
	{
		m_instanceRef = instanceRef;
	}

	@Override
	public NeuronRef ref()
	{
		return m_instanceRef;
	}

	@Override
	public ObjectConfig config()
	{
		return ObjectConfigBuilder.config().build();
	}

}
