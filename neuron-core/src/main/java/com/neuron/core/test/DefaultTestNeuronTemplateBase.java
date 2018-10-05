package com.neuron.core.test;

import com.neuron.core.DefaultNeuronTemplateBase;
import com.neuron.core.INeuronInitialization;
import com.neuron.core.NeuronRef;
import com.neuron.core.ObjectConfigBuilder.ObjectConfig;
import com.neuron.core.TemplateRef;

public class DefaultTestNeuronTemplateBase extends DefaultNeuronTemplateBase
{
	public DefaultTestNeuronTemplateBase(TemplateRef ref)
	{
		super(ref);
	}

	@Override
	public INeuronInitialization createNeuron(NeuronRef ref, ObjectConfig config)
	{
		return null;
	}

}
