package com.neuron.core.http;

import com.neuron.core.DefaultNeuronTemplateBase;
import com.neuron.core.INeuronInitialization;
import com.neuron.core.NeuronApplication;
import com.neuron.core.NeuronRef;
import com.neuron.core.ObjectConfigBuilder.ObjectConfig;
import com.neuron.core.TemplateRef;

import io.netty.util.concurrent.Future;

public class HTTPClientNeuronTemplate extends DefaultNeuronTemplateBase
{
			
	public HTTPClientNeuronTemplate(TemplateRef ref) {
		super(ref);
	}

	@Override
	public Future<Void> runSelftest()
	{
		return NeuronApplication.getTaskPool().next().newSucceededFuture(null);
	}

	@Override
	public boolean isSingleInstance()
	{
		return false;
	}

	@Override
	public INeuronInitialization createNeuron(NeuronRef ref, ObjectConfig config)
	{
		return new HTTPClientNeuron(ref);
	}

}
