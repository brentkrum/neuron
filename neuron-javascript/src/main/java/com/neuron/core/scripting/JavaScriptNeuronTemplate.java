package com.neuron.core.scripting;

import org.mozilla.javascript.Context;

import com.neuron.core.DefaultNeuronTemplateBase;
import com.neuron.core.INeuronInitialization;
import com.neuron.core.NeuronApplication;
import com.neuron.core.NeuronRef;
import com.neuron.core.ObjectConfigBuilder.ObjectConfig;
import com.neuron.core.TemplateRef;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;

public class JavaScriptNeuronTemplate extends DefaultNeuronTemplateBase
{
			
	public JavaScriptNeuronTemplate(TemplateRef ref) {
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
	public void init(Promise<Void> initDone) {
		Context cx = Context.enter();
		try {
			
		} finally {
			Context.exit();
		}
	}

	@Override
	public INeuronInitialization createNeuron(NeuronRef ref, ObjectConfig config)
	{
		return null;
	}

}
