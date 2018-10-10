package com.neuron.core;

import com.neuron.core.ObjectConfigBuilder.ObjectConfig;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;

public interface INeuronTemplate
{
	TemplateRef ref();
	
	/*
	 * This method will only be called by a single thread at a time by the NeuronSystem.
	 * This method should only create and populate the neuron without blocking.  This
	 * method is called optimistically and there are conditions where the created neuron
	 * will simply be released.
	 * 
	 * Do not block.  Do not make any external calls, even reading files.
	 * 
	 */
	INeuronInitialization createNeuron(NeuronRef ref, ObjectConfig config);
	
	default long initTimeoutInMS() {
		return 1000;
	}
	
	default void init(Promise<Void> initDone) {
		initDone.setSuccess((Void)null);
	}
	
	default Future<Void> runSelftest()
	{
		return NeuronApplication.getTaskPool().next().newSucceededFuture((Void)null);
	}

	default boolean isSingleInstance()
	{
		return false;
	}
}
