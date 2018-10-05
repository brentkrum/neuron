package com.neuron.core;

import io.netty.util.concurrent.Future;

public interface INeuronApplicationSystem
{
	String systemName();

	default void init()
	{
	}

	default void startRun()
	{
	}

	default Future<Void> startShutdown()
	{
		return NeuronApplication.getTaskPool().next().newSucceededFuture((Void)null);
	}
}
