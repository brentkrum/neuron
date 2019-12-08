package com.neuron.core.socket;

import com.neuron.core.Config;
import com.neuron.core.DefaultNeuronTemplateBase;
import com.neuron.core.INeuronInitialization;
import com.neuron.core.NeuronApplication;
import com.neuron.core.NeuronRef;
import com.neuron.core.ObjectConfigBuilder.ObjectConfig;
import com.neuron.core.TemplateRef;

import io.netty.util.concurrent.Future;
import io.netty.util.internal.ObjectUtil;

public class OutboundUnixSocketNeuronTemplate extends DefaultNeuronTemplateBase
{
	public static final String Config_SocketAddress = "socketAddress";
	public static final String Config_RetryDelayMS = "retryDelay";
	private static final Long DEFAULT_RETRY_DELAY_MS = Config.getFWLong("core.OutboundUnixSocketNeuronTemplate.defaultRetryDelayMS", Long.valueOf(15000));
			
	public OutboundUnixSocketNeuronTemplate(TemplateRef ref) {
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
		String socketAddress = config.getString(Config_SocketAddress, null);
		long retryDelayMS = config.getlong(Config_RetryDelayMS, DEFAULT_RETRY_DELAY_MS);
		
		ObjectUtil.checkNotNull(socketAddress, "Neuron config item Config_SocketAddress is either missing or invalid");
		
		return new OutboundUnixSocketNeuron(ref, socketAddress, retryDelayMS);
	}

}
