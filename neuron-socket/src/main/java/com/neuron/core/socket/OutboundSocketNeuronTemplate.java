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

public class OutboundSocketNeuronTemplate extends DefaultNeuronTemplateBase
{
	public static final String Config_InetHost = "host";
	public static final String Config_Port = "port";
	public static final String Config_RetryDelayMS = "retryDelay";
	private static final Long DEFAULT_RETRY_DELAY_MS = Config.getFWLong("core.OutboundSocketNeuronTemplate.defaultRetryDelayMS", Long.valueOf(15000));
			
	public OutboundSocketNeuronTemplate(TemplateRef ref) {
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
		String inetHost = config.getString(Config_InetHost, null);
		Integer port = config.getInteger(Config_Port, null);
		Long retryDelayMS = config.getLong(Config_RetryDelayMS, DEFAULT_RETRY_DELAY_MS);
		
		ObjectUtil.checkNotNull(inetHost, "Neuron config item Config_InetHost is either missing or invalid");
		ObjectUtil.checkNotNull(port, "Neuron config item Config_Port is either missing or invalid");
		
		return new OutboundSocketNeuron(ref, inetHost, port, retryDelayMS);
	}

}
