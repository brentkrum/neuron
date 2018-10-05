package com.neuron.core.socket;

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
		
		ObjectUtil.checkNotNull(inetHost, "Need to add Config_InetHost to the neuron config");
		ObjectUtil.checkNotNull(port, "Need to add Config_Port to the neuron config");
		
		return new OutboundSocketNeuron(ref, inetHost, port);
	}

}
