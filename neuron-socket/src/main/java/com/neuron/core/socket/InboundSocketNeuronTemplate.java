package com.neuron.core.socket;

import com.neuron.core.DefaultNeuronTemplateBase;
import com.neuron.core.INeuronInitialization;
import com.neuron.core.NeuronApplication;
import com.neuron.core.NeuronRef;
import com.neuron.core.ObjectConfigBuilder.ObjectConfig;
import com.neuron.core.TemplateRef;

import io.netty.util.concurrent.Future;
import io.netty.util.internal.ObjectUtil;

public class InboundSocketNeuronTemplate extends DefaultNeuronTemplateBase
{
	public static final String Config_Port = "port";
	public static final String Config_InPipeMaxPipeMsgCount = "inPipeMaxMsgCount";
	public static final String Config_OutPipeMaxPipeMsgCount = "outPipeMaxMsgCount";
			
	public InboundSocketNeuronTemplate(TemplateRef ref) {
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
		Integer port = config.getInteger(Config_Port, null);
		ObjectUtil.checkNotNull(port, "Neuron config item Config_Port is either missing or invalid");
		return new InboundSocketNeuron(ref, port, config);
	}

}
