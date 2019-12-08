package com.neuron.core.http;

import com.neuron.core.DefaultNeuronTemplateBase;
import com.neuron.core.INeuronInitialization;
import com.neuron.core.NeuronApplication;
import com.neuron.core.NeuronRef;
import com.neuron.core.TemplateRef;
import com.neuron.core.ObjectConfigBuilder.ObjectConfig;

import io.netty.util.concurrent.Future;
import io.netty.util.internal.ObjectUtil;

public class HTTPServerNeuronTemplate extends DefaultNeuronTemplateBase
{
	public static final String Config_StaticContentPathRoot = "staticContentPathRoot";
	public static final String Config_Port = "port";
	public static final String Config_UseSSL = "useSSL";
	public static final String Config_ServerBacklog = "serverSocketBacklog";
	public static final String Config_InPipeMaxPipeMsgCount = "inPipeMaxMsgCount";
	public static final String Config_OutPipeMaxPipeMsgCount = "outPipeMaxMsgCount";
	public static final String Config_SSLProtocols = "sslProtocols";
	
	public static final String Config_KeyManagerAlgorithm = "keyManagerAlgorithm";
	public static final String Config_KeyStoreFilename = "keyStoreFilename";
	public static final String Config_KeyStorePassword = "keyStorePassword";
	public static final String Config_KeyStoreCertPassword = "keyStoreCertPassword";
	
	public static final String Config_TrustManagerAlgorithm = "trustManagerAlgorithm";
	public static final String Config_TrustStoreFilename = "trustStoreFilename";
	public static final String Config_TrustStorePassword = "trustStorePassword";
	
	public HTTPServerNeuronTemplate(TemplateRef ref) {
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
		return new HTTPServerNeuron(ref, port, config);
	}

}
