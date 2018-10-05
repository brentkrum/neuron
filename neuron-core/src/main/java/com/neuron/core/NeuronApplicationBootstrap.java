package com.neuron.core;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.plugins.util.PluginManager;

import io.netty.util.internal.PlatformDependent;

public abstract class NeuronApplicationBootstrap
{

	public static NeuronApplicationBuilder_Setup bootstrapUnitTest(String args[])
	{
		Config.keys(); // Force Config to load
		PluginManager.addPackage("com.neuron.core");
		PluginManager.addPackage("com.neuron.core.log4j");
        // Config.overrideValue("unit-test", OperationBootstrap.HOSTNAME_FOR_OPERATIONS, "UNITTEST");
		NeuronApplication.initLogging();
		NeuronApplication.registerCoreSystems();
		return bootstrap(args);
	}
	public static NeuronApplicationBuilder_Setup bootstrapUnitTest(String applicationLog4jXMLFilename, String args[])
	{
		Config.keys(); // Force Config to load
		PluginManager.addPackage("com.neuron.core");
		PluginManager.addPackage("com.neuron.core.log4j");
		if (applicationLog4jXMLFilename.equals("neuron-log4j2.xml")) {
			PlatformDependent.throwException(new IllegalArgumentException("Your application log4j2 XML filename cannot be the same as the one defined by the framework.  Use something like 'log4j2-myapp.xml'"));
		}
		System.setProperty("log4j.configurationFile", "neuron-log4j2.xml," + applicationLog4jXMLFilename);
        // Config.overrideValue("unit-test", OperationBootstrap.HOSTNAME_FOR_OPERATIONS, "UNITTEST");
		NeuronApplication.initLogging();
		NeuronApplication.registerCoreSystems();
		return new NeuronApplicationBuilder_Setup(args);
	}

	public static NeuronApplicationBuilder_Setup bootstrap(String args[])
	{
		Config.keys(); // Force Config to load
		PluginManager.addPackage("com.neuron.core");
		PluginManager.addPackage("com.neuron.core.log4j");
		NeuronApplication.initLogging();
		NeuronApplication.registerCoreSystems();
		return new NeuronApplicationBuilder_Setup(args);
	}
	public static NeuronApplicationBuilder_Setup bootstrap(String applicationLog4jXMLFilename, String args[])
	{
		Config.keys(); // Force Config to load
		PluginManager.addPackage("com.neuron.core");
		PluginManager.addPackage("com.neuron.core.log4j");
		if (applicationLog4jXMLFilename.equals("neuron-log4j2.xml")) {
			PlatformDependent.throwException( new IllegalArgumentException("Your application log4j2 XML filename cannot be the same as the one defined by the framework.  Use something like 'log4j2-myapp.xml'") );
		}
		System.setProperty("log4j.configurationFile", "neuron-log4j2.xml," + applicationLog4jXMLFilename);
		NeuronApplication.initLogging();
		NeuronApplication.registerCoreSystems();
		return new NeuronApplicationBuilder_Setup(args);
	}

	
	public static class NeuronApplicationBuilder_Setup {
		private static final Logger LOG = LogManager.getLogger(NeuronApplication.class);
		
		private NeuronApplicationBuilder_Setup(String[] args) {
			final String ver = NeuronApplication.class.getPackage().getImplementationVersion();
			if (ver != null) {
				LOG.info("Starting ({})", ver);
			} else {
				LOG.info("Starting");
			}
			NeuronApplication.init(args);
		}
		
		public NeuronApplicationBuilder_Setup overrideConfigValue(String key, String value) {
			Config.overrideValue("service-bus-application-builder", key, value);
			return this;
		}
		
		public NeuronApplicationBuilder_Setup registerSystem(INeuronApplicationSystem registrant) {
			NeuronApplication.register(registrant);
			try {
				if (LOG.isDebugEnabled()) {
					LOG.debug("{}.init()", registrant.systemName());
				}
				registrant.init();
			} catch(Exception ex) {
				LogManager.getLogger(NeuronApplication.class).fatal("Exception during system initialization", ex);
				NeuronApplication.fatalExit();
				return this;
			}
			return this;
		}
		
        // public NeuronApplicationBuilder_Setup addService(InlineServiceConfiguration config) {
        //     final IService service = config.build();
        //     NeuronApplication.getServiceBus().addService(service);
        //     return this;
        // }
        //
        // public NeuronApplicationBuilder_Setup createHandlerLimiter(String name, int simultaneous, int backlog) {
        //     NeuronApplication.getServiceBus().createHandlerLimiter(name, simultaneous, backlog);
        //     return this;
        // }
        // public NeuronApplicationBuilder_Setup createHandlerLimiterChain(String chainLimiterName, String... limiterNames) {
        //     NeuronApplication.getServiceBus().createHandlerLimiterChain(chainLimiterName, limiterNames);
        //     return this;
        // }
        // public NeuronApplicationBuilder_Setup createSubmitLimiter(String name, int simultaneous, int backlog) {
        //     NeuronApplication.getServiceBus().createSubmitLimiter(name, simultaneous, backlog);
        //     return this;
        // }
        // public NeuronApplicationBuilder_Setup createSubmitLimiterChain(String chainLimiterName, String... limiterNames) {
        //     NeuronApplication.getServiceBus().createSubmitLimiterChain(chainLimiterName, limiterNames);
        //     return this;
        // }
        //
        // public NeuronApplicationBuilder_Setup addService(IService service) {
        //     NeuronApplication.getServiceBus().addService(service);
        //     return this;
        // }
        //
        // public <RequestT> NeuronApplicationBuilder_Setup addMessageHandler(String messageId, IMessageHandler<RequestT> handler, Class<RequestT> requestClass) {
        //     NeuronApplication.getServiceBus().addMessageHandler(messageId, handler, requestClass);
        //     return this;
        // }
        // public <RequestT> NeuronApplicationBuilder_Setup addMessageHandler(String messageId, IMessageHandler<RequestT> handler, Class<RequestT> requestClass, String handlerLimiterName) {
        //     NeuronApplication.getServiceBus().addMessageHandler(messageId, handler, requestClass, handlerLimiterName);
        //     return this;
        // }
        // public <RequestT,AdapedRequestT,ResponseT> NeuronApplicationBuilder_Setup addMessageHandler(String messageId, IMessageHandler<AdapedRequestT> handler, Class<RequestT> requestClass, IMessageResponseCallbackAdaptor<?,ResponseT> callbackAdaptor, IServiceRequestMessageAdaptor<RequestT,AdapedRequestT> requestAdaptor, IServiceResponseMessageAdaptor<?,ResponseT> responseAdaptor, String handlerLimiterName) {
        //     NeuronApplication.getServiceBus().addMessageHandler(messageId, handler, requestClass, callbackAdaptor, requestAdaptor, responseAdaptor, handlerLimiterName);
        //     return this;
        // }
		
		public void run() {
			NeuronApplication.run();
		}
		
		public void run(Runnable executeThisThenTerminate) {
			NeuronApplication.run(executeThisThenTerminate);
		}
	}
}
