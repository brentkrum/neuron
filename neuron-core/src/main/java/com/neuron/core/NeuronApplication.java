package com.neuron.core;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.message.ParameterizedMessage;

import com.neuron.core.NeuronRef.INeuronStateLock;
import com.neuron.core.NeuronStateManager.NeuronState;
import com.neuron.core.TemplateRef.ITemplateStateLock;
import com.neuron.core.TemplateStateManager.TemplateState;
import com.neuron.core.log4j.Log4jConsoleLogger;
import com.neuron.utility.StackTraceUtil;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ScheduledFuture;
import io.netty.util.internal.PlatformDependent;

public final class NeuronApplication {
	private static final Logger LOG = LogManager.getLogger(NeuronApplication.class);
	private static final long SHUTDOWN_QUIET_PERIOD = Config.getFWInt("core.NeuronApplication.shutdownQuietPeriod", 0);
	private static final long SHUTDOWN_MAX_WAIT = Config.getFWInt("core.NeuronApplication.shutdownMaxWait", 0);
	
	private static final ArrayList<NeuronApplicationSystemRegistrant> m_appSystems = new ArrayList<>();
	private static final PooledByteBufAllocator m_ioBufferPool = PooledByteBufAllocator.DEFAULT;
	private static final NioEventLoopGroup m_ioPool;
	private static final DefaultEventLoopGroup m_taskPool;
	
	private static boolean m_haltOnFatalExit = true;
	private static String[] m_args;
	private static volatile boolean m_terminateRun;
	private static volatile boolean m_registerDone = false;

//	private static int m_numTLSStorageSlots;
	
	static {
		m_ioPool = new NioEventLoopGroup(Config.getFWInt("core.NeuronApplication.ioPoolCount", 2));
		int taskPoolSize = Config.getFWInt("core.NeuronApplication.taskPoolCount", -1);
		if (taskPoolSize <= 0) {
			m_taskPool = new DefaultEventLoopGroup();
		} else {
			m_taskPool = new DefaultEventLoopGroup(taskPoolSize);
		}
	}
	
	static void register(INeuronApplicationSystem registrant) {
		synchronized(NeuronApplication.class) {
			if (m_registerDone) {
				PlatformDependent.throwException( new IllegalStateException("You can only register a new system in the NeuronApplication bootstrap process.  Calling run stops all registration.") );
				return;
			}
			m_appSystems.add(new NeuronApplicationSystemRegistrant(registrant));
			if (LOG.isDebugEnabled()) {
				LOG.debug("{}.register()", registrant.systemName());
			}
		}
	}
	
	static void registerCoreSystems() {
		StatusSystem.register();
		TimerSystem.register();
		NamedValueSystem.register();
		BytePipeSystem.register();
		MessagePipeSystem.register();
	}

	public static void fatalExit() {
		if (m_haltOnFatalExit) {
			Configurator.shutdown(LoggerContext.getContext(), 60, TimeUnit.SECONDS); // TODO pull this from config
			Runtime.getRuntime().halt(1);
		}
	}
	
	public static void shutdown() {
		terminate();
	}
	
	static void disableFatalExitHalt() {
		m_haltOnFatalExit = false;
	}
	
	public static String[] getStartupArgs() {
		return m_args;
	}

	public static ByteBuf allocateIOBuffer() {
		return m_ioBufferPool.buffer();
	}

	public static CompositeByteBuf allocateCompositeBuffer() {
		return m_ioBufferPool.compositeBuffer();
	}

	public static CompositeByteBuf allocateCompositeBuffer(int maxNumComponents) {
		return m_ioBufferPool.compositeBuffer(maxNumComponents);
	}

	public static NioEventLoopGroup getIOPool() {
		return m_ioPool;
	}
	
	public static MultithreadEventLoopGroup getTaskPool() {
		return m_taskPool;
	}
	
   public static ScheduledFuture<?> scheduleForCurrentTemplate(Runnable command, long delay, TimeUnit unit) {
   	NeuronSystemTLS.validateTemplateAwareThread();
   	final TemplateRef ref = NeuronSystemTLS.currentTemplate();
   	
   	ScheduledFuture<?> schedFuture = NeuronApplication.getTaskPool().schedule(() -> {
   		try(final ITemplateStateLock lock = ref.lockState()) {
				command.run();
   		}
		}, delay, unit);
   	
   	return schedFuture;
   }

   public static ScheduledFuture<?> scheduleForCurrentTemplate(Runnable command, long delay, TimeUnit unit, TemplateState... runnableStates) {
   	NeuronSystemTLS.validateTemplateAwareThread();
   	final TemplateRef ref = NeuronSystemTLS.currentTemplate();
   	
   	ScheduledFuture<?> schedFuture = NeuronApplication.getTaskPool().schedule(() -> {
   		try(final ITemplateStateLock lock = ref.lockState()) {
   			if (lock.isStateOneOf(runnableStates)) {
   				command.run();
   			}
   		}
		}, delay, unit);
   	
   	return schedFuture;
   }

   public static ScheduledFuture<?> scheduleForCurrentNeuron(Runnable command, long delay, TimeUnit unit) {
   	NeuronSystemTLS.validateNeuronAwareThread();
   	final NeuronRef ref = NeuronSystemTLS.currentNeuron();
   	
   	ScheduledFuture<?> schedFuture = NeuronApplication.getTaskPool().schedule(() -> {
   		try(final INeuronStateLock lock = ref.lockState()) {
				command.run();
   		}
		}, delay, unit);
   	
   	return schedFuture;
   }
	
   public static ScheduledFuture<?> scheduleForCurrentNeuron(Runnable command, long delay, TimeUnit unit, NeuronState... runnableStates) {
   	NeuronSystemTLS.validateNeuronAwareThread();
   	final NeuronRef ref = NeuronSystemTLS.currentNeuron();
   	
   	ScheduledFuture<?> schedFuture = NeuronApplication.getTaskPool().schedule(() -> {
   		try(final INeuronStateLock lock = ref.lockState()) {
   			if (lock.isStateOneOf(runnableStates)) {
   				command.run();
   			}
   		}
		}, delay, unit);
   	
   	return schedFuture;
   }
	
	public static final void log(Level neuronLevel, Level log4jLevel, Logger logger, String log4jFormatString, Object... arguments) {
		StringBuilder sb = _log(neuronLevel, log4jFormatString, arguments);
		logger.log(log4jLevel, sb);
	}
	public static final void log(Level neuronLevel, String log4jFormatString, Object... arguments) {
		_log(neuronLevel, log4jFormatString, arguments);
	}
	private static final StringBuilder _log(Level neuronLevel, String log4jFormatString, Object... arguments) {
		NeuronSystemTLS.validateContextAwareThread();
		final ParameterizedMessage msg = new ParameterizedMessage(log4jFormatString, arguments);
		final StringBuilder sb = new StringBuilder();
		msg.formatTo(sb);
		if (msg.getThrowable() != null) {
			sb.append('\n');
			StackTraceUtil.stackTraceFormat(sb, msg.getThrowable());
		}
		if (NeuronSystemTLS.isNeuronCurrent()) {
			NeuronSystemTLS.currentNeuron().log(neuronLevel, sb);
		} else {
			NeuronSystemTLS.currentTemplate().log(neuronLevel, sb);
		}
		return sb;
	}
	
	public static void logInfo(Logger logger, String log4jFormatString, Object... arguments) {
		log(Level.INFO, Level.INFO, logger, log4jFormatString, arguments);
	}
	public static void logInfo(Level log4jLevel, Logger logger, String log4jFormatString, Object... arguments) {
		log(Level.INFO, log4jLevel, logger, log4jFormatString, arguments);
	}
	public static void logInfo(String log4jFormatString, Object... arguments) {
		log(Level.INFO, log4jFormatString, arguments);
	}
	
	public static void logWarn(Logger logger, String log4jFormatString, Object... arguments) {
		log(Level.WARN, Level.WARN, logger, log4jFormatString, arguments);
	}
	public static void logWarn(Level log4jLevel, Logger logger, String log4jFormatString, Object... arguments) {
		log(Level.WARN, log4jLevel, logger, log4jFormatString, arguments);
	}
	public static void logWarn(String log4jFormatString, Object... arguments) {
		log(Level.WARN, log4jFormatString, arguments);
	}
	
	public static void logError(Logger logger, String log4jFormatString, Object... arguments) {
		log(Level.ERROR, Level.ERROR, logger, log4jFormatString, arguments);
	}
	public static void logError(Level log4jLevel, Logger logger, String log4jFormatString, Object... arguments) {
		log(Level.ERROR, log4jLevel, logger, log4jFormatString, arguments);
	}
	public static void logError(String log4jFormatString, Object... arguments) {
		log(Level.ERROR, log4jFormatString, arguments);
	}
	
	static void initLogging() {
		if (m_terminateRun) {
			LOG.error("Attempt to re-initialize a terminated application");
			fatalExit();
		}
		Config.setConfigLogger(new Log4jConsoleLogger());
		for(String configKey : Config.keys()) {
			if (configKey.startsWith("logger.")) {
				final String loggerName = configKey.substring(7);
				final String levelString = Config.getString(configKey, null);
				final Level level;
				try {
					level = Level.getLevel(levelString);
				} catch(Exception ex) {
					LOG.error("Could not parse logger level '{}' for '{}'", levelString, configKey, ex);
					continue;
				}
				if (level == null) {
					LOG.error("Could not parse logger level '{}' for '{}'", levelString, configKey);
					continue;
					
				} else if (loggerName.equals("root")) {
					Configurator.setRootLevel(level);
					
				} else {
					Configurator.setLevel(loggerName, level);
				}
			}
		}
	}
	
	static void init(String[] args) {
		if (m_terminateRun) {
			LOG.error("Attempt to re-initialize a terminated application");
			fatalExit();
		}
		m_args = args;
		for(NeuronApplicationSystemRegistrant r : m_appSystems) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("{}.init()", r.m_registrant.systemName());
			}
			try {
				r.m_registrant.init();
			} catch(Exception ex) {
				LOG.fatal("Exception during system initialization", ex);
				fatalExit();
				return;
			}
		}
	}

	static void run() {
		synchronized(NeuronApplication.class) {
			m_registerDone = true;
		}
		Runtime.getRuntime().addShutdownHook(new ShutdownHook());
		for(NeuronApplicationSystemRegistrant r : m_appSystems) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("{}.startRun()", r.m_registrant.systemName());
			}
			try {
				r.m_registrant.startRun();
			} catch(Exception ex) {
				LOG.fatal("Exception during system startup", ex);
				fatalExit();
				return;
			}
		}
		
	}

	static void run(Runnable executeThisThenTerminate) {
		synchronized(NeuronApplication.class) {
			m_registerDone = true;
		}
		
//		for(NeuronApplicationSystemRegistrant r : m_appSystems) {
//			m_numTLSStorageSlots += r.m_registrant.numTLSMigrationStorageSlots();
//		}
		for(NeuronApplicationSystemRegistrant r : m_appSystems) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("{}.startRun()", r.m_registrant.systemName());
			}
			try {
				r.m_registrant.startRun();
			} catch(Exception ex) {
				LOG.fatal("Exception during system startup", ex);
				terminate();
				fatalExit();
				return;
			}
		}
		try {
			executeThisThenTerminate.run();
		} catch(Exception ex) {
			LOG.error("Unhandled exception in user code", ex);
		}
		terminate();
	}
	
	static void terminate() {
		synchronized(NeuronApplication.class) {
			if (m_terminateRun) {
				return;
			}
			m_terminateRun = true;
		}

		LOG.info("Start of shutdown hook");
		
		final int numAppSystems = m_appSystems.size();
		for(int i=numAppSystems-1; i>=0; i--) {
			final NeuronApplicationSystemRegistrant r = m_appSystems.get(i);
			if (LOG.isDebugEnabled()) {
				LOG.debug("Shutting down {}", r.m_registrant.systemName());
			}
			try {
				r.startShutdown();
			} catch(Exception ex) {
				LOG.error("Exception during system startup", ex);
			}
			if (!r.waitForSuccessfulShutdown()) {
				LOG.error("Unsuccessful shutdown of registrant {}", r.m_registrant.toString());
			}
		}
		
		// Shut down thread pools
		LOG.debug("Waiting for io pool shutdown");
		m_ioPool.shutdownGracefully(SHUTDOWN_QUIET_PERIOD, SHUTDOWN_MAX_WAIT, TimeUnit.MILLISECONDS).awaitUninterruptibly();
		LOG.debug("Waiting for task pool shutdown");
		m_taskPool.shutdownGracefully(SHUTDOWN_QUIET_PERIOD, SHUTDOWN_MAX_WAIT, TimeUnit.MILLISECONDS).awaitUninterruptibly();
		LOG.info("End of shutdown hook");
		Configurator.shutdown(LoggerContext.getContext(), 60, TimeUnit.SECONDS); // TODO pull this from config
	}

	private static class ShutdownHook extends Thread {

		@Override
		public void run() {
			terminate();
		}
		
	}
	
	private static class NeuronApplicationSystemRegistrant {
		private final INeuronApplicationSystem m_registrant;
		private Future<Void> m_shutdownFuture;
		
		private NeuronApplicationSystemRegistrant(INeuronApplicationSystem registrant) {
			m_registrant = registrant;
		}
		
		void startShutdown() {
			m_shutdownFuture = m_registrant.startShutdown();
		}
		
		boolean waitForSuccessfulShutdown() {
			if (m_shutdownFuture == null) {
				return true;
			}
			m_shutdownFuture.syncUninterruptibly();
			return m_shutdownFuture.isSuccess();
		}
	}
}
