package com.neuron.core;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.neuron.core.NeuronRef.INeuronStateLock;
import com.neuron.core.NeuronStateSystem.NeuronState;
import com.neuron.core.ObjectConfigBuilder.ObjectConfig;
import com.neuron.utility.CharSequenceTrie;
import com.neuron.utility.IntTrie;

import io.netty.util.ReferenceCounted;
import io.netty.util.internal.PlatformDependent;

public final class MessagePipeSystem
{
	private static final Logger LOG = LogManager.getLogger(MessagePipeSystem.class);
	
	public static final String pipeBrokerConfig_MaxPipeMsgCount = "maxPipeMsgCount";

	private static final int DEFAULT_PIPE_MSG_COUNT = Config.getFWInt("core.MessagePipeSystem.defaultMaxPipeMsgCount", Integer.valueOf(32));
	private static final ReadWriteLock m_brokerLock = new ReentrantReadWriteLock(true);
	private static final CharSequenceTrie<PipeBroker> m_pipeBrokerByName = new CharSequenceTrie<>();
	private static final IntTrie<InstancePipeBrokers> m_brokerByGen = new IntTrie<>();

	private MessagePipeSystem() {
	}

	static void register() {
		NeuronApplication.register(new Registrant());
	}
	
	public static void configurePipeBroker(String pipeName, ObjectConfig brokerConfig) {
		NeuronSystemTLS.validateNeuronAwareThread();
		final NeuronRef currentInstanceRef = NeuronSystemTLS.currentNeuron();
		final String fqpn = createFQPN(currentInstanceRef.name(), pipeName);
		
		try(INeuronStateLock lock = currentInstanceRef.lockState()) {
			NeuronSystemTLS.validateInNeuronConnectResources(lock);
			
			m_brokerLock.writeLock().lock();
			try {
				PipeBroker broker = m_pipeBrokerByName.get(fqpn);
				if (broker instanceof ReadPlaceholderPipeBroker) {
					CreatedPipeBroker createdBroker = new CreatedPipeBroker(fqpn, brokerConfig, currentInstanceRef, (ReadPlaceholderPipeBroker)broker);
					m_pipeBrokerByName.replace(fqpn, createdBroker);
					addOrReplaceBrokerInInstance0(((ReadPlaceholderPipeBroker)broker).m_reader.owner(), createdBroker);
					if (LOG.isDebugEnabled()) {
						LOG.debug("Created pipe broker {} and attached existing reader {}", fqpn, ((ReadPlaceholderPipeBroker)broker).m_reader.owner().logString());
					}
					broker = createdBroker;
					
				} else if (broker instanceof WritePlaceholderPipeBroker) {
					CreatedPipeBroker createdBroker = new CreatedPipeBroker(fqpn, brokerConfig, currentInstanceRef, (WritePlaceholderPipeBroker)broker);
					m_pipeBrokerByName.replace(fqpn, createdBroker);
					addOrReplaceBrokerInInstance0(((WritePlaceholderPipeBroker)broker).m_writeContext.owner(), createdBroker);
					if (LOG.isDebugEnabled()) {
						LOG.debug("Created pipe broker {} and attached existing writer {}", fqpn, ((WritePlaceholderPipeBroker)broker).m_writeContext.owner().logString());
					}
					broker = createdBroker;
					
				} else if (broker == null) {
					CreatedPipeBroker createdBroker = new CreatedPipeBroker(fqpn, brokerConfig, currentInstanceRef);
					m_pipeBrokerByName.addOrFetch(fqpn, createdBroker);
					broker = createdBroker;
					if (LOG.isDebugEnabled()) {
						LOG.debug("Created pipe broker {}", fqpn);
					}
					
				} else {
					PlatformDependent.throwException(new PipeAlreadyCreatedException(fqpn));
				}
				addOrReplaceBrokerInInstance0(currentInstanceRef, broker);
	
			} finally {
				m_brokerLock.writeLock().unlock();
			}
		}
	}

	public static void readFromPipe(String pipeName, ObjectConfig readConfig, IMessagePipeReaderCallback callback) {
		NeuronSystemTLS.validateNeuronAwareThread();
		final NeuronRef currentNeuronRef = NeuronSystemTLS.currentNeuron();
		try(INeuronStateLock lock = currentNeuronRef.lockState()) {
			NeuronSystemTLS.validateInNeuronConnectResources(lock);
			_readFromPipe(currentNeuronRef, currentNeuronRef.name(), pipeName, readConfig, callback);
		}
	}
	
	public static void readFromPipe(String declaringNeuronInstanceName, String pipeName, ObjectConfig readConfig, IMessagePipeReaderCallback callback) {
		NeuronSystemTLS.validateNeuronAwareThread();
		final NeuronRef currentNeuronRef = NeuronSystemTLS.currentNeuron();
		try(INeuronStateLock lock = currentNeuronRef.lockState()) {
			NeuronSystemTLS.validateInNeuronConnectResources(lock);
			_readFromPipe(currentNeuronRef, declaringNeuronInstanceName, pipeName, readConfig, callback);
		}
	}
	
	public static void _readFromPipe(final NeuronRef currentNeuronRef, String declaringNeuronInstanceName, String pipeName, ObjectConfig readConfig, IMessagePipeReaderCallback callback) {
		final String fqpn = createFQPN(declaringNeuronInstanceName, pipeName);
		
		m_brokerLock.writeLock().lock();
		try {
			PipeBroker broker = m_pipeBrokerByName.get(fqpn);
			if (broker instanceof CreatedPipeBroker) {
				((CreatedPipeBroker)broker).validateAndSetNewReader( new MessagePipeReader(currentNeuronRef, broker, readConfig, callback) );
				if (LOG.isDebugEnabled()) {
					LOG.debug("Adding reader to existing pipe broker {}", fqpn);
				}

			} else if (broker == null) {
				final ReadPlaceholderPipeBroker placeholderBroker = new ReadPlaceholderPipeBroker(fqpn, new MessagePipeReader(currentNeuronRef, null, readConfig, callback));
				m_pipeBrokerByName.addOrFetch(fqpn, placeholderBroker);
				broker = placeholderBroker;
				if (LOG.isDebugEnabled()) {
					LOG.debug("Creating ReadPlaceholderPipeBroker for pipe {}", fqpn);
				}
				
			} else if (broker instanceof ReadPlaceholderPipeBroker) {
				final UnsupportedOperationException ex = new UnsupportedOperationException();
				NeuronApplication.logError(LOG, "Attempted to add a second reader for pipe '{}' the other reader was added by neuron {}. Pipes must have only one reader and one writer.", pipeName, ((ReadPlaceholderPipeBroker)broker).m_reader.owner().logString(), ex);
				PlatformDependent.throwException(ex);
				return;
				
			} else { // if (broker instanceof WritePlaceholderPipeBroker) {
				final UnsupportedOperationException ex = new UnsupportedOperationException();
				NeuronApplication.logError(LOG, "Attempted to add a reader for pipe '{}' with neuron {} connected as writer, but it was not created by either one.  Either the writer or the reader need to own the pipe by calling configurePipeBroker", pipeName, ((WritePlaceholderPipeBroker)broker).m_writeContext.owner().logString(), ex);
				PlatformDependent.throwException(ex);
				return;

			}
			addOrReplaceBrokerInInstance0(currentNeuronRef, broker);
		} finally {
			m_brokerLock.writeLock().unlock();
		}
		
	}
		
	public static IPipeWriterContext writeToPipe(String pipeName, ObjectConfig writeConfig, IMessagePipeWriterListener listener) {
		NeuronSystemTLS.validateNeuronAwareThread();
		final NeuronRef currentInstanceRef = NeuronSystemTLS.currentNeuron();
		try(INeuronStateLock lock = currentInstanceRef.lockState()) {
			NeuronSystemTLS.validateInNeuronConnectResources(lock);
			return _writeToPipe(currentInstanceRef, currentInstanceRef.name(), pipeName, writeConfig, listener);
		}
	}
	public static IPipeWriterContext writeToPipe(String declaringNeuronInstanceName, String pipeName, ObjectConfig writeConfig, IMessagePipeWriterListener listener) {
		NeuronSystemTLS.validateNeuronAwareThread();
		final NeuronRef currentInstanceRef = NeuronSystemTLS.currentNeuron();
		try(INeuronStateLock lock = currentInstanceRef.lockState()) {
			NeuronSystemTLS.validateInNeuronConnectResources(lock);
			return _writeToPipe(currentInstanceRef, declaringNeuronInstanceName, pipeName, writeConfig, listener);
		}
	}
	private static IPipeWriterContext _writeToPipe(final NeuronRef currentNeuronRef, String declaringNeuronInstanceName, String pipeName, ObjectConfig writeConfig, IMessagePipeWriterListener listener) {
		final String fqpn = createFQPN(declaringNeuronInstanceName, pipeName);
		
		final MessagePipeWriterContext writerContext;
		m_brokerLock.writeLock().lock();
		try {
			PipeBroker broker = m_pipeBrokerByName.get(fqpn);
			if (broker instanceof CreatedPipeBroker) {
				writerContext = new MessagePipeWriterContext(fqpn, currentNeuronRef, broker, listener, writeConfig);
				((CreatedPipeBroker)broker).validateAndSetNewWriter(writerContext);
				if (LOG.isDebugEnabled()) {
					LOG.debug("Adding writer to existing pipe broker {}", fqpn);
				}
				
			} else if (broker == null) {
				writerContext = new MessagePipeWriterContext(fqpn, currentNeuronRef, broker, listener, writeConfig);
				final WritePlaceholderPipeBroker nullBroker = new WritePlaceholderPipeBroker(fqpn, writerContext );
				m_pipeBrokerByName.addOrFetch(fqpn, nullBroker);
				broker = nullBroker;
				if (LOG.isDebugEnabled()) {
					LOG.debug("Creating WritePlaceholderPipeBroker for pipe {}", fqpn);
				}
				
			} else if (broker instanceof ReadPlaceholderPipeBroker) {
				final UnsupportedOperationException ex = new UnsupportedOperationException();
				NeuronApplication.logError(LOG, "Attempted to add a writer for pipe '{}' with neuron {} connected as reader, but it was not created by either one.  Either the writer or the reader need to own the pipe by calling configurePipeBroker", pipeName, ((ReadPlaceholderPipeBroker)broker).m_reader.owner().logString(), ex);
				PlatformDependent.throwException(ex);
				return null; // Just to make Eclipse happy 
			
			} else { // if (broker instanceof WritePlaceholderPipeBroker) {
				final UnsupportedOperationException ex = new UnsupportedOperationException();
				NeuronApplication.logError(LOG, "Attempted to add a second writer for pipe '{}' the other reader was added by neuron {}. Pipes must have only one reader and one writer.", pipeName, ((WritePlaceholderPipeBroker)broker).m_writeContext.owner().logString(), ex);
				PlatformDependent.throwException(ex);
				return null; // Just to make Eclipse happy 

			}
			addOrReplaceBrokerInInstance0(currentNeuronRef, broker);
		} finally {
			m_brokerLock.writeLock().unlock();
		}
		return writerContext;
	}
	
	private static void addOrReplaceBrokerInInstance0(NeuronRef ref, PipeBroker broker) {
		// Each instance might have multiple pipes, need to track each pipe by instance
		InstancePipeBrokers instanceBrokers = m_brokerByGen.get(ref.generation());
		if (instanceBrokers == null) {
			m_brokerByGen.addOrFetch(ref.generation(), instanceBrokers = new InstancePipeBrokers(ref));
		}
		instanceBrokers.addBroker(broker.pipeName(), broker);
	}

	private static String createFQPN(String declaringNeuronInstanceName, String pipeName) {
		return declaringNeuronInstanceName + ":" + pipeName;
	}
	
	interface IMessagePipeReader {
		public enum Event { DataReady };
		NeuronRef owner();
		boolean writeThrough(ReferenceCounted buf);
		void onEvent(Event event);
		void replaceBroker(PipeBroker newBroker);
		void close();
		ObjectConfig config();
		Object callback();
	}
	
	interface IMessagePipeWriter {
		public enum Event { PipeEmpty, PipeWriteable, ReaderOnline };
		NeuronRef owner();
		void onEvent(Event event);
		void replaceBroker(PipeBroker newBroker);
		void close();
//		WritePipeType pipeType();
		ObjectConfig config();
	}
	
	static abstract class PipeBroker {
		protected final String m_pipeName;
		
		PipeBroker(String pipeName) {
			m_pipeName = pipeName;
		}
		
		final String pipeName() {
			return m_pipeName;
		}
		
		abstract NeuronRef owner();
		abstract ReferenceCounted dequeue();
		abstract boolean tryWrite(ReferenceCounted buf);
		abstract void close();
	}
	
	static final class ReadPlaceholderPipeBroker extends PipeBroker {
		private final IMessagePipeReader m_reader;
		
		private ReadPlaceholderPipeBroker(String pipeName, IMessagePipeReader reader) {
			super(pipeName);
			m_reader = reader;
			m_reader.replaceBroker(this);
		}
		
		@Override
		void close() {
			m_reader.close();
		}

		@Override
		NeuronRef owner() {
			return m_reader.owner();
		}
		
		@Override
		ReferenceCounted dequeue()
		{
			return null;
		}

		@Override
		boolean tryWrite(ReferenceCounted buf)
		{
			return false;
		}
	}
	
	static final class WritePlaceholderPipeBroker extends PipeBroker {
		private final IMessagePipeWriter m_writeContext;
		
		private WritePlaceholderPipeBroker(String pipeName, IMessagePipeWriter writeContext) {
			super(pipeName);
			m_writeContext = writeContext;
			m_writeContext.replaceBroker(this);
		}

		@Override
		NeuronRef owner() {
			return m_writeContext.owner();
		}
		
		@Override
		void close() {
			m_writeContext.close();
		}

		@Override
		ReferenceCounted dequeue()
		{
			return null;
		}

		@Override
		boolean tryWrite(ReferenceCounted buf)
		{
			return false;
		}
	}
	
	static final class CreatedPipeBroker extends PipeBroker {
		private final NeuronRef m_owner;
		private final ReferenceCounted[] m_queue;
		private IMessagePipeWriter m_writer;
		private IMessagePipeReader m_reader;
		private int m_head; // Add to Head
		private int m_tail; // Remove from tail
		private boolean m_full;
		private int m_count;
		
		private CreatedPipeBroker(String pipeName, ObjectConfig config, NeuronRef owner, WritePlaceholderPipeBroker placeholder) {
			this(pipeName, config, owner);
			m_writer = placeholder.m_writeContext;
			m_writer.replaceBroker(this);
		}
		
		private CreatedPipeBroker(String pipeName, ObjectConfig config, NeuronRef owner, ReadPlaceholderPipeBroker placeholder) {
			this(pipeName, config, owner);
			m_reader = placeholder.m_reader;
			m_reader.replaceBroker(this);
		}
		
		private CreatedPipeBroker(String pipeName, ObjectConfig config, NeuronRef owner) {
			super(pipeName);
			m_owner = owner;
			final int pipeMsgCountMax = config.getInteger(MessagePipeSystem.pipeBrokerConfig_MaxPipeMsgCount, DEFAULT_PIPE_MSG_COUNT);
			m_queue = new ReferenceCounted[pipeMsgCountMax];
		}

		@Override
		NeuronRef owner() {
			return m_owner;
		}

		synchronized void closeReaderWriter(NeuronRef ref) {
			if (m_writer.owner() == ref) {
				closeWriter();
			}
			if (m_reader.owner() == ref) {
				closeReader();
			}
		}
		
		@Override
		synchronized void close() {
			if (m_writer != null) {
				closeWriter();
			}
			if (m_reader != null) {
				closeReader();
			}
		}
		
		synchronized PipeBroker detachPlaceholder() {
			final PipeBroker placeholder;
			if (m_writer.owner() == m_owner) {
				placeholder = new ReadPlaceholderPipeBroker(pipeName(), m_reader);
				m_reader = null;
			} else {
				placeholder = new WritePlaceholderPipeBroker(pipeName(), m_writer);
				m_writer = null;
			}
			return placeholder;
		}
		
		synchronized void validateAndSetNewReader(IMessagePipeReader reader)
		{
			if (m_reader != null) {
				final UnsupportedOperationException ex = new UnsupportedOperationException();
				NeuronApplication.logError(LOG, "Attempted to add a second reader for pipe '{}' which was created by neuron {}. Pipes must have only one reader and one writer.", m_pipeName, m_owner.logString(), ex);
				PlatformDependent.throwException(ex);
				return;
			}
			m_reader = reader;
			try(INeuronStateLock lock = m_reader.owner().lockState()) {
				lock.addStateListener(NeuronState.SystemOnline, (ignoreParam) -> {
					synchronized(CreatedPipeBroker.this) {
						if (m_count > 0 && m_reader != null) {
							if (LOG.isTraceEnabled()) {
								LOG.trace("Pipe {} sending DataReady event to reader", m_pipeName);
							}
							m_reader.onEvent(IMessagePipeReader.Event.DataReady);
						}
						if (m_writer != null) {
							if (LOG.isTraceEnabled()) {
								LOG.trace("Pipe {} sending ReaderOnline event to writer", m_pipeName);
							}
							m_writer.onEvent(IMessagePipeWriter.Event.ReaderOnline);
						}
					}
				});
			}
		}
		
		synchronized void validateAndSetNewWriter(IMessagePipeWriter writer)
		{
			if (m_writer != null) {
				final UnsupportedOperationException ex = new UnsupportedOperationException();
				NeuronApplication.logError(LOG, "Attempted to add a second writer for pipe '{}' which was created by neuron {}. Pipes must have only one reader and one writer.", m_pipeName, m_owner.logString(), ex);
				PlatformDependent.throwException(ex);
				return;
			}
			m_writer = writer;
			try(INeuronStateLock lock = m_writer.owner().lockState()) {
				lock.addStateListener(NeuronState.SystemOnline, (ignoreParam) -> {
					IMessagePipeWriter.Event eventToSend = null;
					
					synchronized(CreatedPipeBroker.this) {
						if (writer != m_writer) {
							return;
						}
						if (m_count == 0) {
							eventToSend = IMessagePipeWriter.Event.PipeEmpty;
						} else if (!m_full) {
							eventToSend = IMessagePipeWriter.Event.PipeWriteable;
						}
					}
					if (eventToSend != null) {
						if (LOG.isTraceEnabled()) {
							LOG.trace("Pipe {} sending {} event to writer", m_pipeName, eventToSend);
						}
						m_writer.onEvent(eventToSend);
					}					
				});
			}
			
		}
		
		synchronized void closeReader() {
			m_reader.close();
			m_reader = null;
			// Clear the queue
			for(int i=0; i<m_count; i++) {
				m_queue[m_tail].release();
				m_queue[m_tail++] = null;
				if (m_tail == m_queue.length) {
					m_tail = 0;
				}
			}
			m_head = 0;
			m_tail = 0;
			m_full = false;
			m_count = 0;
			// TODO send an event?
		}
		
		synchronized void closeWriter() {
			m_writer.close();
			m_writer = null;
			// TODO send an event and the reader can clear/drain the queue?
		}
		
		private void enqueue0(ReferenceCounted msg) {
			m_queue[m_head++] = msg;
			if (m_head == m_queue.length) {
				m_head = 0;
			}
			m_count++;
			if (m_count == m_queue.length) {
				m_full = true;
			}
		}
		
		private ReferenceCounted dequeue0() {
			if (m_count == 0) {
				return null;
			}
			final ReferenceCounted buf = m_queue[m_tail];
			m_queue[m_tail++] = null; // Remove it so it can be garbage collected
			if (m_tail == m_queue.length) {
				m_tail = 0;
			}
			m_full = false;
			m_count--;
			return buf;
		}
		
		synchronized ReferenceCounted dequeue() {
			// The reader was removed, but due to race conditions they
			// still had a cached local copy
			if (m_reader == null) {
				return null;
			}
			final boolean wasFull = m_full;
			final ReferenceCounted msg = dequeue0();
			if (msg != null && m_writer != null) {
				if (m_count == 0) {
					if (LOG.isTraceEnabled()) {
						LOG.trace("Pipe {} dequeue() sending PipeEmpty event to writer", m_pipeName);
					}
					// PipeEmpty event happens, well, when the pipe becomes empty
					m_writer.onEvent(IMessagePipeWriter.Event.PipeEmpty);
				} else if (wasFull && m_full != wasFull) {
					if (LOG.isTraceEnabled()) {
						LOG.trace("Pipe {} dequeue() sending PipeWriteable event to writer", m_pipeName);
					}
					// PipeWriteable event happens when the pipe was full and no longer is
					m_writer.onEvent(IMessagePipeWriter.Event.PipeWriteable);
				}
			}
			return msg;
		}
		
		synchronized boolean tryWrite(ReferenceCounted msg) {
			if (msg == null) {
				throw new IllegalArgumentException("msg cannot be null");
			}
			// The writer was removed, but due to race conditions they
			// still had a cached local copy
			if (m_writer == null) {
				return false;
			}
			if (m_full) {
				return false;
			}
			if (m_reader == null) {
				return false;
			}
			if (m_count == 0) {
				if (!m_reader.writeThrough(msg)) {
					enqueue0(msg);
				}
				if (LOG.isTraceEnabled()) {
					LOG.trace("Pipe {} tryWrite() sending DataReady event to reader", m_pipeName);
				}
				m_reader.onEvent(IMessagePipeReader.Event.DataReady);
			} else {
				enqueue0(msg);
			}
			return true;
		}
		
	}

	private static final class InstancePipeBrokers {
		private final CharSequenceTrie<PipeBroker> m_pipeBrokerByName = new CharSequenceTrie<>();
		private final NeuronRef m_neuronRef;
		
		InstancePipeBrokers(NeuronRef neuronRef) {
			m_neuronRef = neuronRef;
			
			// Set up a listener for when this neuron enters the state of Disconnecting
			// We will then remove this neuron from all pipes
			try(INeuronStateLock lock = m_neuronRef.lockState()) {
				lock.addStateListener(NeuronState.Disconnecting, (ignoreThis) -> {
					if (LOG.isDebugEnabled()) {
						LOG.debug("Neuron disconnecting");
					}
					onNeuronDisconnecting();
				});
			}
		}
		
		void onNeuronDisconnecting() {
			m_brokerLock.writeLock().lock();
			try {
				m_pipeBrokerByName.forEach((fqpn, broker) -> {
					if (broker instanceof CreatedPipeBroker) {
						final CreatedPipeBroker cpb = (CreatedPipeBroker)broker;
						if (cpb.m_owner == m_neuronRef) {
							if (cpb.m_reader == null || cpb.m_writer == null) {
								if (LOG.isDebugEnabled()) {
									LOG.debug("Pipe broker {} only has owner attached, closing broker", fqpn);
								}
								// The non-owner reader or writer is gone, we can just remove it
								MessagePipeSystem.m_pipeBrokerByName.remove(fqpn);
								cpb.close();
								
							} else {
								// This neuron owned the pipe, we need to replace it with a placeholder
								final PipeBroker placeholderBroker = cpb.detachPlaceholder();
								if (LOG.isDebugEnabled()) {
									LOG.debug("Detaching {} placeholder for neuron {} from non-owned pipe broker {}", placeholderBroker.getClass().getSimpleName(), placeholderBroker.owner().logString(), fqpn);
								}
								addOrReplaceBrokerInInstance0(placeholderBroker.owner(), placeholderBroker);
								MessagePipeSystem.m_pipeBrokerByName.replace(fqpn, placeholderBroker);
								cpb.close();
							}
						} else {
							// This neuron did not own the pipe, remove the reader/writer from it
							cpb.closeReaderWriter(m_neuronRef);
							if (LOG.isDebugEnabled()) {
								LOG.debug("Closing a reader/writer for existing pipe broker {}", fqpn);
							}
						}
	
					} else { // This is a placeholder
						if (LOG.isDebugEnabled()) {
							LOG.debug("Removing {} placeholder for pipe {}", broker.getClass().getSimpleName(), fqpn);
						}
						MessagePipeSystem.m_pipeBrokerByName.remove(fqpn);
						((PipeBroker)broker).close();
	
					}
					return true;
				});
			} finally {
				m_brokerLock.writeLock().unlock();
			}
		}
		
		void addBroker(String pipeName, PipeBroker broker) {
			m_pipeBrokerByName.addOrReplace(pipeName, broker);
		}
	}
	
	public interface IPipeWriterContext {
		String name();
		/**
		 * This just takes the passed in buffer and submits it to the pipe broker.
		 * If the broker queue is full, it will return false
		 * 
		 * @param buf
		 * @return
		 */
		boolean offer(ReferenceCounted msg);
		
	}
	
	private static class Registrant implements INeuronApplicationSystem {

		@Override
		public String systemName()
		{
			return "MessagePipeSystem";
		}
		
	}
}
