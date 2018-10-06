package com.neuron.core;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.neuron.core.NeuronRef.INeuronStateLock;
import com.neuron.core.NeuronStateManager.NeuronState;
import com.neuron.core.ObjectConfigBuilder.ObjectConfig;
import com.neuron.utility.CharSequenceTrie;
import com.neuron.utility.IntTrie;

import io.netty.buffer.ByteBuf;
import io.netty.util.internal.PlatformDependent;

public final class BytePipeSystem
{
	public static final String AppendBufConfig_MaxBufAvailableBytes = "maxBufAvailableBytes";
	
	private static final Logger LOG = LogManager.getLogger(BytePipeSystem.class);
	
	public enum ReadPipeType { AppendBuf, Stream, Chunk };
//	public enum WritePipeType { ByteBuf, Stream };
	public static final String pipeBrokerConfig_MaxPipeByteSize = "maxPipeByteSize";
	public static final String pipeBrokerConfig_MaxPipeMsgCount = "maxPipeMsgCount";

	private static final ReadWriteLock m_brokerLock = new ReentrantReadWriteLock(true);
	private static final CharSequenceTrie<PipeBroker> m_pipeBrokerByName = new CharSequenceTrie<>();
	private static final IntTrie<InstancePipeBrokers> m_brokerByGen = new IntTrie<>();

	private BytePipeSystem() {
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

	public static void readFromPipeAsAppendBuf(String declaringNeuronInstanceName, String pipeName, ObjectConfig readConfig, IBytePipeBufReaderCallback callback) {
		NeuronSystemTLS.validateNeuronAwareThread();
		final NeuronRef currentInstanceRef = NeuronSystemTLS.currentNeuron();
		try(INeuronStateLock lock = currentInstanceRef.lockState()) {
			NeuronSystemTLS.validateInNeuronConnectResources(lock);
			_readFromPipe(currentInstanceRef, declaringNeuronInstanceName, pipeName, ReadPipeType.AppendBuf, readConfig, callback);
		}
	}
	public static void readFromPipeAsChunk(String declaringNeuronInstanceName, String pipeName, ObjectConfig readConfig, IBytePipeBufReaderCallback callback) {
		NeuronSystemTLS.validateNeuronAwareThread();
		final NeuronRef currentInstanceRef = NeuronSystemTLS.currentNeuron();
		try(INeuronStateLock lock = currentInstanceRef.lockState()) {
			NeuronSystemTLS.validateInNeuronConnectResources(lock);
			_readFromPipe(currentInstanceRef, declaringNeuronInstanceName, pipeName, ReadPipeType.Chunk, readConfig, callback);
		}
	}
	public static void readFromPipeAsStream(String declaringNeuronInstanceName, String pipeName, ObjectConfig readConfig, IBytePipeStreamReaderCallback callback) {
		NeuronSystemTLS.validateNeuronAwareThread();
		final NeuronRef currentInstanceRef = NeuronSystemTLS.currentNeuron();
		try(INeuronStateLock lock = currentInstanceRef.lockState()) {
			NeuronSystemTLS.validateInNeuronConnectResources(lock);
			_readFromPipe(currentInstanceRef, declaringNeuronInstanceName, pipeName, ReadPipeType.Stream, readConfig, callback);
		}
	}

	public static void readFromPipeAsAppendBuf(String pipeName, ObjectConfig readConfig, IBytePipeBufReaderCallback callback) {
		NeuronSystemTLS.validateNeuronAwareThread();
		final NeuronRef currentInstanceRef = NeuronSystemTLS.currentNeuron();
		try(INeuronStateLock lock = currentInstanceRef.lockState()) {
			NeuronSystemTLS.validateInNeuronConnectResources(lock);
			_readFromPipe(currentInstanceRef, currentInstanceRef.name(), pipeName, ReadPipeType.AppendBuf, readConfig, callback);
		}
	}
	public static void readFromPipeAsChunk(String pipeName, ObjectConfig readConfig, IBytePipeBufReaderCallback callback) {
		NeuronSystemTLS.validateNeuronAwareThread();
		final NeuronRef currentInstanceRef = NeuronSystemTLS.currentNeuron();
		try(INeuronStateLock lock = currentInstanceRef.lockState()) {
			NeuronSystemTLS.validateInNeuronConnectResources(lock);
			_readFromPipe(currentInstanceRef, currentInstanceRef.name(), pipeName, ReadPipeType.Chunk, readConfig, callback);
		}
	}
	public static void readFromPipeAsStream(String pipeName, ObjectConfig readConfig, IBytePipeStreamReaderCallback callback) {
		NeuronSystemTLS.validateNeuronAwareThread();
		final NeuronRef currentInstanceRef = NeuronSystemTLS.currentNeuron();
		try(INeuronStateLock lock = currentInstanceRef.lockState()) {
			NeuronSystemTLS.validateInNeuronConnectResources(lock);
			_readFromPipe(currentInstanceRef, currentInstanceRef.name(), pipeName, ReadPipeType.Stream, readConfig, callback);
		}
	}

	private static void _readFromPipe(final NeuronRef currentNeuronRef, String declaringNeuronInstanceName, String pipeName, ReadPipeType pipeType, ObjectConfig readConfig, Object callback) {
		final String fqpn = createFQPN(declaringNeuronInstanceName, pipeName);
		
		m_brokerLock.writeLock().lock();
		try {
			PipeBroker broker = m_pipeBrokerByName.get(fqpn);
			if (broker instanceof CreatedPipeBroker) {
				((CreatedPipeBroker)broker).validateAndSetNewReader( createReader(currentNeuronRef, broker, pipeType, readConfig, callback) );
				if (LOG.isDebugEnabled()) {
					LOG.debug("Adding reader to existing pipe broker {}", fqpn);
				}

			} else if (broker == null) {
				final ReadPlaceholderPipeBroker placeholderBroker = new ReadPlaceholderPipeBroker(fqpn, createReader(currentNeuronRef, null, pipeType, readConfig, callback));
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
		
//	public static IPipeWriterContext writeToPipeWithBuf(String declaringNeuronInstanceName, String pipeName, ObjectConfig writeConfig, IBytePipeWriterListener listener) {
//		NeuronSystem.validateInNeuronConnectResources();
//		return _writeToPipe(declaringNeuronInstanceName, pipeName, WritePipeType.ByteBuf, writeConfig, listener);
//	}
//	public static IPipeWriterContext writeToPipeWithBuf(String pipeName, ObjectConfig writeConfig, IBytePipeWriterListener listener) {
//		NeuronSystem.validateInNeuronConnectResources();
//		return _writeToPipe(NeuronSystem.getCurrentInstanceRef().name(), pipeName, WritePipeType.ByteBuf, writeConfig, listener);
//	}
//	public static IPipeWriterContext writeToPipeWithStream(String declaringNeuronInstanceName, String pipeName, ObjectConfig writeConfig, IBytePipeWriterListener listener) {
//		NeuronSystem.validateInNeuronConnectResources();
//		return _writeToPipe(declaringNeuronInstanceName, pipeName, WritePipeType.Stream, writeConfig, listener);
//	}
//	public static IPipeWriterContext writeToPipeWithStream(String pipeName, ObjectConfig writeConfig, IBytePipeWriterListener listener) {
//		NeuronSystem.validateInNeuronConnectResources();
//		return _writeToPipe(NeuronSystem.getCurrentInstanceRef().name(), pipeName, WritePipeType.Stream, writeConfig, listener);
//	}
	public static IPipeWriterContext writeToPipe(String pipeName, ObjectConfig writeConfig, IBytePipeWriterListener listener) {
		NeuronSystemTLS.validateNeuronAwareThread();
		final NeuronRef currentInstanceRef = NeuronSystemTLS.currentNeuron();
		try(INeuronStateLock lock = currentInstanceRef.lockState()) {
			NeuronSystemTLS.validateInNeuronConnectResources(lock);
			return _writeToPipe(currentInstanceRef, currentInstanceRef.name(), pipeName, writeConfig, listener);
		}
	}
	public static IPipeWriterContext writeToPipe(String declaringNeuronInstanceName, String pipeName, ObjectConfig writeConfig, IBytePipeWriterListener listener) {
		NeuronSystemTLS.validateNeuronAwareThread();
		final NeuronRef currentInstanceRef = NeuronSystemTLS.currentNeuron();
		try(INeuronStateLock lock = currentInstanceRef.lockState()) {
			NeuronSystemTLS.validateInNeuronConnectResources(lock);
			return _writeToPipe(currentInstanceRef, declaringNeuronInstanceName, pipeName, writeConfig, listener);
		}
	}
	private static IPipeWriterContext _writeToPipe(final NeuronRef currentNeuronRef, String declaringNeuronInstanceName, String pipeName, ObjectConfig writeConfig, IBytePipeWriterListener listener) {
		final String fqpn = createFQPN(declaringNeuronInstanceName, pipeName);
		
		final BytePipeBufWriterContext writerContext;
		m_brokerLock.writeLock().lock();
		try {
			PipeBroker broker = m_pipeBrokerByName.get(fqpn);
			if (broker instanceof CreatedPipeBroker) {
				writerContext = new BytePipeBufWriterContext(currentNeuronRef, broker, listener, writeConfig);
				((CreatedPipeBroker)broker).validateAndSetNewWriter(writerContext);
				if (LOG.isDebugEnabled()) {
					LOG.debug("Adding writer to existing CreatedPipeBroker {}", fqpn);
				}
				
			} else if (broker == null) {
				writerContext = new BytePipeBufWriterContext(currentNeuronRef, broker, listener, writeConfig);
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
	
	private static IBytePipeReader createReader(NeuronRef ref, PipeBroker broker, ReadPipeType pipeType, ObjectConfig config, Object callback) {
		switch(pipeType) {
			case AppendBuf:
				return new BytePipeBufReader(ref, broker, config, (IBytePipeBufReaderCallback)callback);

			case Stream:
				return null; // TODO <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
				
			case Chunk:
				return new BytePipeChunkReader(ref, broker, config, (IBytePipeBufReaderCallback)callback);
		}

		throw new RuntimeException("This should never happen"); // Just to make Eclipse happy
	}
//	
//	private static IBytePipeWriter createWriter(NeuronRef context, PipeBroker broker, WritePipeType pipeType, ObjectConfig config, Object listener) {
//		return new BytePipeBufWriterContext(context, broker, pipeType, (IBytePipeWriterEventListener)listener, config);
//	}
	
	interface IBytePipeReader {
		public enum Event { DataReady };
		NeuronRef owner();
		boolean writeThrough(ByteBuf buf);
		void onEvent(Event event);
		void replaceBroker(PipeBroker newBroker);
		void close();
		ReadPipeType pipeType();
		ObjectConfig config();
		Object callback();
	}
	
	interface IBytePipeWriter {
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
		abstract ByteBuf dequeue();
		abstract boolean tryWrite(ByteBuf buf);
		abstract void close();
	}
	
	static final class ReadPlaceholderPipeBroker extends PipeBroker {
		private final IBytePipeReader m_reader;
		
		private ReadPlaceholderPipeBroker(String pipeName, IBytePipeReader reader) {
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
		ByteBuf dequeue()
		{
			return null;
		}

		@Override
		boolean tryWrite(ByteBuf buf)
		{
			return false;
		}
	}
	
	static final class WritePlaceholderPipeBroker extends PipeBroker {
		private final IBytePipeWriter m_writeContext;
		
		private WritePlaceholderPipeBroker(String pipeName, IBytePipeWriter writeContext) {
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
		ByteBuf dequeue()
		{
			return null;
		}

		@Override
		boolean tryWrite(ByteBuf buf)
		{
			return false;
		}
	}

//	static final class NullPipeBroker extends PipeBroker {
//		private final NeuronRef m_owner;
//		private final IBytePipeWriter m_writer;
//		private final IBytePipeReader m_reader;
//		
//		NullPipeBroker(String fqpn, IBytePipeWriter writer) {
//			super(fqpn);
//			m_owner = writer.owner();
//			m_writer = writer;
//			m_reader = null;
//		}
//		NullPipeBroker(String fqpn, IBytePipeReader reader) {
//			super(fqpn);
//			m_owner = reader.owner();
//			m_writer = null;
//			m_reader = reader;
//		}
//		
//		@Override
//		ByteBuf dequeue() {
//			return null;
//		}
//		@Override
//		boolean tryWrite(ByteBuf buf) {
//			return false;
//		}
//	}
	
	static final class CreatedPipeBroker extends PipeBroker {
		private final int m_pipeByteMax;
		private final NeuronRef m_owner;
		private final ByteBuf[] m_queue;
		private IBytePipeWriter m_writer;
		private IBytePipeReader m_reader;
		private int m_head; // Add to Head
		private int m_tail; // Remove from tail
		private boolean m_full;
		private int m_count;
		private int m_queueSize;
		
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
			m_pipeByteMax = config.getInteger(BytePipeSystem.pipeBrokerConfig_MaxPipeByteSize, 32*1024);
			final int pipeMsgCountMax = config.getInteger(BytePipeSystem.pipeBrokerConfig_MaxPipeMsgCount, 32);
			m_queue = new ByteBuf[pipeMsgCountMax];
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
		
		synchronized void validateAndSetNewReader(IBytePipeReader reader)
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
							m_reader.onEvent(IBytePipeReader.Event.DataReady);
						}
						if (m_writer != null) {
							if (LOG.isTraceEnabled()) {
								LOG.trace("Pipe {} sending ReaderOnline event to writer", m_pipeName);
							}
							m_writer.onEvent(IBytePipeWriter.Event.ReaderOnline);
						}
					}
				});
			}
		}
		
		synchronized void validateAndSetNewWriter(IBytePipeWriter writer)
		{
			if (m_writer != null) {
				final UnsupportedOperationException ex = new UnsupportedOperationException();
				NeuronApplication.logError(LOG, "Attempted to add a second writer for pipe '{}' which was created by neuron {}. Pipes must have only one reader and one writer.", m_pipeName, m_owner.logString(), ex);
				PlatformDependent.throwException(ex);
				return;
			}
			m_writer = writer;
			if (m_count == 0) {
				if (LOG.isTraceEnabled()) {
					LOG.trace("Pipe {} sending PipeEmpty event to writer", m_pipeName);
				}
				m_writer.onEvent(IBytePipeWriter.Event.PipeEmpty);
			} else if (!m_full) {
				if (LOG.isTraceEnabled()) {
					LOG.trace("Pipe {} sending PipeWriteable event to writer", m_pipeName);
				}
				m_writer.onEvent(IBytePipeWriter.Event.PipeWriteable);
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
			m_queueSize = 0;
			// TODO send an event
		}
		
		synchronized void closeWriter() {
			m_writer.close();
			m_writer = null;
			// TODO send an event and the reader can clear/drain the queue
		}
		
		private void enqueue0(ByteBuf buf) {
			m_queue[m_head++] = buf;
			if (m_head == m_queue.length) {
				m_head = 0;
			}
			if (m_head == m_tail) {
				m_full = true;
			}
			m_count++;
			m_queueSize += buf.readableBytes();
			if (m_queueSize >= m_pipeByteMax) {
				m_full = true;
			}
		}
		
		private ByteBuf dequeue0() {
			if (m_head == m_tail && !m_full) {
				return null;
			}
			final ByteBuf buf = m_queue[m_tail];
			m_queue[m_tail++] = null; // Remove it so it can be garbage collected
			if (m_tail == m_queue.length) {
				m_tail = 0;
			}
			if (m_full && m_queueSize < m_pipeByteMax) {
				m_full = false;
			}
			m_count--;
			m_queueSize -= buf.readableBytes();
			if (m_count == 0 && m_queueSize != 0) {
				m_queueSize = 0;
				if (m_writer == null) {
					LOG.error("Pipe [{}] queue size and count are not synchronized.  Most likely this is the fault of a writer submitting a buffer and then using the submitted buffer again. The errant writer is no longer attached.");
				} else {
					LOG.error("Pipe [{}] queue size and count are not synchronized.  Most likely this is the fault of a writer submitting a buffer and then using the submitted buffer again. The errant writer may or may not have been {}", m_pipeName, m_writer.owner().logString());
				}
			}
			return buf;
		}
		
		synchronized ByteBuf dequeue() {
			// The reader was removed, but due to race conditions they
			// still had a cached local copy
			if (m_reader == null) {
				return null;
			}
			final boolean wasFull = m_full;
			final ByteBuf buf = dequeue0();
			if (buf != null && m_writer != null) {
				if (m_count == 0) {
					if (LOG.isTraceEnabled()) {
						LOG.trace("Pipe {} dequeue() sending PipeEmpty event to writer", m_pipeName);
					}
					// PipeEmpty event happens, well, when the pipe becomes empty
					m_writer.onEvent(IBytePipeWriter.Event.PipeEmpty);
				} else if (wasFull && m_full != wasFull) {
					if (LOG.isTraceEnabled()) {
						LOG.trace("Pipe {} dequeue() sending PipeWriteable event to writer", m_pipeName);
					}
					// PipeWriteable event happens when the pipe was full and no longer is
					m_writer.onEvent(IBytePipeWriter.Event.PipeWriteable);
				}
			}
			return buf;
		}
		
		synchronized boolean tryWrite(ByteBuf buf) {
			// The writer was removed, but due to race conditions they
			// still had a cached local copy
			if (m_writer == null) {
				return false;
			}
			if (m_full) {
				return false;
			}
			if (m_reader == null) {
				buf.release();
				return true;
			}
			if (m_count == 0) {
				if (!m_reader.writeThrough(buf)) {
					enqueue0(buf);
				}
				if (LOG.isTraceEnabled()) {
					LOG.trace("Pipe {} tryWrite() sending DataReady event to reader", m_pipeName);
				}
				m_reader.onEvent(IBytePipeReader.Event.DataReady);
			} else {
				enqueue0(buf);
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
								BytePipeSystem.m_pipeBrokerByName.remove(fqpn);
								cpb.close();
								
							} else {
								// This neuron owned the pipe, we need to replace it with a placeholder
								final PipeBroker placeholderBroker = cpb.detachPlaceholder();
								if (LOG.isDebugEnabled()) {
									LOG.debug("Detaching {} placeholder for neuron {} from non-owned pipe broker {}", placeholderBroker.getClass().getSimpleName(), placeholderBroker.owner().logString(), fqpn);
								}
								addOrReplaceBrokerInInstance0(placeholderBroker.owner(), placeholderBroker);
								BytePipeSystem.m_pipeBrokerByName.replace(fqpn, placeholderBroker);
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
						BytePipeSystem.m_pipeBrokerByName.remove(fqpn);
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
		/**
		 * Data written to the BytePipeBufWriter returned by this method will not
		 * be written until either the tryClose or close() method is called.
		 * It is not intended to be long lived, but intended to be opened
		 * written to and closed for every "packet" or "set" of data you
		 * want to write.
		 * 
		 * If the neuron for this pipe goes offline, the pipe broker will be
		 * disconnected and bytes being written to this writer will be silently
		 * discarded.
		 * 
		 * @return
		 */
		BytePipeBufWriter openBufWriter();
		
		/**
		 * Data written to the BytePipeStreamWriter returned by this method will not
		 * be written until either the tryClose or close() method is called.
		 * It is not intended to be long lived, but intended to be opened
		 * written to and closed for every "packet" or "set" of data you
		 * want to write.
		 * 
		 * If the neuron for this pipe goes offline, the pipe broker will be
		 * disconnected and bytes being written to this writer will be silently
		 * discarded.
		 * 
		 * @return
		 */
		BytePipeStreamWriter openStreamWriter();
		
		/**
		 * This just takes the passed in buffer and submits it to the pipe broker.
		 * If the broker queue is full, it will return false
		 * 
		 * @param buf
		 * @return
		 */
		boolean offerBuf(ByteBuf buf);
		
	}
	
//	public interface IBytePipeWriterEventListener {
//		public enum Event { PipeEmpty, PipeWriteable };
//		void onEvent(Event event, IPipeWriterContext context);
//	}

	private static class Registrant implements INeuronApplicationSystem {

		@Override
		public String systemName()
		{
			return "BytePipeSystem";
		}
		
	}
}
