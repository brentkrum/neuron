package com.neuron.core;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.neuron.core.MessagePipeSystem.IMessagePipeWriter;
import com.neuron.core.MessagePipeSystem.IPipeWriterContext;
import com.neuron.core.MessagePipeSystem.PipeBroker;
import com.neuron.core.NeuronRef.INeuronStateLock;
import com.neuron.core.NeuronStateManager.NeuronState;
import com.neuron.core.ObjectConfigBuilder.ObjectConfig;

import io.netty.util.ReferenceCounted;

class MessagePipeWriterContext implements IPipeWriterContext, IMessagePipeWriter {
	private static final Logger LOG = LogManager.getLogger(MessagePipeWriterContext.class);
	
	private final String m_pipeName;
	private final IMessagePipeWriterListener m_listener;
	private final EventWorker m_worker;
	private final NeuronRef m_owner;
	private final ObjectConfig m_config;
	private volatile PipeBroker m_broker;
	private boolean m_pipeEmptyEvent;
	private boolean m_pipeWriteableEvent;
	private boolean m_pipeReaderOnline;
	
	MessagePipeWriterContext(String pipeName, NeuronRef ref, PipeBroker broker, IMessagePipeWriterListener listener, ObjectConfig config) {
		m_pipeName = pipeName;
		m_owner = ref;
		m_broker = broker;
		m_worker = new EventWorker(ref);
		m_listener = listener;
		m_config = config;
	}
	
	@Override
	public synchronized void close() {
		m_broker = null;
	}
	
	
	@Override
	public synchronized void replaceBroker(PipeBroker newBroker) {
		m_broker = newBroker;
	}
//
//	@Override
//	public WritePipeType pipeType() {
//		return m_pipeType;
//	}

	@Override
	public ObjectConfig config() {
		return m_config;
	}

	@Override
	public NeuronRef owner() {
		return m_owner;
	}


	@Override
	public String name() {
		return m_pipeName;
	}

	@Override
	public boolean offer(ReferenceCounted msg) {
		final PipeBroker broker = m_broker;
		if (broker == null) {
			// The broker has closed this writer, so pipe is "full"
			return false;
		}
		return broker.tryWrite(msg);
	}

	@Override
	public void onEvent(IMessagePipeWriter.Event event) {
		synchronized(this) {
			if (event == IMessagePipeWriter.Event.PipeEmpty) {
				m_pipeEmptyEvent = true;
			} else if (event == IMessagePipeWriter.Event.PipeWriteable) {
				m_pipeWriteableEvent = true;
			} else { // if (event == IBytePipeWriter.Event.ReaderConnected) {
				m_pipeReaderOnline = true;
			}
		}
		m_worker.requestMoreWork();
	}
	
	private class EventWorker extends PerpetualWorkContextAware {

		public EventWorker(NeuronRef ref) {
			super(ref);
		}

		@Override
		protected void _lockException(Exception ex) {
			LOG.fatal("Unexpected locking exception", ex);
		}

		@Override
		protected void _doWork(INeuronStateLock neuronLock)
		{
			// We only take data from Online and GoingOffline neurons
			if (!neuronLock.isStateOneOf(NeuronState.SystemOnline, NeuronState.Online, NeuronState.GoingOffline)) {
				return;
			}
			
			try {
				if (m_broker == null) {
					return;
				}
				final boolean pipeEmpty;
				final boolean pipeWriteable;
				final boolean pipeReaderOnline;
				synchronized(MessagePipeWriterContext.this) {
					pipeEmpty = m_pipeEmptyEvent;
					pipeWriteable = m_pipeWriteableEvent;
					pipeReaderOnline = m_pipeReaderOnline;
					m_pipeEmptyEvent = false;
					m_pipeWriteableEvent = false;
					m_pipeReaderOnline = false;
				}
				if (m_listener != null) {
					if (pipeWriteable) {
						try {
							m_listener.onEvent(IMessagePipeWriterListener.Event.PipeWriteable, MessagePipeWriterContext.this);
						} catch(Throwable t) {
							NeuronApplication.logError(LOG, "Uhandled exception in user provided listener", t);
						}
					}
					if (pipeEmpty) {
						try {
							m_listener.onEvent(IMessagePipeWriterListener.Event.PipeEmpty, MessagePipeWriterContext.this);
						} catch(Throwable t) {
							NeuronApplication.logError(LOG, "Uhandled exception in user provided listener", t);
						}
					}
					if (pipeReaderOnline) {
						try {
							m_listener.onEvent(IMessagePipeWriterListener.Event.ReaderOnline, MessagePipeWriterContext.this);
						} catch(Throwable t) {
							NeuronApplication.logError(LOG, "Uhandled exception in user provided listener", t);
						}
					}
				}
				
			} catch(Exception ex) {
				LOG.error("Unhandled exception in reader", ex);
			}
		}
		
	}
}
