package com.neuron.core;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.neuron.core.BytePipeSystem.IBytePipeWriter;
import com.neuron.core.BytePipeSystem.IPipeWriterContext;
import com.neuron.core.BytePipeSystem.PipeBroker;
import com.neuron.core.NeuronRef.INeuronStateLock;
import com.neuron.core.NeuronStateManager.NeuronState;
import com.neuron.core.ObjectConfigBuilder.ObjectConfig;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;

class BytePipeWriterContext implements IPipeWriterContext, IBytePipeWriter {
	private static final Logger LOG = LogManager.getLogger(BytePipeWriterContext.class);
	
	private IBytePipeWriterListener m_listener;
	private final EventWorker m_worker;
	private final NeuronRef m_owner;
//	private final WritePipeType m_pipeType;
	private final ObjectConfig m_config;
	private volatile BytePipeSystem.PipeBroker m_broker;
	private boolean m_pipeEmptyEvent;
	private boolean m_pipeWriteableEvent;
	private boolean m_pipeReaderOnline;
	
	BytePipeWriterContext(NeuronRef ref, BytePipeSystem.PipeBroker broker, IBytePipeWriterListener listener, ObjectConfig config) {
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
	public BytePipeBufWriter openBufWriter() {
		return new PipeBufWriter();
	}

	@Override
	public BytePipeStreamWriter openStreamWriter() {
		return new PipeStreamWriter();
	}

	@Override
	public NeuronRef owner() {
		return m_owner;
	}


	@Override
	public boolean offerBuf(ByteBuf buf) {
		final BytePipeSystem.PipeBroker broker = m_broker;
		if (broker == null) {
			// The broker has closed this writer, so silently let the buffer disappear
			buf.release();
			return true;
		}
		return broker.tryWrite(buf);
	}

	@Override
	public void onEvent(IBytePipeWriter.Event event) {
		synchronized(this) {
			if (event == IBytePipeWriter.Event.PipeEmpty) {
				m_pipeEmptyEvent = true;
			} else if (event == IBytePipeWriter.Event.PipeWriteable) {
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
				synchronized(BytePipeWriterContext.this) {
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
							m_listener.onEvent(IBytePipeWriterListener.Event.PipeWriteable, BytePipeWriterContext.this);
						} catch(Throwable t) {
							NeuronApplication.logError(LOG, "Uhandled exception in user provided listener", t);
						}
					}
					if (pipeEmpty) {
						try {
							m_listener.onEvent(IBytePipeWriterListener.Event.PipeEmpty, BytePipeWriterContext.this);
						} catch(Throwable t) {
							NeuronApplication.logError(LOG, "Uhandled exception in user provided listener", t);
						}
					}
					if (pipeReaderOnline) {
						try {
							m_listener.onEvent(IBytePipeWriterListener.Event.ReaderOnline, BytePipeWriterContext.this);
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
	
	private class PipeBufWriter extends BytePipeBufWriter {
		private final ByteBuf m_temp = NeuronApplication.allocateIOBuffer();

		@Override
		public ByteBuf buffer() {
			return m_temp;
		}

		@Override
		public boolean tryClose() {
			final BytePipeSystem.PipeBroker broker = m_broker;
			if (broker == null) {
				// The broker has closed this writer, so silently let the buffer disappear
				m_temp.release();
				return true;
			}
			if (m_temp.refCnt() > 1) {
				throw new IllegalStateException("The buffer supplied by this writer had its retain() method called.  This is not allowed");
			}
			return broker.tryWrite(m_temp);
		}

		@Override
		public void close() throws PipeFullException {
			if (!tryClose()) {
				m_temp.release();
				throw new PipeFullException();
			}
		}
		
	}
	
	private class PipeStreamWriter extends BytePipeStreamWriter {
		private final ByteBuf m_temp = NeuronApplication.allocateIOBuffer();

		@Override
		public void writeBoolean(boolean v) throws IOException {
			m_temp.writeBoolean(v);
		}

		@Override
		public void writeByte(int v) throws IOException {
			m_temp.writeByte(v);
		}

		@Override
		public void writeShort(int v) throws IOException {
			m_temp.writeShort(v);
		}

		@Override
		public void writeChar(int v) throws IOException {
			m_temp.writeChar(v);
		}

		@Override
		public void writeInt(int v) throws IOException {
			m_temp.writeInt(v);
		}

		@Override
		public void writeLong(long v) throws IOException {
			m_temp.writeLong(v);
		}

		@Override
		public void writeFloat(float v) throws IOException {
			m_temp.writeFloat(v);
		}

		@Override
		public void writeDouble(double v) throws IOException {
			m_temp.writeDouble(v);
		}

		@Override
		public void writeBytes(String s) throws IOException {
			m_temp.writeBytes(s.getBytes());
		}

		@Override
		public void writeChars(String s) throws IOException {
			ByteBufUtil.writeAscii(m_temp, s);
		}

		@Override
		public void writeUTF(String s) throws IOException {
			ByteBufUtil.writeUtf8(m_temp, s);
		}

		@Override
		public void write(int b) throws IOException {
			m_temp.writeByte(b);
		}

		@Override
		public void write(byte[] b) throws IOException {
			m_temp.writeBytes(b);
		}

		@Override
		public void write(byte[] b, int off, int len) throws IOException {
			m_temp.writeBytes(b, off, len);
		}

		@Override
		public void flush() throws IOException {
			// Nothing to do here
		}

		@Override
		public boolean tryClose() {
			final BytePipeSystem.PipeBroker broker = m_broker;
			if (broker == null) {
				// The broker has closed this writer, so silently let the buffer disappear
				m_temp.release();
				return true;
			}
			if (m_temp.refCnt() > 1) {
				throw new IllegalStateException("The buffer supplied by this writer had its retain() method called.  This is not allowed");
			}
			return broker.tryWrite(m_temp);
		}

		@Override
		public void close() throws PipeFullException {
			if (!tryClose()) {
				m_temp.release();
				throw new PipeFullException();
			}
		}
		
	}
}
