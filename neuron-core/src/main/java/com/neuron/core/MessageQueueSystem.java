package com.neuron.core;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.neuron.core.NeuronRef.INeuronStateLock;
import com.neuron.core.NeuronStateSystem.NeuronState;
import com.neuron.core.ObjectConfigBuilder.ObjectConfig;
import com.neuron.utility.CharSequenceTrie;
import com.neuron.utility.FastLinkedList;

import io.netty.util.ReferenceCounted;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import io.netty.util.internal.PlatformDependent;

public final class MessageQueueSystem
{
	private static final Logger LOG = LogManager.getLogger(MessageQueueSystem.class);
	
	public static final String queueBrokerConfig_MaxMsgCount = "maxQueueMsgCount";

	private static final int DEFAULT_QUEUE_MSG_COUNT = Config.getFWInt("core.MessageQueueSystem.defaultMaxQueueMsgCount", Integer.valueOf(1024));
	private static final ReadWriteLock m_brokerLock = new ReentrantReadWriteLock(true);
	private static final CharSequenceTrie<CreatedQueueBroker> m_queueBrokerByName = new CharSequenceTrie<>();

	private static final AtomicInteger m_numConnectedReaders = new AtomicInteger();
	private static final AtomicInteger m_numConnectedSubmitters = new AtomicInteger();
	private static final ReadWriteLock m_shutdownLock = new ReentrantReadWriteLock(true);
	private static Promise<Void> m_shutdownPromise;
	private static boolean m_shuttingDown = false;

	private MessageQueueSystem() {
	}

	static void register() {
		NeuronApplication.register(new Registrant());
	}

	public static String createFQQN(String declaringNeuronInstanceName, String queueName) {
		return declaringNeuronInstanceName + ":" + queueName;
	}
	
	public static void defineQueue(String queueName, ObjectConfig queueConfig, IMessageQueueReaderCallback callback) {
		NeuronSystemTLS.validateNeuronAwareThread();
		final NeuronRef currentNeuronRef = NeuronSystemTLS.currentNeuron();
		final String fqqn = createFQQN(currentNeuronRef.name(), queueName);
		
		try(INeuronStateLock lock = currentNeuronRef.lockState()) {
			NeuronSystemTLS.validateInNeuronConnectResources(lock);
			
			startAddReader();
			m_brokerLock.writeLock().lock();
			try {
				CreatedQueueBroker broker = m_queueBrokerByName.get(fqqn);
				if (broker == null) {
					m_queueBrokerByName.addOrFetch(fqqn, broker = new CreatedQueueBroker(fqqn));
				}
				broker.setReader(queueConfig, new MessageQueueReader(NeuronSystemTLS.currentNeuron(), broker, queueConfig, callback));
	
			} finally {
				m_brokerLock.writeLock().unlock();
				endAddReader();
			}
		}
		
	}
	
	public static void defineQueue(String queueName, ObjectConfig queueConfig, IMessageQueueAsyncReaderCallback callback) {
		NeuronSystemTLS.validateNeuronAwareThread();
		final NeuronRef currentNeuronRef = NeuronSystemTLS.currentNeuron();
		final String fqqn = createFQQN(currentNeuronRef.name(), queueName);
		
		try(INeuronStateLock lock = currentNeuronRef.lockState()) {
			NeuronSystemTLS.validateInNeuronConnectResources(lock);
			
			startAddReader();
			m_brokerLock.writeLock().lock();
			try {
				CreatedQueueBroker broker = m_queueBrokerByName.get(fqqn);
				if (broker == null) {
					m_queueBrokerByName.addOrFetch(fqqn, broker = new CreatedQueueBroker(fqqn));
				}
				broker.setReader(queueConfig, new MessageQueueAsyncReader(NeuronSystemTLS.currentNeuron(), broker, queueConfig, callback));
	
			} finally {
				m_brokerLock.writeLock().unlock();
				endAddReader();
			}
		}
		
	}
	
	private static void startAddReader() {
		m_shutdownLock.readLock().lock();
		try {
			if (m_shuttingDown) {
				throw new SystemShutdownException();
			}
			m_numConnectedReaders.incrementAndGet();
		} finally {
			m_shutdownLock.readLock().unlock();
		}
	}
	
	private static void endAddReader() {
		readerDisconnected();
	}
	
	private static void readerDisconnected() {
		if (m_numConnectedReaders.decrementAndGet() == 0) {
			m_shutdownLock.writeLock().lock();
			try {
				if (m_shuttingDown && m_numConnectedSubmitters.get()==0) {
					m_shutdownPromise.setSuccess((Void)null);
				}
			} finally {
				m_shutdownLock.writeLock().unlock();
			}
		}
	}
	

	/**
	 * 
	 * @param declaringNeuronInstanceName - name of the neuron who owns the queue and configures it (the one who did or will call defineQueue)
	 * @param queueName - the name of the queue.
	 * @param msg - you are passing your reference into the queue, the Queue does not call retain()
	 * @param listener - callback for progress of the message.  May be null if you don't care.  The listener
	 *  is responsible for setting and checking the state of a Neuron via a state lock. 
	 * 
	 * @return true if it was added to the queue, false otherwise.  When false, the Queue did not
	 *  do anything with msg, the reference still stands and is now re-owned by the caller.
	 *  
	 */
	public static boolean submitToQueue(String declaringNeuronInstanceName, String queueName, ReferenceCounted msg, IMessageQueueSubmissionListener listener) {
		final String fqqn = createFQQN(declaringNeuronInstanceName, queueName);
		return submitToQueue(fqqn, msg, listener);
	}
	
	/**
	 * 
	 * @param fqqn - fully qualified queue name
	 * @param msg - you are passing your reference into the queue, the Queue does not call retain()
	 * @param listener - callback for progress of the message.  May be null if you don't care.  The listener
	 *  is responsible for setting and checking the state of a Neuron via a state lock. 
	 * 
	 * @return true if it was added to the queue, false otherwise.  When false, the Queue did not
	 *  do anything with msg, the reference still stands and is now re-owned by the caller.
	 *  
	 */
	public static boolean submitToQueue(String fqqn, ReferenceCounted msg, IMessageQueueSubmissionListener listener) {
		if (fqqn == null) {
			throw new IllegalArgumentException("fqqn cannot be null");
		}
		if (msg == null) {
			throw new IllegalArgumentException("msg cannot be null");
		}
		// TODO If system is shutting down, need to prevent queue creation <<<<<<----------------------------------------------------------------
		
		NeuronSystemTLS.validateNeuronAwareThread();
		startAddSubmitter();
		m_brokerLock.readLock().lock();
		try {
			CreatedQueueBroker broker = m_queueBrokerByName.get(fqqn);
			if (broker == null) {
				m_brokerLock.readLock().unlock();
				m_brokerLock.writeLock().lock();
				try {
					// Need to re-grab the read lock so we can release it after enqueue
					m_brokerLock.readLock().lock();
					// Need to re-check now that we have the write lock
					broker = m_queueBrokerByName.get(fqqn);
					if (broker == null) {
						m_queueBrokerByName.addOrReplace(fqqn, broker = new CreatedQueueBroker(fqqn));
						broker.initForWrite();
					}
				} finally {
					m_brokerLock.writeLock().unlock();
				}
			}
			return broker.tryEnqueue(msg, listener);
		} finally {
			m_brokerLock.readLock().unlock();
			endAddSubmitter();
		}
		
	}
	
	private static void startAddSubmitter() {
		m_shutdownLock.readLock().lock();
		try {
			if (m_shuttingDown) {
				throw new SystemShutdownException();
			}
			m_numConnectedSubmitters.incrementAndGet();
		} finally {
			m_shutdownLock.readLock().unlock();
		}
	}
	
	private static void endAddSubmitter() {
		if (m_numConnectedSubmitters.decrementAndGet() == 0) {
			m_shutdownLock.writeLock().lock();
			try {
				if (m_shuttingDown && m_numConnectedReaders.get()==0) {
					m_shutdownPromise.setSuccess((Void)null);
				}
			} finally {
				m_shutdownLock.writeLock().unlock();
			}
		}
	}
	
	interface IMessageReader {
		public enum Event { DataReady };
		NeuronRef owner();
		void onEvent(Event event);
		void close();
	}

	private static final class MessageWrapper extends FastLinkedList.LLNode<MessageWrapper> implements IMessageQueueSubmission {
		private static final AtomicInteger m_nextId = new AtomicInteger();
		private final int m_messageId = m_nextId.incrementAndGet();
		private final Worker m_worker = new Worker();
		private final ReferenceCounted m_msg;
		private final NeuronRef m_listenerRef;
		private final IMessageQueueSubmissionListener m_listener;
		
		// These can be called multiple times
		private boolean m_wasReceived;
		private boolean m_notifiedReceived;
		private boolean m_startedProcessing;
		private boolean m_notifiedStartedProcessing;
		
		// These will only be called once
		private boolean m_completedProcessing;
		private boolean m_notifiedCompletedProcessing;
		private boolean m_undelivered; 
		private boolean m_notifiedUndelivered; 
		
		MessageWrapper(ReferenceCounted msg, NeuronRef listenerRef, IMessageQueueSubmissionListener listener) {
			m_msg = msg;
			m_listenerRef = listenerRef;
			m_listener = listener;
		}
		
		@Override
		protected MessageWrapper getObject() {
			return this;
		}

		void reset() {
			m_wasReceived = false;
			m_notifiedReceived = false;
			m_startedProcessing = false;
			m_notifiedStartedProcessing = false;
		}
		
		// Mutually exclusive with setAsReceived()
		void setAsUndelivered() {
			m_undelivered = true;
			m_worker.requestMoreWork();
		}
		
		// Mutually exclusive with setAsUndelivered()
		@Override
		public void setAsReceived() {
			if (!m_wasReceived) {
				m_wasReceived = true;
				m_worker.requestMoreWork();
			}
		}

		@Override
		public int id() {
			return m_messageId;
		}

		@Override
		public ReferenceCounted message() {
			return m_msg;
		}

		@Override
		public void setAsStartedProcessing() {
			m_startedProcessing = true;
			m_worker.requestMoreWork();
		}

		@Override
		public void setAsProcessed() {
			m_completedProcessing = true;
			m_worker.requestMoreWork();
		}
//
//		@Override
//		public boolean waitForReceived(long timeoutInMS) {
//			return m_wasReceived.awaitUninterruptibly(timeoutInMS);
//		}
//
//		@Override
//		public boolean waitForStartedProcessing(long timeoutInMS) {
//			return m_startedProcessing.awaitUninterruptibly(timeoutInMS);
//		}
//
//		@Override
//		public boolean waitForProcessed(long timeoutInMS) {
//			return m_completedProcessing.awaitUninterruptibly(timeoutInMS);
//		}
		
		private final class Worker extends PerpetualWork {

			@Override
			protected void _doWork() {
				if (m_undelivered && !m_notifiedUndelivered) {
					m_notifiedUndelivered = true;
					if (m_listener != null) {
						try(INeuronStateLock lock = m_listenerRef.lockState()) {
							if (lock.isStateOneOf(NeuronState.SystemOnline, NeuronState.Online, NeuronState.GoingOffline)) {
								// If the listener wants to keep m_msg, it needs to call retain()
								m_listener.onUndelivered(m_msg);
							}
						} catch(Exception ex) {
							LOG.error("Unhandled exception in listener callback", ex);
						}
					}
					// The message was NOT delivered, so we still own the reference
					m_msg.release();
					return;
				}
				if (!m_wasReceived) {
					return;
				} else if (!m_notifiedReceived) {
					m_notifiedReceived = true;
					if (m_listener != null) {
						try(INeuronStateLock lock = m_listenerRef.lockState()) {
							if (lock.isStateOneOf(NeuronState.SystemOnline, NeuronState.Online, NeuronState.GoingOffline)) {
								m_listener.onReceived(m_msg);
							}
						} catch(Exception ex) {
							LOG.error("Unhandled exception in listener callback", ex);
						}
					}
					requestMoreWork();
				}
				
				if (!m_startedProcessing) {
					return;
				} else if (!m_notifiedStartedProcessing) {
					m_notifiedStartedProcessing = true;
					if (m_listener != null) {
						try(INeuronStateLock lock = m_listenerRef.lockState()) {
							if (lock.isStateOneOf(NeuronState.SystemOnline, NeuronState.Online, NeuronState.GoingOffline)) {
								m_listener.onStartProcessing(m_msg);
							}
						} catch(Exception ex) {
							LOG.error("Unhandled exception in listener callback", ex);
						}
					}
					requestMoreWork();
				}
				
				if (!m_completedProcessing) {
					return;
				} else if (!m_notifiedCompletedProcessing) {
					m_notifiedCompletedProcessing = true;
					if (m_listener != null) {
						try(INeuronStateLock lock = m_listenerRef.lockState()) {
							if (lock.isStateOneOf(NeuronState.SystemOnline, NeuronState.Online, NeuronState.GoingOffline)) {
								m_listener.onProcessed(m_msg);
							}
						} catch(Exception ex) {
							LOG.error("Unhandled exception in listener callback", ex);
						}
					}
				}
				
			}
			
		}
	}
	
	static abstract class QueueBroker {
		protected final String m_queueName;
		
		QueueBroker(String queueName) {
			m_queueName = queueName;
		}
		
		final String queueName() {
			return m_queueName;
		}
		
		abstract IMessageQueueSubmission checkout();
		abstract IMessageQueueSubmission dequeue();
		abstract boolean tryEnqueue(ReferenceCounted msg, IMessageQueueSubmissionListener listener);
	}
	
	static final class CreatedQueueBroker extends QueueBroker {
		private final FastLinkedList<MessageWrapper> m_queue = new FastLinkedList<>();
		private int m_maxMsgCount;
		private IMessageReader m_reader;
		private CheckoutWrapper m_currentCheckout;
		
		private CreatedQueueBroker(String queueName) {
			super(queueName);
		}
		
		void close() {
			m_queue.forEach(mw -> {
				mw.setAsUndelivered();
				return true;
			});
			m_queue.clear();
		}
		
		synchronized void initForWrite() {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Queue {} - writer initializing queue", m_queueName);
			}
		}
		
		synchronized void setReader(ObjectConfig config, IMessageReader reader) {
			if (m_reader != null) {
				final UnsupportedOperationException ex = new UnsupportedOperationException();
				NeuronApplication.logError(LOG, "Attempted to add a second reader for queue '{}' which was created by neuron {}. Queues can only have a single reader.", m_queueName, m_reader.owner().logString(), ex);
				PlatformDependent.throwException(ex);
				return;
			}
			m_reader = reader;
			m_numConnectedReaders.incrementAndGet();
			NeuronApplication.log(Level.INFO, Level.DEBUG, LOG, "Reader connected to queue '{}'", m_queueName);
			
			m_maxMsgCount = config.getInteger(MessageQueueSystem.queueBrokerConfig_MaxMsgCount, DEFAULT_QUEUE_MSG_COUNT);
			final IMessageReader r = m_reader;
			try(INeuronStateLock lock = m_reader.owner().lockState()) {
				lock.addStateListener(NeuronState.SystemOnline, (ignoreParam) -> {
					synchronized(CreatedQueueBroker.this) {
						if (m_queue.count() > 0) {
							if (LOG.isTraceEnabled()) {
								LOG.trace("Queue {} sending DataReady event to reader", m_queueName);
							}
							r.onEvent(IMessageReader.Event.DataReady);
						} else {
							if (LOG.isTraceEnabled()) {
								LOG.trace("Queue {} sending no events to reader, queue empty", m_queueName);
							}
						}
					}
				});
				
				lock.addStateListener(NeuronState.Disconnecting, (ignoreThis) -> {
					closeReader();
					readerDisconnected();
					NeuronApplication.log(Level.INFO, Level.DEBUG, LOG, "Disconnected from queue '{}'", m_queueName);
				});
			}
		}
		
		synchronized void closeReader() {
			if (m_currentCheckout != null) {
				m_currentCheckout.close0();
				m_currentCheckout = null;
			}
			m_reader.close();
			m_reader = null;
		}
		
		@Override
		synchronized IMessageQueueSubmission dequeue() {
			// The reader was removed, but due to race conditions they
			// still had a cached local copy
			if (m_reader == null) {
				return null;
			}
			if (m_currentCheckout != null) {
				throw new IllegalStateException("This should not happen");
			}
			final MessageWrapper msg = m_queue.removeFirst();
			if (msg != null) {
				msg.setAsReceived();
			}
			return msg;
		}
		
		@Override
		synchronized IMessageQueueSubmission checkout() {
			// The reader was removed, but due to race conditions they
			// still had a cached local copy
			if (m_reader == null) {
				return null;
			}
			if (m_currentCheckout != null) {
				throw new IllegalStateException("This should not happen");
			}
			final MessageWrapper msg = m_queue.removeFirst();
			if (msg == null) {
				return null;
			}
			return m_currentCheckout = new CheckoutWrapper(msg);
		}
		
		@Override
		synchronized boolean tryEnqueue(ReferenceCounted msg, IMessageQueueSubmissionListener listener) {
			if (m_queue.count() >= m_maxMsgCount) {
				return false;
			}
			// TODO If system is shutting down, reject messages <<<<<<----------------------------------------------------------------
			
			final MessageWrapper mw = new MessageWrapper(msg, NeuronSystemTLS.currentNeuron(), listener);
			m_queue.addLast(mw);
			if (m_queue.count() == 1) {
				if (m_reader != null) {
					if (LOG.isTraceEnabled()) {
						LOG.trace("Queue {} tryWrite() sending DataReady event to reader", m_queueName);
					}
					m_reader.onEvent(IMessageReader.Event.DataReady);
				}
			}
			return true;
		}
		
		private class CheckoutWrapper implements IMessageQueueSubmission {
			private final int m_id;
			private MessageWrapper m_wrapped;

			CheckoutWrapper(MessageWrapper wrapped) {
				m_id = wrapped.id();
				m_wrapped = wrapped;
			}
			
			private void close0() {
				if (m_wrapped != null) {
					if (LOG.isTraceEnabled()) {
						LOG.trace("Queue {} moving incomplete message back to queue", m_queueName);
					}
					m_wrapped.reset();
					m_queue.addFirst(m_wrapped);
					m_wrapped = null;
				}
			}
			
			@Override
			public int id() {
				return m_id;
			}

			@Override
			public void setAsReceived() {
				synchronized(CreatedQueueBroker.this) {
					if (m_wrapped != null) {
						m_wrapped.setAsReceived();
					}
				}
			}

			@Override
			public void setAsStartedProcessing() {
				synchronized(CreatedQueueBroker.this) {
					if (m_wrapped != null) {
						m_wrapped.setAsStartedProcessing();
					}
				}
			}

			@Override
			public void setAsProcessed() {
				synchronized(CreatedQueueBroker.this) {
					if (m_wrapped != null) {
						m_wrapped.setAsProcessed();
						m_wrapped = null;
						m_currentCheckout = null;
						if (m_reader != null) {
							if (LOG.isTraceEnabled()) {
								LOG.trace("Queue {} CheckoutWrapper.setAsProcessed() sending DataReady event to reader", m_queueName);
							}
							m_reader.onEvent(IMessageReader.Event.DataReady);
						}
					}
				}
			}

			@Override
			public ReferenceCounted message() {
				synchronized(CreatedQueueBroker.this) {
					if (m_wrapped != null) {
						return m_wrapped.message();
					} else {
						return null;
					}
				}
			}
			
		}
	}
	
	private static class Registrant implements INeuronApplicationSystem {

		@Override
		public String systemName()
		{
			return "MessageQueueSystem";
		}

		@Override
		public Future<Void> startShutdown() {
			final boolean noActive;
			m_shutdownLock.writeLock().lock();
			try {
				m_shuttingDown = true;
				if (m_numConnectedReaders.get()==0 && m_numConnectedSubmitters.get()==0) {
					noActive = true;
				} else {
					m_shutdownPromise = NeuronApplication.newPromise();
					noActive = false;
				}
			} finally {
				m_shutdownLock.writeLock().unlock();
			}
			if (noActive) {
				closeQueues();
				return NeuronApplication.newSucceededFuture((Void)null);
			}
			final Promise<Void> shutdownCompletePromise = NeuronApplication.newPromise();
			m_shutdownPromise.addListener((f) -> {
				closeQueues();
				shutdownCompletePromise.trySuccess((Void)null);
			});
			return shutdownCompletePromise;
		}
		
		private void closeQueues() {
			m_queueBrokerByName.forEach((name, q) -> {
				q.close();
				return true;
			});
			m_queueBrokerByName.clear();
		}
		
	}
}
