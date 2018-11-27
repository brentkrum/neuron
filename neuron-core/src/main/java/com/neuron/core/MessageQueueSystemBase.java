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
import com.neuron.core.netty.TSPromiseCombiner;
import com.neuron.utility.CharSequenceTrie;
import com.neuron.utility.FastLinkedList;

import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import io.netty.util.internal.PlatformDependent;

class MessageQueueSystemBase
{
	private static final Logger LOG = LogManager.getLogger(MessageQueueSystemBase.class);
	
	protected static final String queueBrokerConfig_MaxMsgCount = "maxQueueMsgCount";
	protected static final String queueBrokerConfig_MaxSimultaneousCheckoutCount = "maxSimultaneousCheckoutCount";

	private static final int DEFAULT_QUEUE_MSG_COUNT = Config.getFWInt("core.MessageQueueSystem.defaultMaxQueueMsgCount", Integer.valueOf(1024));
	private static final int DEFAULT_MAX_SIMULTANEOUS_CHECKOUT_COUNT = Config.getFWInt("core.MessageQueueSystem.defaultMaxSimultaneousCheckoutCount", Integer.valueOf(8));
	private static final ReadWriteLock m_brokerLock = new ReentrantReadWriteLock(true);
	private static final CharSequenceTrie<CreatedQueueBroker> m_queueBrokerByName = new CharSequenceTrie<>();

	private static final AtomicInteger m_numConnectedReaders = new AtomicInteger();
	private static final AtomicInteger m_numConnectedSubmitters = new AtomicInteger();
	private static final ReadWriteLock m_shutdownLock = new ReentrantReadWriteLock(true);
	private static Promise<Void> m_shutdownPromise;
	private static boolean m_shuttingDown = false;
	private static boolean m_shutdownComplete = false;

	protected static String createFQQN(String declaringNeuronInstanceName, String queueName) {
		return declaringNeuronInstanceName + ":" + queueName;
	}
	
	protected static void defineQueue(String queueName, ObjectConfig queueConfig, IDuplexMessageQueueReaderCallback callback) {
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
	
	protected static void defineQueue(String queueName, ObjectConfig queueConfig, IMessageQueueAsyncReaderCallback callback) {
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
					m_shutdownComplete = true;
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
	protected static boolean submitToQueue(String declaringNeuronInstanceName, String queueName, ReferenceCounted msg, IMessageQueueSubmissionListener listener) {
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
	protected static boolean submitToQueue(String fqqn, ReferenceCounted msg, IMessageQueueSubmissionListener listener) {
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
			if (m_shutdownComplete) {
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
					m_shutdownComplete = true;
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
		private ReferenceCounted m_response;
		
		// These can be called multiple times
		private boolean m_reset;
		private boolean m_wasReceived;
		private boolean m_notifiedReceived;
		private boolean m_startedProcessing;
		private boolean m_notifiedStartedProcessing;
		
		// These will only be called once
		private boolean m_completedProcessing;
		private boolean m_notifiedCompletedProcessing;
		private boolean m_undelivered; 
		private boolean m_notifiedUndelivered; 
		
		private Runnable m_resetCallback;
		
		MessageWrapper(ReferenceCounted msg, NeuronRef listenerRef, IMessageQueueSubmissionListener listener) {
			m_msg = msg.retain(); // We own a reference to it, in addition to the one we are passing along
			m_listenerRef = listenerRef;
			m_listener = listener;
		}
		
		@Override
		protected MessageWrapper getObject() {
			return this;
		}

		void reset(Runnable resetCallback) {
			m_resetCallback = resetCallback;
			m_reset = true;
			m_worker.requestMoreWork();
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
		public ReferenceCounted startProcessing() {
			if (!m_startedProcessing) {
				m_wasReceived = true;
				m_startedProcessing = true;
				m_worker.requestMoreWork();
			}
			return m_msg;
		}

		@Override
		public void cancelProcessing() {
			throw new UnsupportedOperationException("This should not be called");
		}

		@Override
		public void setAsProcessed(ReferenceCounted response) {
			m_wasReceived = true;
			m_startedProcessing = true;
			m_response = response;
			m_completedProcessing = true;
			m_worker.requestMoreWork();
		}
		
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
					// The message was NOT delivered, we still have two references (the one we added and the original)
					ReferenceCountUtil.safeRelease(m_msg);
					ReferenceCountUtil.safeRelease(m_msg);
					return;
				}
				if (m_reset) {
					m_reset = false;
					if (!m_undelivered && !m_completedProcessing) {
						m_wasReceived = false;
						m_notifiedReceived = false;
						m_startedProcessing = false;
						m_notifiedStartedProcessing = false;
						requestMoreWork();
					}
					if (m_resetCallback != null) {
						m_resetCallback.run();
						m_resetCallback = null;
					}
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
								if (m_listener instanceof ISimplexMessageQueueSubmissionListener) {
									((ISimplexMessageQueueSubmissionListener)m_listener).onProcessed(m_msg);
								} else {
									((IDuplexMessageQueueSubmissionListener)m_listener).onProcessed(m_msg, m_response);
								}
							}
						} catch(Exception ex) {
							LOG.error("Unhandled exception in listener callback", ex);
						}
					}
					ReferenceCountUtil.safeRelease(m_response);
					// We still own a reference to this message, the other one was passed to reader
					ReferenceCountUtil.safeRelease(m_msg);
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
		private final FastLinkedList<CheckoutWrapper> m_inProcess = new FastLinkedList<>();
		private int m_maxSimultaneous;
		private int m_maxMsgCount;
		private IMessageReader m_reader;
		
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
			
			m_maxSimultaneous = config.getInteger(MessageQueueSystemBase.queueBrokerConfig_MaxSimultaneousCheckoutCount, DEFAULT_MAX_SIMULTANEOUS_CHECKOUT_COUNT);
			m_maxMsgCount = config.getInteger(MessageQueueSystemBase.queueBrokerConfig_MaxMsgCount, DEFAULT_QUEUE_MSG_COUNT);
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
				lock.addStateAsyncListener(NeuronState.Disconnecting, (successful, neuronRef, promise) -> {
					final Promise<Void> closePromise = NeuronApplication.newPromise();
					closePromise.addListener((f) -> {
						try(INeuronStateLock l = neuronRef.lockState()) {
							NeuronApplication.log(Level.INFO, Level.DEBUG, LOG, "Disconnected from queue '{}'", m_queueName);
						}
						promise.setSuccess((Void)null);
						readerDisconnected();
					});
					closeReader(closePromise);
				});
			}
		}
		
		synchronized void closeReader(Promise<Void> closePromise) {
			final TSPromiseCombiner groupPromise = new TSPromiseCombiner();
			// Clear the in-process messages
			while(true) {
				// Remove the oldest item, reset it, and put back into queue
				final CheckoutWrapper cw = m_inProcess.removeFirst();
				if (cw == null) {
					break;
				}
				Promise<Void> p = NeuronApplication.newPromise();
				groupPromise.add(p);
				cw.close0(p);
			}
			groupPromise.finish(closePromise);
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
			if (m_inProcess.count() >= m_maxSimultaneous) {
				return null;
			}
			final MessageWrapper msg = m_queue.removeFirst();
			if (msg == null) {
				return null;
			}
			final CheckoutWrapper cw;
			m_inProcess.add(cw = new CheckoutWrapper(msg));
			return cw;
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
		
		private class CheckoutWrapper extends FastLinkedList.LLNode<CheckoutWrapper> implements IMessageQueueSubmission {
			private final int m_id;
			private volatile MessageWrapper m_wrapped;
			private volatile boolean m_started;
			private volatile Promise<Void> m_closingPromise;
			
			CheckoutWrapper(MessageWrapper wrapped) {
				m_id = wrapped.id();
				m_wrapped = wrapped;
			}
			
			@Override
			protected CheckoutWrapper getObject() {
				return this;
			}
			
			private void close0(Promise<Void> closingPromise) {
				synchronized(CreatedQueueBroker.this) {
					if (m_wrapped == null) {
						m_closingPromise.setSuccess((Void)null);
						return;
					}
					if (m_started) {
						// The message has been grabbed by neuron and is currently being processed
						m_closingPromise = closingPromise;
						return;
					}
					m_inProcess.remove(this);
					final MessageWrapper mw = m_wrapped;
					m_wrapped.reset(() -> {
						if (LOG.isTraceEnabled()) {
							LOG.trace("Queue {} moving incomplete message back to queue", m_queueName);
						}
						synchronized(CreatedQueueBroker.this) {
							m_queue.addFirst(mw);
						}						
						m_closingPromise.setSuccess((Void)null);
					});
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
			public ReferenceCounted startProcessing() {
				synchronized(CreatedQueueBroker.this) {
					if (m_wrapped != null) {
						m_started = true;
						return m_wrapped.startProcessing();
					}
				}
				return null;
			}

			
			@Override
			public void cancelProcessing() {
				synchronized(CreatedQueueBroker.this) {
					if (m_wrapped == null) {
						return;
					}
					if (LOG.isTraceEnabled()) {
						LOG.trace("Queue {} moving cancelled message back to queue", m_queueName);
					}
					m_inProcess.remove(this);
					final MessageWrapper mw = m_wrapped;
					m_wrapped = null;
					mw.reset(() -> {
						synchronized(CreatedQueueBroker.this) {
							m_queue.addFirst(m_wrapped);
							if (m_reader != null) {
								if (LOG.isTraceEnabled()) {
									LOG.trace("Queue {} CheckoutWrapper.setAsProcessed() sending DataReady event to reader", m_queueName);
								}
								m_reader.onEvent(IMessageReader.Event.DataReady);
							}
						}
						if (m_closingPromise != null) {
							m_closingPromise.setSuccess((Void)null);
							m_closingPromise = null;
						}
					});
				}
			}

			@Override
			public void setAsProcessed(ReferenceCounted response) {
				synchronized(CreatedQueueBroker.this) {
					if (m_wrapped == null) {
						return;
					}
					m_inProcess.remove(this);
					m_wrapped.setAsProcessed(response);
					m_wrapped = null;
					if (m_closingPromise != null) {
						m_closingPromise.setSuccess((Void)null);
						m_closingPromise = null;
					}
					if (m_reader != null) {
						if (LOG.isTraceEnabled()) {
							LOG.trace("Queue {} CheckoutWrapper.setAsProcessed() sending DataReady event to reader", m_queueName);
						}
						m_reader.onEvent(IMessageReader.Event.DataReady);
					}
				}
			}
			
		}
	}
	
	protected static Future<Void> startShutdown() {
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
	
	private static void closeQueues() {
		m_queueBrokerByName.forEach((name, q) -> {
			q.close();
			return true;
		});
		m_queueBrokerByName.clear();
	}

}
