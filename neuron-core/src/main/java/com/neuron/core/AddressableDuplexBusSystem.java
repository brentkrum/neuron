package com.neuron.core;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;

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
import io.netty.util.internal.ObjectUtil;
import io.netty.util.internal.PlatformDependent;


public final class AddressableDuplexBusSystem
{
	private static final Logger LOG = LogManager.getLogger(AddressableDuplexBusSystem.class);
	
	public static final String submitConfig_TimeoutInMS = "timeout";
	public static final String submitConfig_autoCreateAddress = "autoCreate";
	
	public static final String readerConfig_MaxQueueMsgCount = "maxQueueMsgCount";
	public static final String readerConfig_MaxSimultaneousMsgCount = "maxSimultaneousMsgCount";
	public static final String readerConfig_RemoveAddressOnDisconnect = "removeAddressOnDisconnect";

	private static final int DEFAULT_QUEUE_MSG_COUNT = Config.getFWInt("core.AddressableDuplexBusSystem.defaultMaxQueueMsgCount", Integer.valueOf(1024));
	private static final ReadWriteLock m_busNameLock = new ReentrantReadWriteLock(true);
	private static final CharSequenceTrie<AddressableDuplexBus> m_busByName = new CharSequenceTrie<>();
	private static final AtomicInteger m_numConnectedReaders = new AtomicInteger();
	private static final AtomicInteger m_numConnectedSubmitters = new AtomicInteger();
	
	private static final ReadWriteLock m_shutdownLock = new ReentrantReadWriteLock(true);
	private static Promise<Void> m_shutdownPromise;
	private static volatile boolean m_shuttingDown = false;
	private static volatile boolean m_shutdownComplete = false;
	
	private AddressableDuplexBusSystem() {
	}
	
	// TODO need to add options <<<<---------------------------------------------------------------------------------------------------------------------------------------
	//		"remove bus address pattern on disconnect" flag
	//
	public static void listenOnBus(String busName, Pattern addressPattern, ObjectConfig listenerConfig, IAddressableDuplexBusListener listen) {
		ObjectUtil.checkNotNull(busName, "busName cannot be null");
		ObjectUtil.checkNotNull(addressPattern, "addressPattern cannot be null");
		ObjectUtil.checkNotNull(listenerConfig, "listenerConfig cannot be null");
		ObjectUtil.checkNotNull(listen, "listen cannot be null");
		
		startAddReader();
		try {
			NeuronSystemTLS.validateNeuronAwareThread();
			final NeuronRef currentNeuronRef = NeuronSystemTLS.currentNeuron();
	
			try(INeuronStateLock lock = currentNeuronRef.lockState()) {
				NeuronSystemTLS.validateInNeuronConnectResources(lock);
				
				final AddressableDuplexBus bus = getOrCreateBus(busName);
				bus.createAddress(addressPattern, currentNeuronRef, listenerConfig, listen);
			}
		} finally {
			endAddReader();
		}
}
	
	// TODO need to add options <<<<---------------------------------------------------------------------------------------------------------------------------------------
	//		"remove bus address on disconnect" flag
	//
	public static void listenOnBus(String busName, String address, ObjectConfig listenerConfig, IAddressableDuplexBusListener listen) {
		ObjectUtil.checkNotNull(busName, "busName cannot be null");
		ObjectUtil.checkNotNull(address, "address cannot be null");
		ObjectUtil.checkNotNull(listenerConfig, "listenerConfig cannot be null");
		ObjectUtil.checkNotNull(listen, "listen cannot be null");
		
		startAddReader();
		try {
			NeuronSystemTLS.validateNeuronAwareThread();
			final NeuronRef currentNeuronRef = NeuronSystemTLS.currentNeuron();
			
			try(INeuronStateLock lock = currentNeuronRef.lockState()) {
				NeuronSystemTLS.validateInNeuronConnectResources(lock);
				
				final AddressableDuplexBus bus = getOrCreateBus(busName);
				bus.createAddress(address, currentNeuronRef, listenerConfig, listen);
			}
		} finally {
			endAddReader();
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
			/*
			 * Right here:
			 * 	shutdown could start and complete
			 * 	OR a new reader could get through its read lock
			 */
			m_shutdownLock.writeLock().lock();
			try {
				if (m_shuttingDown && !m_shutdownComplete && m_numConnectedReaders.get()==0 && m_numConnectedSubmitters.get()==0) {
					m_shutdownComplete = true;
					m_shutdownPromise.setSuccess((Void)null);
				}
			} finally {
				m_shutdownLock.writeLock().unlock();
			}
		}
	}
	
	// TODO need to add options <<<<---------------------------------------------------------------------------------------------------------------------------------------
	//		timeout
	//		
	public static boolean submitToBus(String busName, String address, ReferenceCounted msg) {
		return submitToBus(busName, address, msg, null, ObjectConfigBuilder.emptyConfig());
	}
	public static boolean submitToBus(String busName, String address, ReferenceCounted msg, ObjectConfig config) {
		return submitToBus(busName, address, msg, null, config);
	}
	public static boolean submitToBus(String busName, String address, ReferenceCounted msg, IDuplexBusSubmissionListener listener) {
		return submitToBus(busName, address, msg, listener, ObjectConfigBuilder.emptyConfig());
	}
	public static boolean submitToBus(String busName, String address, ReferenceCounted msg, IDuplexBusSubmissionListener listener, ObjectConfig config) {
		ObjectUtil.checkNotNull(busName, "busName cannot be null");
		ObjectUtil.checkNotNull(address, "address cannot be null");
		ObjectUtil.checkNotNull(msg, "msg cannot be null");

		NeuronSystemTLS.validateNeuronAwareThread();
		final NeuronRef currentNeuronRef = NeuronSystemTLS.currentNeuron();
		
		try(INeuronStateLock lock = currentNeuronRef.lockState()) {
			if (!lock.isStateOneOf(NeuronState.SystemOnline, NeuronState.Online, NeuronState.GoingOffline)) {
				return false;
			}
			startAddSubmitter();
			try {
				final AddressableDuplexBus bus = getOrCreateBus(busName);
				final boolean autoCreateAddress = config.getBoolean(submitConfig_autoCreateAddress);
				return bus.tryEnqueue(address, msg, listener, autoCreateAddress);
			} finally {
				endAddSubmitter();
			}
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
			/*
			 * Right here:
			 * 	shutdown could start and complete
			 * 	OR a new submitter could get through its read lock
			 */
			m_shutdownLock.writeLock().lock();
			try {
				if (m_shuttingDown && !m_shutdownComplete && m_numConnectedSubmitters.get()==0 && m_numConnectedReaders.get()==0) {
					m_shutdownComplete = true;
					m_shutdownPromise.setSuccess((Void)null);
				}
			} finally {
				m_shutdownLock.writeLock().unlock();
			}
		}
	}

	private static AddressableDuplexBus getOrCreateBus(String busName) {
		AddressableDuplexBus bus;
		boolean unlockRead = true;
		m_busNameLock.readLock().lock();
		try {
			bus = m_busByName.get(busName);
			if (bus == null) {
				// A read lock cannot upgrade to a write lock, so we release the read and acquire the write
				m_busNameLock.readLock().unlock();
				m_busNameLock.writeLock().lock();
				try {
					unlockRead = false;
					bus = m_busByName.get(busName);
					if (bus == null) {
						LOG.info("Creating addressable duplex bus: {}", busName);
						m_busByName.addOrFetch(busName, bus = new AddressableDuplexBus(busName));
					}
				} finally {
					m_busNameLock.writeLock().unlock();
				}
			}
		} finally {
			if (unlockRead) {
				m_busNameLock.readLock().unlock();
			}
		}
		return bus;
	}
	
	interface IMessageReader {
		public enum Event { DataReady };
		NeuronRef owner();
		void onEvent(Event event);
		void close();
	}

	private static final class MessageWrapper extends FastLinkedList.LLNode<MessageWrapper> {
		private static final AtomicInteger m_nextId = new AtomicInteger();
		private final int m_messageId = m_nextId.incrementAndGet();
		private final Worker m_worker = new Worker();
		private final ReferenceCounted m_requestMsg;
		private final NeuronRef m_listenerRef;
		private final IDuplexBusSubmissionListener m_listener;
		
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
		
		private ReferenceCounted m_responseMsg;
		private Throwable m_failureCause;
		private Runnable m_resetCallback;
		
		MessageWrapper(ReferenceCounted msg, NeuronRef listenerRef, IDuplexBusSubmissionListener listener) {
			m_requestMsg = msg.retain(); // We own a reference to it, in addition to the one we are passing along
			m_listenerRef = listenerRef;
			m_listener = listener;
		}
		
		@Override
		protected MessageWrapper getObject() {
			return this;
		}
		
		// Mutually exclusive with setAsReceived()
		void setAsUndelivered() {
			m_undelivered = true;
			m_worker.requestMoreWork();
		}
		
		// Mutually exclusive with setAsUndelivered()
		void setAsReceived() {
			if (!m_wasReceived) {
				m_wasReceived = true;
				m_worker.requestMoreWork();
			}
		}

		int id() {
			return m_messageId;
		}

		ReferenceCounted startProcessing() {
			if (!m_startedProcessing) {
				m_startedProcessing = true;
				m_worker.requestMoreWork();
			}
			return m_requestMsg;
		}

		void reset(Runnable resetCallback) {
			m_resetCallback = resetCallback;
			m_reset = true;
			m_worker.requestMoreWork();
		}

		void setAsProcessed(ReferenceCounted responseMsg) {
			m_responseMsg = responseMsg;
			m_completedProcessing = true;
			m_worker.requestMoreWork();
		}

		void setAsFailed(Throwable t) {
			m_failureCause = t;
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
								m_listener.onUndelivered(m_requestMsg);
							}
						} catch(Exception ex) {
							NeuronApplication.logError(LOG, "Unhandled exception in listener callback", ex);
						}
					}
					// The message was NOT delivered, so we still own the reference
					m_requestMsg.release();
					return;
				}
				if (m_reset) {
					m_reset = false;
					if (!m_undelivered && !m_completedProcessing) {
						m_wasReceived = false;
						m_notifiedReceived = false;
						m_startedProcessing = false;
						m_notifiedStartedProcessing = false;
						
						if (m_listener != null) {
							try(INeuronStateLock lock = m_listenerRef.lockState()) {
								if (lock.isStateOneOf(NeuronState.SystemOnline, NeuronState.Online, NeuronState.GoingOffline)) {
									// If the listener wants to keep m_msg, it needs to call retain()
									m_listener.onReset(m_requestMsg);
								}
							} catch(Exception ex) {
								NeuronApplication.logError(LOG, "Unhandled exception in listener callback", ex);
							}
						}
						requestMoreWork();
					}
					m_resetCallback.run();
					m_resetCallback = null;
					return;
				}
				if (!m_wasReceived) {
					return;
				} else if (!m_notifiedReceived) {
					m_notifiedReceived = true;
					if (m_listener != null) {
						try(INeuronStateLock lock = m_listenerRef.lockState()) {
							if (lock.isStateOneOf(NeuronState.SystemOnline, NeuronState.Online, NeuronState.GoingOffline)) {
								m_listener.onReceived(m_requestMsg);
							}
						} catch(Exception ex) {
							NeuronApplication.logError(LOG, "Unhandled exception in listener callback", ex);
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
								m_listener.onStartProcessing(m_requestMsg);
							}
						} catch(Exception ex) {
							NeuronApplication.logError(LOG, "Unhandled exception in listener callback", ex);
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
								if (m_failureCause != null) {
									NeuronApplication.logError(LOG, "System failure in processing neuron", m_failureCause);
									m_listener.onSystemFailure(m_requestMsg);
								} else {
									m_listener.onProcessed(m_requestMsg, m_responseMsg);
								}
							}
						} catch(Exception ex) {
							NeuronApplication.logError(LOG, "Unhandled exception in listener callback", ex);
						}
					}
					ReferenceCountUtil.safeRelease(m_responseMsg);
					// We still own a reference to this message, the other one was passed to reader
					ReferenceCountUtil.safeRelease(m_requestMsg);
				}
				
			}
			
		}
	}
	
	private static final class AddressableDuplexBus {
		private final GatherBrokers m_gather = new GatherBrokers();
		private final ReadWriteLock m_addressLock = new ReentrantReadWriteLock(true);
		private final String m_busName;
		private final CharSequenceTrie<ReaderBroker> m_addresses = new CharSequenceTrie<>();
		private final LinkedList<ReaderBroker> m_addressPatterns = new LinkedList<>();
		
		AddressableDuplexBus(String busName) {
			m_busName = busName;
		}
		
		void close() {
			m_addresses.forEach((addr, broker) -> {
				broker.close();
				return true;
			});
			for(ReaderBroker broker : m_addressPatterns) {
				broker.close();
			}
		}
		
		public boolean tryEnqueue(String address, ReferenceCounted msg, IDuplexBusSubmissionListener listener, boolean autoCreateBroker) {
			ReaderBroker broker;
			m_addressLock.readLock().lock();
			try {
				broker = m_addresses.get(address);
				if (broker == null) {
					for(ReaderBroker b : m_addressPatterns) {
						if (b.m_addressPattern.matcher(address).matches()) {
							broker = b;
							break;
						}
					}
				}
				if (broker == null) {
					if (!autoCreateBroker) {
						return false;
					}
					LOG.info("Bus {}, auto-creating broker at address: {}", m_busName, address);
					m_addresses.addOrFetch(address, broker = new ReaderBroker(new BusContext(address)));
				}
				return broker.tryEnqueue(msg, listener);
			} finally {
				m_addressLock.readLock().unlock();
			}
		}

		public void createAddress(String address, NeuronRef ref, ObjectConfig config, IAddressableDuplexBusListener listener) {
			ReaderBroker broker;
			m_addressLock.writeLock().lock();
			try {
				broker = m_addresses.get(address);
				if (broker == null) {
					for(ReaderBroker b : m_addressPatterns) {
						if (b.m_busContext.m_addressPattern.matcher(address).matches()) {
							throw new IllegalArgumentException("In bus '" + m_busName + "' the address '" + address + "' matches the addressPattern '" + b.m_address + "'.  Cannot listen on an address that matches an existing address pattern.");
						}
					}
					m_addresses.addOrFetch(address, broker = new ReaderBroker(new BusContext(address)));
				}
				broker.setNewReader(config, new AddressableDuplexBusReader(ref, broker, config, listener));
			} finally {
				m_addressLock.writeLock().unlock();
			}
		}

		public void createAddress(Pattern addressPattern, NeuronRef ref, ObjectConfig config, IAddressableDuplexBusListener listener) {
			m_addressLock.writeLock().lock();
			try {
				m_gather.reset(addressPattern);
				m_addresses.forEach(m_gather);
				final ReaderBroker broker = m_gather.createBroker();
				m_addressPatterns.add(broker);
				broker.setNewReader(config, new AddressableDuplexBusReader(ref, broker, config, listener));
			} finally {
				m_addressLock.writeLock().unlock();
			}
		}
		
		private final class GatherBrokers implements CharSequenceTrie.IForEach<ReaderBroker> {
			private final ArrayList<ReaderBroker> existingBrokers = new ArrayList<>();
			Pattern addressPattern;

			void reset(Pattern addressPattern) {
				this.addressPattern = addressPattern;
			}
			
			ReaderBroker createBroker() {
				if (m_gather.existingBrokers.size() == 0) {
					// Create a new broker
					LOG.info("Bus {}, creating pattern broker at address '{}'", m_busName, addressPattern.toString());
					return new ReaderBroker(new BusContext(addressPattern));
				}
//				
//				if (m_gather.existingBrokers.size() == 1) {
//					// Just use existing broker
//					final ReaderBroker broker = m_gather.existingBrokers.get(0);
//					existingBrokers.clear();
//					return broker;
//				}
				// Merge all existing broker's queued items
				final ReaderBroker broker = new ReaderBroker(new BusContext(addressPattern));
				for(ReaderBroker oldBroker : m_gather.existingBrokers) {
					LOG.info("Bus {}, merging broker at address '{}' into new pattern broker for pattern '{}'", m_busName, oldBroker.m_address, broker.m_address);
					broker.merge(oldBroker);
				}
				existingBrokers.clear();
				return broker;
			}
			
			@Override
			public boolean process(CharSequence key, ReaderBroker object) {
				if (addressPattern.matcher(key).matches()) {
					if (object.m_reader != null) {
						throw new IllegalArgumentException("In bus '" + m_busName + "' the supplied address pattern matches the existing in-use address '" + object.m_address + "'.  Cannot add an address pattern that matches addresses already in use.");
						
					} else {
						// If there is no reader, we need to move the queued items to us
						existingBrokers.add(object);
					}
				}
				return true;
			}
			
		}
		
		private final class BusContext {
			private final String m_address;
			private final Pattern m_addressPattern;
			
			BusContext(String address) {
				m_address = address;
				m_addressPattern = null;
			}

			BusContext(Pattern addressPattern) {
				m_address = addressPattern.pattern();
				m_addressPattern = addressPattern;
			}
			
			String getBusName() {
				return m_busName;
			}

			String getAddress() {
				return m_address;
			}
			
			Pattern getAddressPattern() {
				return m_addressPattern;
			}
			
			void removeAddressFromBus(ReaderBroker broker) {
				m_addressLock.writeLock().lock();
				try {
					if (m_addressPattern != null) {
						LOG.info("Bus {}, removing broker for address pattern: {}", m_busName, m_address);
						m_addressPatterns.remove(broker);
					} else {
						LOG.info("Bus {}, removing broker at address: {}", m_busName, m_address);
						m_addresses.remove(m_address);
					}
				} finally {
					m_addressLock.writeLock().unlock();
				}
			}
		}
	}
	
	static final class ReaderBroker {
		private final AddressableDuplexBus.BusContext m_busContext;
		private final String m_busName;
		private final String m_address;
		private final Pattern m_addressPattern;
		private final FastLinkedList<MessageWrapper> m_queue = new FastLinkedList<>();
		private final FastLinkedList<CheckoutWrapper> m_inProcess = new FastLinkedList<>();
		private int m_maxSimultaneous;
		private int m_maxQueueCount;
		private IMessageReader m_reader;
		private boolean m_removeOnReaderDisconnect = true;
		
		ReaderBroker(AddressableDuplexBus.BusContext busContext) {
			m_busContext = busContext;
			m_busName = m_busContext.getBusName();
			m_address = m_busContext.getAddress();
			m_addressPattern = m_busContext.getAddressPattern();
			m_maxQueueCount = DEFAULT_QUEUE_MSG_COUNT;
		}
//		
//		ReaderBroker(String busName, String address) {
//			m_busName = busName;
//			m_address = address;
//			m_addressPattern = null;
//			m_maxQueueCount = DEFAULT_QUEUE_MSG_COUNT;
//		}
//
//		ReaderBroker(String busName, Pattern addressPattern) {
//			m_busName = busName;
//			m_address = addressPattern.pattern();
//			m_addressPattern = addressPattern;
//			m_maxQueueCount = DEFAULT_QUEUE_MSG_COUNT;
//		}
		
		String busName() {
			return m_busName;
		}
		
		void merge(ReaderBroker oldBroker) {
			List<MessageWrapper> oldItems = oldBroker.m_queue.snapshotList();
			oldBroker.m_queue.clear();
			for(MessageWrapper mw : oldItems) {
				m_queue.add(mw);
			}
		}
		
		void close() {
			m_queue.forEach(cw -> {
				cw.setAsUndelivered();
				return true;
			});
			m_queue.clear();
		}
		
		synchronized void setNewReader(ObjectConfig config, IMessageReader reader) {
			if (m_reader != null) {
				final UnsupportedOperationException ex = new UnsupportedOperationException("Attempted to add a second reader for bus '" + m_busName + "' address '" + m_address + "' which is currently listened to by neuron " + m_reader.owner().logString() + ". A bus address can only have a single reader.");
				PlatformDependent.throwException(ex);
				return;
			}
			m_reader = reader;
			m_numConnectedReaders.incrementAndGet();
			NeuronApplication.log(Level.INFO, Level.DEBUG, LOG, "Connected to bus '{}' at address '{}'", m_busName, m_address);
			
			m_maxQueueCount = config.getInteger(AddressableDuplexBusSystem.readerConfig_MaxQueueMsgCount, DEFAULT_QUEUE_MSG_COUNT);
			m_maxSimultaneous = config.getInteger(AddressableDuplexBusSystem.readerConfig_MaxSimultaneousMsgCount, 4);
			m_removeOnReaderDisconnect = config.getBoolean(readerConfig_RemoveAddressOnDisconnect, Boolean.TRUE);
			
			final IMessageReader r = m_reader;
			try(INeuronStateLock lock = m_reader.owner().lockState()) {
				lock.addStateListener(NeuronState.SystemOnline, (isSuccess) -> {
					if (!isSuccess) {
						return;
					}
					synchronized(ReaderBroker.this) {
						if (m_queue.count() > 0) {
							if (LOG.isTraceEnabled()) {
								LOG.trace("Bus {} address {} sending DataReady event to reader", m_busName, m_address);
							}
							r.onEvent(IMessageReader.Event.DataReady);
						} else {
							if (LOG.isTraceEnabled()) {
								LOG.trace("Bus {} address {} sending no events to reader, queue empty", m_busName, m_address);
							}
						}
					}
				});

				lock.addStateAsyncListener(NeuronState.Disconnecting, (successful, neuronRef, promise) -> {
					if (m_removeOnReaderDisconnect) {
						m_busContext.removeAddressFromBus(ReaderBroker.this);
						m_reader.close();
						m_reader = null;
					}
					
					final Promise<Void> closePromise = NeuronApplication.newPromise();
					startCloseReader(closePromise);
					closePromise.addListener((f) -> {
						try(INeuronStateLock l = neuronRef.lockState()) {
							NeuronApplication.log(Level.INFO, Level.DEBUG, LOG, "Disconnected from bus '{}' address '{}'", m_busName, m_address);
						}
						if (m_removeOnReaderDisconnect) {
							close();
						} else {
							m_reader.close();
							m_reader = null;
						}
						readerDisconnected();
						promise.setSuccess((Void)null);
					});
				});
			}
		}
		
		private synchronized void startCloseReader(Promise<Void> closePromise) {
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
		}
		
		synchronized IAddressableDuplexBusSubmission dequeue() {
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
			msg.setAsReceived();
			final CheckoutWrapper cw;
			m_inProcess.add(cw = new CheckoutWrapper(msg));
			return cw;
		}
		
		synchronized boolean tryEnqueue(ReferenceCounted msg, IDuplexBusSubmissionListener listener) {
			if (m_queue.count() >= m_maxQueueCount) {
				return false;
			}
			
			final MessageWrapper mw = new MessageWrapper(msg, NeuronSystemTLS.currentNeuron(), listener);
			if (!m_queue.add(mw)) {
				return false;
			}
			if (m_queue.count() == 1) {
				if (m_reader != null) {
					if (LOG.isTraceEnabled()) {
						LOG.trace("Bus {} address {} sending DataReady event to reader", m_busName, m_address);
					}
					m_reader.onEvent(IMessageReader.Event.DataReady);
				}
			}
			return true;
		}
		
		private class CheckoutWrapper extends FastLinkedList.LLNode<CheckoutWrapper> implements IAddressableDuplexBusSubmission {
			private final int m_id;
			private volatile MessageWrapper m_wrapped;
			private volatile boolean m_started;
			private volatile Promise<Void> m_closePromise;
			
			CheckoutWrapper(MessageWrapper wrapped) {
				m_id = wrapped.id();
				m_wrapped = wrapped;
			}
			
			@Override
			protected CheckoutWrapper getObject() {
				return this;
			}

			private void close0(Promise<Void> closePromise) {
				synchronized(ReaderBroker.this) {
					if (m_wrapped == null) {
						closePromise.setSuccess((Void)null);
						return;
					}
					if (m_started) {
						m_closePromise = closePromise;
						return;
					}
					m_inProcess.remove(this);
					final MessageWrapper mw = m_wrapped;
					m_wrapped = null;
					mw.reset(() -> {
						if (LOG.isTraceEnabled()) {
							LOG.trace("Bus {} address {} moving message {} from in-process back to queue", m_busName, m_address, m_id);
						}
						synchronized(ReaderBroker.this) {
							// Push to front of queue
							m_queue.addFirst(mw);
						}
						closePromise.setSuccess((Void)null);
					});
				}
			}
			
			@Override
			public int id() {
				return m_id;
			}

			@Override
			public void setAsReceived() {
				synchronized(ReaderBroker.this) {
					if (m_wrapped != null) {
						m_wrapped.setAsReceived();
					}
				}
			}

			@Override
			public ReferenceCounted startProcessing() {
				synchronized(ReaderBroker.this) {
					if (m_wrapped != null) {
						m_started = true;
						return m_wrapped.startProcessing();
					}
					return null;
				}
			}
			
			

			@Override
			public void cancelProcessing() {
				synchronized(ReaderBroker.this) {
					if (m_wrapped == null) {
						return;
					}
					if (LOG.isTraceEnabled()) {
						LOG.trace("Bus {} address {} moving message {} from in-process back to queue", m_busName, m_address, m_id);
					}
					m_inProcess.remove(this);
					final MessageWrapper mw = m_wrapped;
					m_wrapped = null;
					mw.reset(() -> {
						synchronized(ReaderBroker.this) {
							// Push to front of queue
							m_queue.addFirst(mw);
							if (m_reader != null) {
								// We have to notify the reader that there might now be items to dequeue
								if (LOG.isTraceEnabled()) {
									LOG.trace("Bus {} address {} CheckoutWrapper.setAsProcessed() sending DataReady event to reader", m_busName, m_address);
								}
								m_reader.onEvent(IMessageReader.Event.DataReady);
							}
						}
						if (m_closePromise != null) {
							m_closePromise.setSuccess((Void)null);
							m_closePromise = null;
						}
					});
				}
			}

			@Override
			public void setAsProcessed(ReferenceCounted responseMsg) {
				synchronized(ReaderBroker.this) {
					if (m_wrapped == null) {
						return;
					}
					m_inProcess.remove(this);
					m_wrapped.setAsProcessed(responseMsg);
					m_wrapped = null;
					if (m_closePromise != null) {
						m_closePromise.setSuccess((Void)null);
						m_closePromise = null;
					}
					if (m_reader != null) {
						// We have to notify the reader that there might now be items to dequeue
						if (LOG.isTraceEnabled()) {
							LOG.trace("Bus {} address {} CheckoutWrapper.setAsProcessed() sending DataReady event to reader", m_busName, m_address);
						}
						m_reader.onEvent(IMessageReader.Event.DataReady);
					}
				}
			}

			@Override
			public void setAsFailed(Throwable t) {
				synchronized(ReaderBroker.this) {
					if (m_wrapped == null) {
						return;
					}
					m_inProcess.remove(this);
					m_wrapped.setAsFailed(t);
					m_wrapped = null;
					if (m_reader != null) {
						// We have to notify the reader that there might now be items to dequeue
						if (LOG.isTraceEnabled()) {
							LOG.trace("Bus {} address {} CheckoutWrapper.setAsProcessed() sending DataReady event to reader", m_busName, m_address);
						}
						m_reader.onEvent(IMessageReader.Event.DataReady);
					}
				}
			}
			
		}
	}
	
	static void register() {
		NeuronApplication.register(new Registrant());
	}
	private static class Registrant implements INeuronApplicationSystem {

		@Override
		public String systemName()
		{
			return "AddressableDuplexBusSystem";
		}

		@Override
		public Future<Void> startShutdown() {
			final boolean noActive;
			m_shutdownLock.writeLock().lock();
			try {
				m_shuttingDown = true;
				if (m_numConnectedReaders.get()==0 && m_numConnectedSubmitters.get()==0) {
					noActive = true;
					m_shutdownComplete = true;
				} else {
					m_shutdownPromise = NeuronApplication.newPromise();
					noActive = false;
				}
			} finally {
				m_shutdownLock.writeLock().unlock();
			}
			if (noActive) {
				closeBusses();
				return NeuronApplication.newSucceededFuture((Void)null);
			}
			final Promise<Void> shutdownCompletePromise = NeuronApplication.newPromise();
			m_shutdownPromise.addListener((f) -> {
				closeBusses();
				shutdownCompletePromise.trySuccess((Void)null);
			});
			return shutdownCompletePromise;
		}
		
		private void closeBusses() {
			m_busByName.forEach((name, bus) -> {
				bus.close();
				return true;
			});
			m_busByName.clear();
		}
		
	}
}
