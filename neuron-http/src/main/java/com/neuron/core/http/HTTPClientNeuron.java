package com.neuron.core.http;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.asynchttpclient.AsyncCompletionHandlerBase;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.AsyncHttpClientConfig.ResponseBodyPartFactory;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.Dsl;
import org.asynchttpclient.HttpResponseBodyPart;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Response;
import org.asynchttpclient.netty.LazyResponseBodyPart;

import com.neuron.core.DefaultNeuronInstanceBase;
import com.neuron.core.DuplexMessageQueueSystem;
import com.neuron.core.IMessageQueueSubmission;
import com.neuron.core.INeuronInitialization;
import com.neuron.core.NeuronApplication;
import com.neuron.core.NeuronRef;
import com.neuron.core.ObjectConfigBuilder;
import com.neuron.core.PerpetualWork;
import com.neuron.core.NeuronRef.INeuronStateLock;
import com.neuron.core.NeuronStateSystem.NeuronState;
import com.neuron.core.ObjectConfigBuilder.ObjectConfig;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.concurrent.Promise;
import io.netty.util.internal.PlatformDependent;

public class HTTPClientNeuron extends DefaultNeuronInstanceBase implements INeuronInitialization {
	private static final Logger LOG = LogManager.getLogger(HTTPClientNeuron.class);
	private final ObjectConfig m_config;
	private final AtomicInteger m_numOutstandingRequests = new AtomicInteger();
	private final ReadWriteLock m_disconnectLock = new ReentrantReadWriteLock(true);
	private final AtomicInteger m_requestNum = new AtomicInteger();
	
	private AsyncHttpClient m_client;

	private Promise<Void> m_disconnectedPromise;
	private boolean m_disconnecting = false;
	private boolean m_disconnected = false;
	
	public HTTPClientNeuron(NeuronRef instanceRef, ObjectConfig config) {
		super(instanceRef);
		m_config = config;
	}

	@Override
	public void init(Promise<Void> initPromise) {
		/*
		 * We will stay in the Disconnecting state until all current requests finish processing.
		 * Since async listeners don't have a timeout, we can let the requests themselves be the timeout
		 * for the state.  Once all requests are finished, we can move on to Deinit.
		 * 
		 */
		try(INeuronStateLock lock = ref().lockState()) {
			lock.addStateAsyncListener(NeuronState.Disconnecting, (successful, neuronRef, promise) -> {
				// Need to wait until all outstanding requests are done
				final boolean callPromise;
				m_disconnectLock.readLock().lock();
				try {
					if (m_numOutstandingRequests.get() == 0) {
						m_disconnected = true;
						callPromise = true;
					} else {
						m_disconnecting = true;
						m_disconnectedPromise = promise;
						callPromise = false;
					}
				} finally {
					m_disconnectLock.readLock().unlock();
				}
				if (callPromise) {
					promise.setSuccess((Void)null);
				}
			});
		}
		
		NeuronApplication.getTaskPool().submit(() -> {
			try {
				m_client = Dsl.asyncHttpClient(Dsl.config()
					.setEventLoopGroup(NeuronApplication.getIOPool())
					.setAllocator(PooledByteBufAllocator.DEFAULT)
					// .setTcpNoDelay(tcpNoDelay)
					// .setUserAgent(userAgent)
					// .setConnectionTtl(connectionTtl)
					// .setConnectTimeout(connectTimeout)
					// .setKeepAlive(keepAlive)
					// .setKeepAliveStrategy(keepAliveStrategy)
					// .setMaxConnections(maxConnections)
					// .setMaxConnectionsPerHost(maxConnectionsPerHost)
					// .setFollowRedirect(followRedirect)
					// .setMaxRedirects(maxRedirects)
					// .setPooledConnectionIdleTimeout(pooledConnectionIdleTimeout)
					// .setReadTimeout(readTimeout)
					// .setSoLinger(soLinger)
					// .addIOExceptionFilter(ioExceptionFilter)  allow retry on I/O exceptions
					// .addResponseFilter(responseFilter) allow retry on certain HTTP statuses (or no HTTP status)
					// .setEnabledProtocols(enabledProtocols)
					// .setEnabledCipherSuites(enabledCipherSuites)
					
					// .setDisableHttpsEndpointIdentificationAlgorithm(disableHttpsEndpointIdentificationAlgorithm)
					// .setFilterInsecureCipherSuites(filterInsecureCipherSuites)
					// .setHandshakeTimeout(handshakeTimeout)
					// .setSslSessionTimeout(sslSessionTimeout)
					// .setStrict302Handling(strict302Handling)
					// .setUseInsecureTrustManager(useInsecureTrustManager)
					// .setUseNativeTransport(useNativeTransport)
					.setResponseBodyPartFactory(ResponseBodyPartFactory.LAZY));
			} catch (Exception ex) {
//				NeuronApplication.logError(LOG, "Exception creating HTTP client", ex);
				initPromise.setFailure(ex);
				return;
			}
			initPromise.setSuccess((Void) null);
		});
	}
	
	private boolean startRequest() {
		m_disconnectLock.readLock().lock();
		try {
			if (m_disconnected) {
				return false;
			}
			m_numOutstandingRequests.incrementAndGet();
		} finally {
			m_disconnectLock.readLock().unlock();
		}
		return true;
	}
	
	private void endRequest() {
		if (m_numOutstandingRequests.decrementAndGet() == 0) {
			final boolean callPromise;
			m_disconnectLock.writeLock().lock();
			try {
				if (m_disconnecting) {
					m_disconnected = true;
					callPromise = true;
				} else {
					callPromise = false;
				}
			} finally {
				m_disconnectLock.writeLock().unlock();
			}
			if (callPromise) {
				m_disconnectedPromise.setSuccess((Void)null);
			}
		}
	}

	@Override
	public void connectResources() {
		DuplexMessageQueueSystem.defineQueue("Execute", ObjectConfigBuilder.emptyConfig(), (IMessageQueueSubmission context) -> {
			final HTTPClientNeuronRequest nReq = (HTTPClientNeuronRequest)context.startProcessing();
			if (nReq == null) {
				return;
			}
			if (!startRequest()) {
				context.cancelProcessing();
				return;
			}
			final BoundRequestBuilder req;
			try {
				req = m_client.prepareGet(nReq.getURL());
				
				req.setMethod(nReq.getMethod());
				if (nReq.getQueryParams() != null) {
					for(HTTPClientNeuronRequest.NameValue nv :  nReq.getQueryParams()) {
						req.addQueryParam(nv.name, nv.value);
					}
				}
				if (nReq.getFormParams() != null) {
					for(HTTPClientNeuronRequest.NameValue nv :  nReq.getFormParams()) {
						req.addFormParam(nv.name, nv.value);
					}
				} else if (nReq.getBodyData() != null) {
					req.setBody(nReq.getBodyData());
				}
				if (nReq.getHeaders() != null) {
					for(HTTPClientNeuronRequest.NameValue nv :  nReq.getHeaders()) {
						req.addHeader(nv.name, nv.value);
					}
				}
				if (nReq.followRedirect()  != null) {
					req.setFollowRedirect(nReq.followRedirect());
				}
				if (nReq.readTimeout() != null) {
					req.setReadTimeout(nReq.readTimeout());
				}
				if (nReq.requestTimeout() != null) {
					req.setRequestTimeout(nReq.requestTimeout());
				}
				//req.setRealm(realm)
				//req.setSignatureCalculator(signatureCalculator);
				
				if (nReq.getResponseBodyOutputFile() != null) {
					final FileDownloadRequestHandler handler = new FileDownloadRequestHandler(context, nReq.getResponseBodyOutputFile());
					if (!handler.openOutputFile()) {
						return;
					}
					// I am trusting that if req.execute throws an exception it is not in a bad state.  If it catches an
					// exception I am expecting it to call the future it returns.  If it allows an exception to be thrown
					// I trust that it means that it will not put anything into the future
					handler.setResponseFuture(req.execute(handler));
				} else {
					final RequestHandler handler = new RequestHandler(context);
					// I am trusting that if req.execute throws an exception it is not in a bad state.  If it catches an
					// exception I am expecting it to call the future it returns.  If it allows an exception to be thrown
					// I trust that it means that it will not put anything into the future
					handler.setResponseFuture(req.execute(handler));
				}
			} catch(Exception ex) {
				LOG.error("Unexpected exception", ex);
				context.setAsProcessed(new HTTPClientNeuronResponse("Exception processing request parameters, see log for details"));
				endRequest();
				return;
			}
		});

	}

	@Override
	public void deinit(Promise<Void> promise) {
		if (m_client == null) {
			promise.setSuccess((Void) null);

		} else {
			NeuronApplication.getTaskPool().submit(() -> {
				try {
					m_client.close();
				} catch (Exception ex) {
					LOG.info("Exception on close of client, can probably ignore this", ex);
				}
				promise.setSuccess((Void) null);
			});
		}
	}

	@Override
	public long deinitTimeoutInMS() {
		// TODO need to wait until all outstanding requests are done <<-----------------------------------------------------------------------------------------------------------------------------
		return super.deinitTimeoutInMS();
	}

	private class RequestHandler extends AsyncCompletionHandlerBase implements Runnable {
		private final int m_requestId = m_requestNum.incrementAndGet();
		private final CompositeByteBuf m_responseBuf = NeuronApplication.allocateCompositeBuffer();
		private final IMessageQueueSubmission m_context;
		private ListenableFuture<Response> m_responseFuture;
		
		RequestHandler(IMessageQueueSubmission context) {
			m_context = context;
		}
		
		public void setResponseFuture(ListenableFuture<Response> responseFuture) {
			m_responseFuture = responseFuture;
			m_responseFuture.addListener(this, NeuronApplication.getTaskPool());
		}

		/**
		 * Called when the request is completed from the response future
		 */
		@Override
		public void run() {
			if (LOG.isDebugEnabled()) {
				LOG.debug("[{}] Request complete", m_requestId);
			}
			try {
				Response r = m_responseFuture.get();
				// r could be null if there is no Http status (any kind of protocol failure)
				if (r == null) {
					final HTTPClientNeuronResponse nResponse = new HTTPClientNeuronResponse("A protocol failure or I/O exception occurred during the processing of the request, turn on debug logging for " + HTTPClientNeuron.class.getCanonicalName() + " to get the details");
					m_context.setAsProcessed(nResponse);
				} else {
					final HTTPClientNeuronResponse nResponse = new HTTPClientNeuronResponse();
					nResponse.setStatusCode(r.getStatusCode());
					nResponse.setStatusText(r.getStatusText());
					nResponse.setResponseData(m_responseBuf); // The response owns the responsibility of cloning or retaining the buffer
					m_context.setAsProcessed(nResponse);
				}
			} catch (Exception ex) {
				LOG.error("Unexpected exception", ex);
				m_context.setAsProcessed(new HTTPClientNeuronResponse("Exception processing response data, see log for details"));
			}
			m_responseBuf.release();
			endRequest();
		}

		@Override
		public State onBodyPartReceived(HttpResponseBodyPart content) throws Exception {
			m_responseBuf.addComponent(true, ((LazyResponseBodyPart)content).getBuf().retain());
			return State.CONTINUE;
		}

		@Override
		public void onThrowable(Throwable t) {
			// TODO Need to do what with this?
			// Is this returned in the request future?
			LOG.info(t.getMessage(), t);
		}

	}

	private static final Set<OpenOption> FILE_DOWNLOAD_OPTIONS = new HashSet<>(Arrays.asList(StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE));
   private static final FileAttribute<?>[] NO_ATTRIBUTES = new FileAttribute[0];
   
	private final class FileDownloadRequestHandler extends AsyncCompletionHandlerBase implements Runnable {
		private final int m_requestId = m_requestNum.incrementAndGet();
		private final Queue<ByteBuf> s = PlatformDependent.newSpscQueue();
		private final Worker m_worker = new Worker();
		private final IMessageQueueSubmission m_context;
		private final File m_outputFile;
		private ListenableFuture<Response> m_responseFuture;
		private volatile boolean m_responseComplete;
		private boolean m_workerDone;
		private AsynchronousFileChannel m_fileChannel;
		private volatile boolean m_ioOperationPending;
		private long m_currentPosition;
		
		FileDownloadRequestHandler(IMessageQueueSubmission context, File outputFile) {
			m_context = context;
			m_outputFile = outputFile;
		}
		
		public boolean openOutputFile() {
			try {
				m_fileChannel = AsynchronousFileChannel.open(m_outputFile.toPath(), FILE_DOWNLOAD_OPTIONS, NeuronApplication.getTaskPool(), NO_ATTRIBUTES);
				return true;
			} catch(Exception ex) {
				m_context.setAsProcessed(new HTTPClientNeuronResponse("Exception opening file: " + ex.getMessage()));
				endRequest();
				return false;
			}
		}
		
		public void setResponseFuture(ListenableFuture<Response> responseFuture) {
			m_responseFuture = responseFuture;
			m_responseFuture.addListener(this, NeuronApplication.getTaskPool());
		}

		/**
		 * Called when the request is completed from the response future
		 */
		@Override
		public void run() {
			if (LOG.isDebugEnabled()) {
				LOG.debug("[{}] Request complete", m_requestId);
			}
			m_responseComplete = true;
			m_worker.requestMoreWork();
		}

		@Override
		public State onBodyPartReceived(HttpResponseBodyPart content) throws Exception {
			if (!m_workerDone) {
				ByteBuf buf = ((LazyResponseBodyPart)content).getBuf();
				if (buf.readableBytes() > 0) {
					s.add( buf.retain() );
					m_worker.requestMoreWork();
				}
			}
			return State.CONTINUE;
		}

		@Override
		public void onThrowable(Throwable t) {
			// TODO Need to do what with this?
			// Is this returned in the request future?
			LOG.debug(t.getMessage(), t);
		}

		private final class Worker extends PerpetualWork implements CompletionHandler<Integer,ByteBuffer> {

			@Override
			protected void _doWork() {
				if (m_ioOperationPending) {
					return;
				}
				if (m_workerDone) {
					clearQueue();
					return;
				}
				m_ioOperationPending = true;
				startNextIOOperation();
			}
			
			public void clearQueue() {
				while(true) {
					ByteBuf buf = s.poll();
					if (buf == null) {
						break;
					}
					buf.release();
				}
			}
			
			private void startNextIOOperation() {
				ByteBuf buf = s.peek();
				if (buf == null) {
					if (m_responseComplete) {
						m_workerDone = true;
						try {
							Response r = m_responseFuture.get();
							// r could be null if there is no Http status (any kind of protocol failure)
							if (r == null) {
								final HTTPClientNeuronResponse nResponse = new HTTPClientNeuronResponse("A protocol failure or I/O exception occurred during the processing of the request, turn on debug logging for " + HTTPClientNeuron.class.getCanonicalName() + " to get the details");
								m_context.setAsProcessed(nResponse);
							} else {
								final HTTPClientNeuronResponse nResponse = new HTTPClientNeuronResponse();
								nResponse.setStatusCode(r.getStatusCode());
								nResponse.setStatusText(r.getStatusText());
								m_context.setAsProcessed(nResponse);
							}
						} catch (Exception ex) {
							LOG.error("Unexpected exception", ex);
							m_context.setAsProcessed(new HTTPClientNeuronResponse("Exception processing response data, see log for details"));
						}
						if (LOG.isDebugEnabled()) {
							LOG.debug("[{}] endRequest", m_requestId);
						}
						endRequest();
					}
					m_ioOperationPending = false;
					m_worker.requestMoreWork();
					return;
				}
				ByteBuffer bbuf = buf.nioBuffer();
				if (LOG.isDebugEnabled()) {
					LOG.debug("[{}] Start write of {}", m_requestId, bbuf);
				}
				m_fileChannel.write(bbuf, m_currentPosition, bbuf, this);
			}

			@Override
			public void completed(Integer result, ByteBuffer bbuf) {
				m_currentPosition += result;
				if (bbuf.remaining() == 0) {
					if (LOG.isDebugEnabled()) {
						LOG.debug("[{}] Buffer write {} complete, pos={}", m_requestId, bbuf, m_currentPosition);
					}
					final ByteBuf buf = s.remove();
					buf.release();
					startNextIOOperation();
					return;
				}
				m_fileChannel.write(bbuf, m_currentPosition, bbuf, this);
			}

			@Override
			public void failed(Throwable exc, ByteBuffer attachment) {
				m_workerDone = true;
				m_ioOperationPending = false;
				LOG.error("Exception writing {}", m_outputFile, exc);
				final HTTPClientNeuronResponse nResponse = new HTTPClientNeuronResponse("An I/O exception occurred writing " + m_outputFile.toString() + ", see log for details");
				m_context.setAsProcessed(nResponse);
				// Kick off the worker task again to clear the queue
				requestMoreWork();
			}
			
		}
	}
}
