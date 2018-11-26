package com.neuron.core.http;

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
import com.neuron.core.ObjectConfigBuilder.ObjectConfig;

import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.concurrent.Promise;

public class HTTPClientNeuron extends DefaultNeuronInstanceBase implements INeuronInitialization {
	private static final Logger LOG = LogManager.getLogger(HTTPClientNeuron.class);
	private final ObjectConfig m_config;
	private AsyncHttpClient m_client;

	public HTTPClientNeuron(NeuronRef instanceRef, ObjectConfig config) {
		super(instanceRef);
		m_config = config;
	}

	@Override
	public void init(Promise<Void> initPromise) {
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
				initPromise.tryFailure(ex);
				return;
			}
			initPromise.setSuccess((Void) null);
		});
	}

	@Override
	public void connectResources() {
		DuplexMessageQueueSystem.defineQueue("Execute", ObjectConfigBuilder.emptyConfig(), (IMessageQueueSubmission context) -> {
			final HTTPClientNeuronRequest nReq = (HTTPClientNeuronRequest)context.startProcessing();
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
				
				final RequestHandler handler = new RequestHandler(context);
				// I am trusting that if req.execute throws an exception it is not in a bad state.  If it catches an
				// exception I am expecting it to call the future it returns.  If it allows an exception to be thrown
				// I trust that it means that it will not put anything into the future
				handler.setResponseFuture(req.execute(handler));
			} catch(Exception ex) {
				LOG.error("Unexpected exception", ex);
				final HTTPClientNeuronResponse nResponse = new HTTPClientNeuronResponse(false);
				context.setAsProcessed(nResponse);
				return;
			}
		});

	}

	@Override
	public void deinit(Promise<Void> promise) {
		if (m_client == null) {
			promise.setSuccess((Void) null);

		} else {
			// TODO need to wait until all outstanding requests are done <<-----------------------------------------------------------------------------------------------------------------------------
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

	private class RequestHandler extends AsyncCompletionHandlerBase implements Runnable {
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
			try {
				Response r = m_responseFuture.get();
				// r could be null if there is no Http status (any kind of protocol failure)
				if (r == null) {
					final HTTPClientNeuronResponse nResponse = new HTTPClientNeuronResponse(false);
					m_context.setAsProcessed(nResponse);
				} else {
					final HTTPClientNeuronResponse nResponse = new HTTPClientNeuronResponse(true);
					nResponse.setStatusCode(r.getStatusCode());
					nResponse.setStatusText(r.getStatusText());
					nResponse.setResponseData(m_responseBuf); // The response owns the responsibility of cloning or retaining the buffer
					m_context.setAsProcessed(nResponse);
				}
			} catch (Exception ex) {
				LOG.error("Unexpected exception", ex);
				m_context.setAsProcessed(new HTTPClientNeuronResponse(false));
			}
			m_responseBuf.release();
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
}
