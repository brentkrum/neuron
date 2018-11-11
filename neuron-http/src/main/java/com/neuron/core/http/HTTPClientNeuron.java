package com.neuron.core.http;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Dsl;

import com.neuron.core.DefaultNeuronInstanceBase;
import com.neuron.core.INeuronInitialization;
import com.neuron.core.MessagePipeSystem;
import com.neuron.core.NeuronApplication;
import com.neuron.core.NeuronRef;
import com.neuron.core.ObjectConfigBuilder;
import com.neuron.core.ObjectConfigBuilder.ObjectConfigObjectBuilder;

import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.Promise;

public class HTTPClientNeuron extends DefaultNeuronInstanceBase implements INeuronInitialization
{
	private static final Logger LOG = LogManager.getLogger(HTTPClientNeuron.class);
	private AsyncHttpClient m_client;
	
	public HTTPClientNeuron(NeuronRef instanceRef) {
		super(instanceRef);
	}

	@Override
	public void connectResources() {
		DuplexMessageQueueSystem.defineQueue("Execute", ObjectConfigBuilder.emptyConfig(), () -> {
			
		});
		
	}

	@Override
	public void init(Promise<Void> initPromise) {
		NeuronApplication.getTaskPool().submit(() -> {
			try {
				m_client = Dsl.asyncHttpClient(Dsl.config()
					.setEventLoopGroup( NeuronApplication.getIOPool() )
					.setAllocator(PooledByteBufAllocator.DEFAULT)
					//.setConnectionTtl(connectionTtl)
					//.setConnectTimeout(connectTimeout)
					// .setDisableHttpsEndpointIdentificationAlgorithm(disableHttpsEndpointIdentificationAlgorithm) ?
					// .setEnabledCipherSuites(enabledCipherSuites)
					//.setFilterInsecureCipherSuites(filterInsecureCipherSuites)
					// .setEnabledProtocols(enabledProtocols)
					//.setHandshakeTimeout(handshakeTimeout)
					//.setKeepAlive(keepAlive)
					//.setKeepAliveStrategy(keepAliveStrategy)
					//.setMaxConnections(maxConnections)
					//.setMaxConnectionsPerHost(maxConnectionsPerHost)
					//.setFollowRedirect(followRedirect)
					//.setMaxRedirects(maxRedirects)
					//.setPooledConnectionIdleTimeout(pooledConnectionIdleTimeout)
					//.setReadTimeout(readTimeout)
					//.setSoLinger(soLinger)
					//.setSslSessionTimeout(sslSessionTimeout)
					//.setStrict302Handling(strict302Handling)
					//.setTcpNoDelay(tcpNoDelay)
					//.setUseInsecureTrustManager(useInsecureTrustManager)
					//.setUseNativeTransport(useNativeTransport)
					//.setUserAgent(userAgent)
				);
			} catch(Exception ex) {
//				NeuronApplication.logError(LOG, "Exception creating HTTP client", ex);
				initPromise.tryFailure(ex);
				return;
			}
			initPromise.setSuccess((Void)null);
		});
	}
	
	@Override
	public void deinit(Promise<Void> promise) {
		if (m_client == null) {
			promise.setSuccess((Void)null);
			
		} else {
			NeuronApplication.getTaskPool().submit(() -> {
				try {
					m_client.close();
				} catch(Exception ex) {
					LOG.info("Exception on close of client, can probably ignore this", ex);
				}
				promise.setSuccess((Void)null);
			});
		}
	}
	
}
