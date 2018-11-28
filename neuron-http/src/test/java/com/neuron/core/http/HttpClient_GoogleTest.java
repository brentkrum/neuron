package com.neuron.core.http;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.file.Files;

import org.asynchttpclient.netty.util.ByteBufUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.neuron.core.DefaultNeuronInstanceBase;
import com.neuron.core.DuplexMessageQueueSystem;
import com.neuron.core.INeuronInitialization;
import com.neuron.core.NeuronApplication;
import com.neuron.core.NeuronApplicationBootstrap;
import com.neuron.core.NeuronRef;
import com.neuron.core.ObjectConfigBuilder;
import com.neuron.core.TemplateRef;
import com.neuron.core.ObjectConfigBuilder.ObjectConfig;
import com.neuron.core.test.DefaultTestNeuronTemplateBase;
import com.neuron.core.test.NeuronStateTestUtils;
import com.neuron.core.test.TemplateStateTestUtils;

import io.netty.util.concurrent.Promise;

public class HttpClient_GoogleTest {
	
	@BeforeAll
	public static void init() {
//		System.setProperty("logger.com.neuron.core.StatusSystem", "DEBUG");
//		System.setProperty("logger.org.asynchttpclient", "DEBUG");
//		System.setProperty("logger.com.neuron.core.http.HTTPClientNeuron", "DEBUG");
		System.setProperty("com.neuron.core.NeuronThreadContext.leakDetection", "true");
		System.setProperty("io.netty.leakDetection.level", "PARANOID");
		System.setProperty("io.netty.leakDetection.targetRecords", "1");
		
		NeuronApplicationBootstrap.bootstrapUnitTest("test-log4j2.xml", new String[0]).run();
		if (!TemplateStateTestUtils.registerAndBringOnline("HTTPClientNeuronTemplate", HTTPClientNeuronTemplate.class).awaitUninterruptibly(1000)) {
			throw new RuntimeException("HTTPClientNeuronTemplate did not go online");
		}
		if (!NeuronStateTestUtils.bringOnline("HTTPClientNeuronTemplate", "HttpClientNeuron", ObjectConfigBuilder.emptyConfig()).awaitUninterruptibly(5000)) {
			throw new RuntimeException("HttpClientNeuron did not go online");
		}
	}
	
	@AfterAll
	public static void deinit() {
		NeuronApplication.shutdown();
		(new File("./test.html")).delete();
	}

	private static Promise<Void> m_testFuture;
	private static String m_testBody;
	private static Promise<Void> m_testFuture2;
	private static String m_testFileContents;
	
	@Test
	public void testHittingGoogleDotCom() {
		m_testFuture = NeuronApplication.newPromise();
		m_testFuture2 = NeuronApplication.newPromise();
		assertTrue(TemplateStateTestUtils.registerAndBringOnline("HitGoogleTemplate", HitGoogleTemplate.class).awaitUninterruptibly(1000));
		assertTrue(NeuronStateTestUtils.bringOnline("HitGoogleTemplate", "HitGoogleTest", ObjectConfigBuilder.emptyConfig()).awaitUninterruptibly(5000));
		
		assertTrue(m_testFuture.awaitUninterruptibly(5000));
		assertTrue(m_testFuture.isSuccess());
		assertTrue(m_testFuture2.awaitUninterruptibly(5000));
		assertTrue(m_testFuture2.isSuccess());
		Assertions.assertTrue(m_testBody.contains("<html"));
		Assertions.assertTrue(m_testBody.contains("</html>"));
		Assertions.assertTrue(m_testFileContents.contains("<html"));
		Assertions.assertTrue(m_testFileContents.contains("</html>"));
	}
	
	public static class HitGoogleTemplate extends DefaultTestNeuronTemplateBase {
		
		public HitGoogleTemplate(TemplateRef ref)
		{
			super(ref);
		}

		@Override
		public INeuronInitialization createNeuron(NeuronRef ref, ObjectConfig config) {
			return new MyNeuron(ref);
		}
		
		private static class MyNeuron extends DefaultNeuronInstanceBase {
			public MyNeuron(NeuronRef instanceRef) {
				super(instanceRef);
			}

			@Override
			public void nowOnline() {
				HTTPClientNeuronRequest req = new HTTPClientNeuronRequest("http://www.google.com");
				DuplexMessageQueueSystem.submitToQueue("HttpClientNeuron", "Execute", req, (httpRequest, httpResponse) -> {
					HTTPClientNeuronResponse res = (HTTPClientNeuronResponse)httpResponse;
					if (!res.getProtocolSuccess()) {
						m_testFuture.setFailure(new RuntimeException("Did not have a successful HTTP conversation"));
						return;
					}
					System.out.println(res.getHTTPStatusCode() + " " + res.getHTTPStatusText());
					if (res.getResponseData() != null) {
						m_testBody = ByteBufUtils.byteBuf2String(Charset.forName("UTF-8"), res.getResponseData());
						System.out.println(m_testBody);
						m_testFuture.setSuccess((Void)null);
					} else {
						System.out.println(">>> no body data <<<");
						m_testFuture.setFailure(new RuntimeException("No body data"));
					}
				});
				
				File outputFile = new File("./test.html");
				HTTPClientNeuronRequest req2 = new HTTPClientNeuronRequest("http://www.google.com");
				req2.setResponseBodyOutputFile(outputFile);
				DuplexMessageQueueSystem.submitToQueue("HttpClientNeuron", "Execute", req2, (httpRequest, httpResponse) -> {
					HTTPClientNeuronResponse res = (HTTPClientNeuronResponse)httpResponse;
					if (!res.getProtocolSuccess()) {
						m_testFuture.setFailure(new RuntimeException("Did not have a successful HTTP conversation"));
						return;
					}
					System.out.println(res.getHTTPStatusCode() + " " + res.getHTTPStatusText());
					try {
						m_testFileContents = new String(Files.readAllBytes(outputFile.toPath()));
						System.out.println(m_testFileContents);
						m_testFuture2.setSuccess((Void)null);
					} catch(Exception ex) {
						ex.printStackTrace();
						m_testFuture2.setFailure(ex);
					}
				});
			}
			
		}
	}

}
