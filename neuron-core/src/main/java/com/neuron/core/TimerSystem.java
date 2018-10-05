package com.neuron.core;

public class TimerSystem
{
	static void register() {
		NeuronApplication.register(new Registrant());
	}
//	
//	public Future<Object> startTimer(Object delivery, long delay, TimeUnit unit) {
//		Promise<Object> promise = NeuronApplication.getTaskPool().next().newPromise();
//		ScheduledFuture sf = NeuronApplication.getTaskPool().schedule(() -> {
//			promise.setSuccess(delivery);
//		}, delay, unit);
//		return promise;
//	}
	
	
	private static class Registrant implements INeuronApplicationSystem {

		@Override
		public String systemName()
		{
			return "TimerSystem";
		}
	}

}
