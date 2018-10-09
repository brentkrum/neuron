package com.neuron.core.test;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.neuron.core.StatusSystem;
import com.neuron.core.StatusSystem.CurrentHostStatus;
import com.neuron.core.StatusSystem.CurrentNeuronStatus;
import com.neuron.core.StatusSystem.CurrentTemplateStatus;

public final class TestUtils {
	private static final Logger LOG = LogManager.getLogger(TestUtils.class);
	private static final DateFormat m_dtFormatter = SimpleDateFormat.getDateTimeInstance(SimpleDateFormat.SHORT,SimpleDateFormat.SHORT);

	public static void printSystemStatuses() {
		final List<StatusSystem.CurrentStatus> statuses = StatusSystem.getCurrentStatus();
		final StringBuilder sb = new StringBuilder();
		for(StatusSystem.CurrentStatus status : statuses) {
			sb.append(status.timestamp).append(' ').append(m_dtFormatter.format(status.timestamp)).append(" [");
			if (status instanceof CurrentNeuronStatus) {
				sb.append(((CurrentNeuronStatus)status).neuronRef.logString());
			} else if (status instanceof CurrentTemplateStatus) {
				sb.append(((CurrentTemplateStatus)status).templateRef.logString());
			} else if (status instanceof CurrentHostStatus) {
				if (((CurrentHostStatus)status).isInbound) {
					sb.append("in:");
				} else {
					sb.append("out:");
				}
				sb.append(((CurrentHostStatus)status).hostAndPort);
			}
			sb.append("] ").append(status.status.toString()).append(": ").append(status.reasonText).append('\n');
		}
		LOG.info("Status:\n{}", sb);
	}
}
