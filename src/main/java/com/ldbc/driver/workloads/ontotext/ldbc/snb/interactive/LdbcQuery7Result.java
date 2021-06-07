package com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;

import java.util.Objects;

public class LdbcQuery7Result {
	private final IRI personId;
	private final String personFirstName;
	private final String personLastName;
	private final Literal likeCreationDate;
	private final IRI messageId;
	private final String messageContent;
	private final int minutesLatency;
	private final boolean isNew;

	public LdbcQuery7Result(IRI personId, String personFirstName, String personLastName, Literal likeCreationDate, IRI messageId, String messageContent, int minutesLatency, boolean isNew) {
		this.personId = personId;
		this.personFirstName = personFirstName;
		this.personLastName = personLastName;
		this.likeCreationDate = likeCreationDate;
		this.messageId = messageId;
		this.messageContent = messageContent;
		this.minutesLatency = minutesLatency;
		this.isNew = isNew;
	}

	public IRI personId() {
		return personId;
	}

	public String personFirstName() {
		return personFirstName;
	}

	public String personLastName() {
		return personLastName;
	}

	public Literal likeCreationDate() {
		return likeCreationDate;
	}

	public IRI messageId() {
		return messageId;
	}

	public String messageContent() {
		return messageContent;
	}

	public int minutesLatency() {
		return minutesLatency;
	}

	public boolean isNew() {
		return isNew;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		LdbcQuery7Result that = (LdbcQuery7Result) o;

		if (!Objects.equals(messageId, that.messageId)) return false;
		if (isNew != that.isNew) return false;
		if (!Objects.equals(likeCreationDate, that.likeCreationDate)) return false;
		if (minutesLatency != that.minutesLatency) return false;
		if (!Objects.equals(personId, that.personId)) return false;
		if (!Objects.equals(messageContent, that.messageContent))
			return false;
		if (!Objects.equals(personFirstName, that.personFirstName))
			return false;
		return Objects.equals(personLastName, that.personLastName);
	}

	@Override
	public String toString() {
		return "LdbcQuery7Result{" +
				"personId=" + personId +
				", personFirstName='" + personFirstName + '\'' +
				", personLastName='" + personLastName + '\'' +
				", likeCreationDate=" + likeCreationDate +
				", messageId=" + messageId +
				", messageContent='" + messageContent + '\'' +
				", minutesLatency=" + minutesLatency +
				", isNew=" + isNew +
				'}';
	}
}
