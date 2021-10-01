package com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive.queries.longreads;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;

import java.util.Objects;

public class LdbcQuery9Result {
	private final IRI personId;
	private final String personFirstName;
	private final String personLastName;
	private final IRI messageId;
	private final String messageContent;
	private final Literal messageCreationDate;

	public LdbcQuery9Result(IRI personId, String personFirstName, String personLastName, IRI messageId, String messageContent, Literal messageCreationDate) {
		this.personId = personId;
		this.personFirstName = personFirstName;
		this.personLastName = personLastName;
		this.messageId = messageId;
		this.messageContent = messageContent;
		this.messageCreationDate = messageCreationDate;
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

	public IRI messageId() {
		return messageId;
	}

	public String messageContent() {
		return messageContent;
	}

	public Literal messageCreationDate() {
		return messageCreationDate;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		LdbcQuery9Result that = (LdbcQuery9Result) o;

		if (!Objects.equals(messageCreationDate, that.messageCreationDate)) return false;
		if (!Objects.equals(messageId, that.messageId)) return false;
		if (!Objects.equals(personId, that.personId)) return false;
		if (!Objects.equals(messageContent, that.messageContent))
			return false;
		if (!Objects.equals(personFirstName, that.personFirstName))
			return false;
		return Objects.equals(personLastName, that.personLastName);
	}

	@Override
	public String toString() {
		return "LdbcQuery9Result{" +
				"personId=" + personId +
				", personFirstName='" + personFirstName + '\'' +
				", personLastName='" + personLastName + '\'' +
				", messageId=" + messageId +
				", messageContent='" + messageContent + '\'' +
				", messageCreationDate=" + messageCreationDate +
				'}';
	}
}
