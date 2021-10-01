package com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive.queries.longreads;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;

import java.util.Objects;

public class LdbcQuery2Result {
    private final IRI personId;
    private final String personFirstName;
    private final String personLastName;
    private final IRI messageId;
    private final String messageContent;
    private final Literal messageCreationDate;

    public LdbcQuery2Result(IRI personId, String personFirstName, String personLastName, IRI messageId, String messageContent, Literal messageCreationDate) {
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

        LdbcQuery2Result result = (LdbcQuery2Result) o;

        if (!Objects.equals(personId, result.personId)) return false;
        if (!Objects.equals(messageCreationDate, result.messageCreationDate)) return false;
        if (!Objects.equals(messageId, result.messageId)) return false;
        if (!Objects.equals(personFirstName, result.personFirstName))
            return false;
        if (!Objects.equals(personLastName, result.personLastName))
            return false;
        return Objects.equals(messageContent, result.messageContent);
    }

    @Override
    public String toString() {
        return "LdbcQuery2Result{" +
                "personId=" + personId +
                ", personFirstName='" + personFirstName + '\'' +
                ", personLastName='" + personLastName + '\'' +
                ", messageId=" + messageId +
                ", messageContent='" + messageContent + '\'' +
                ", messageCreationDate=" + messageCreationDate +
                '}';
    }
}
