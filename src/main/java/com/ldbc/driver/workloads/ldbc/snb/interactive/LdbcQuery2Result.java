package com.ldbc.driver.workloads.ldbc.snb.interactive;

import java.util.Objects;

public class LdbcQuery2Result {
    private final long personId;
    private final String personFirstName;
    private final String personLastName;
    private final long messageId;
    private final String messageContent;
    private final long messageCreationDate;

    public LdbcQuery2Result(long personId, String personFirstName, String personLastName, long messageId, String messageContent, long messageCreationDate) {
        this.personId = personId;
        this.personFirstName = personFirstName;
        this.personLastName = personLastName;
        this.messageId = messageId;
        this.messageContent = messageContent;
        this.messageCreationDate = messageCreationDate;
    }

    public long personId() {
        return personId;
    }

    public String personFirstName() {
        return personFirstName;
    }

    public String personLastName() {
        return personLastName;
    }

    public long messageId() {
        return messageId;
    }

    public String messageContent() {
        return messageContent;
    }

    public long messageCreationDate() {
        return messageCreationDate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcQuery2Result result = (LdbcQuery2Result) o;

        if (personId != result.personId) return false;
        if (messageCreationDate != result.messageCreationDate) return false;
        if (messageId != result.messageId) return false;
        if (!Objects.equals(personFirstName, result.personFirstName))
            return false;
        if (!Objects.equals(personLastName, result.personLastName))
            return false;
        if (!Objects.equals(messageContent, result.messageContent))
            return false;

        return true;
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
