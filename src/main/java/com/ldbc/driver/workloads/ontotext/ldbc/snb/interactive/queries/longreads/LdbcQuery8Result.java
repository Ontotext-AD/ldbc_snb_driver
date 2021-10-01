package com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive.queries.longreads;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;

import java.util.Objects;

public class LdbcQuery8Result {
    private final IRI personId;
    private final String personFirstName;
    private final String personLastName;
    private final Literal commentCreationDate;
    private final IRI commentId;
    private final String commentContent;

    public LdbcQuery8Result(IRI personId, String personFirstName, String personLastName, Literal commentCreationDate, IRI commentId, String commentContent) {
        this.personId = personId;
        this.personFirstName = personFirstName;
        this.personLastName = personLastName;
        this.commentCreationDate = commentCreationDate;
        this.commentId = commentId;
        this.commentContent = commentContent;
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

    public Literal commentCreationDate() {
        return commentCreationDate;
    }

    public IRI commentId() {
        return commentId;
    }

    public String commentContent() {
        return commentContent;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcQuery8Result that = (LdbcQuery8Result) o;

        if (!Objects.equals(commentCreationDate, that.commentCreationDate)) return false;
        if (!Objects.equals(commentId, that.commentId)) return false;
        if (!Objects.equals(personId, that.personId)) return false;
        if (!Objects.equals(commentContent, that.commentContent))
            return false;
        if (!Objects.equals(personFirstName, that.personFirstName))
            return false;
        return Objects.equals(personLastName, that.personLastName);
    }

    @Override
    public String toString() {
        return "LdbcQuery8Result{" +
                "personId=" + personId +
                ", personFirstName='" + personFirstName + '\'' +
                ", personLastName='" + personLastName + '\'' +
                ", commentCreationDate=" + commentCreationDate +
                ", commentId=" + commentId +
                ", commentContent='" + commentContent + '\'' +
                '}';
    }
}