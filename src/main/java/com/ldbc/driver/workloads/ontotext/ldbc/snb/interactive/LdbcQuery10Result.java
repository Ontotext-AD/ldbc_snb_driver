package com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive;

import org.eclipse.rdf4j.model.IRI;

import java.util.Objects;

public class LdbcQuery10Result {
    private final IRI personId;
    private final String personFirstName;
    private final String personLastName;
    private final int commonInterestScore;
    private final String personGender;
    private final String personCityName;

    public LdbcQuery10Result(IRI personId, String personFirstName, String personLastName, int commonInterestScore, String personGender, String personCityName) {
        this.personId = personId;
        this.personFirstName = personFirstName;
        this.personLastName = personLastName;
        this.commonInterestScore = commonInterestScore;
        this.personGender = personGender;
        this.personCityName = personCityName;
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

    public String personGender() {
        return personGender;
    }

    public String personCityName() {
        return personCityName;
    }

    public int commonInterestScore() {
        return commonInterestScore;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcQuery10Result that = (LdbcQuery10Result) o;

        if (commonInterestScore != that.commonInterestScore) return false;
        if (!Objects.equals(personId, that.personId)) return false;
        if (!Objects.equals(personCityName, that.personCityName))
            return false;
        if (!Objects.equals(personFirstName, that.personFirstName))
            return false;
        if (!Objects.equals(personGender, that.personGender)) return false;
        return Objects.equals(personLastName, that.personLastName);
    }

    @Override
    public int hashCode() {
        int result = personId.hashCode();
        result = 31 * result + (personFirstName != null ? personFirstName.hashCode() : 0);
        result = 31 * result + (personLastName != null ? personLastName.hashCode() : 0);
        result = 31 * result + commonInterestScore;
        result = 31 * result + (personGender != null ? personGender.hashCode() : 0);
        result = 31 * result + (personCityName != null ? personCityName.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "LdbcQuery10Result{" +
                "personId=" + personId +
                ", personFirstName='" + personFirstName + '\'' +
                ", personLastName='" + personLastName + '\'' +
                ", personGender='" + personGender + '\'' +
                ", personCityName='" + personCityName + '\'' +
                ", commonInterestScore=" + commonInterestScore +
                '}';
    }
}
