package com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive;

import org.eclipse.rdf4j.model.IRI;

import java.util.Objects;

public class LdbcQuery11Result {
	private final IRI personId;
	private final String personFirstName;
	private final String personLastName;
	private final String organizationName;
	private final int organizationWorkFromYear;

	public LdbcQuery11Result(IRI personId, String personFirstName, String personLastName, String organizationName, int organizationWorkFromYear) {
		this.personId = personId;
		this.personFirstName = personFirstName;
		this.personLastName = personLastName;
		this.organizationName = organizationName;
		this.organizationWorkFromYear = organizationWorkFromYear;
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

	public String organizationName() {
		return organizationName;
	}

	public int organizationWorkFromYear() {
		return organizationWorkFromYear;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		LdbcQuery11Result that = (LdbcQuery11Result) o;

		if (organizationWorkFromYear != that.organizationWorkFromYear) return false;
		if (!Objects.equals(personId, that.personId)) return false;
		if (!Objects.equals(organizationName, that.organizationName))
			return false;
		if (!Objects.equals(personFirstName, that.personFirstName))
			return false;
		return Objects.equals(personLastName, that.personLastName);
	}

	@Override
	public String toString() {
		return "LdbcQuery11Result{" +
				"personId=" + personId +
				", personFirstName='" + personFirstName + '\'' +
				", personLastName='" + personLastName + '\'' +
				", organizationName='" + organizationName + '\'' +
				", organizationWorkFromYear=" + organizationWorkFromYear +
				'}';
	}
}