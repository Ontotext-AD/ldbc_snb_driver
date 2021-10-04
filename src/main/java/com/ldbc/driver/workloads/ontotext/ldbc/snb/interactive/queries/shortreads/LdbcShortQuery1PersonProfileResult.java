package com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive.queries.shortreads;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;

import java.util.Objects;

public class LdbcShortQuery1PersonProfileResult {

	private final String firstName;
	private final String lastName;
	private final Literal birthday;
	private final String locationIp;
	private final String browserUsed;
	private final IRI cityId;
	private final String gender;
	private final Literal creationDate;

	public LdbcShortQuery1PersonProfileResult(String firstName,
											  String lastName,
											  Literal birthday,
											  String locationIp,
											  String browserUsed,
											  IRI cityId,
											  String gender,
											  Literal creationDate) {
		this.firstName = firstName;
		this.lastName = lastName;
		this.birthday = birthday;
		this.locationIp = locationIp;
		this.browserUsed = browserUsed;
		this.cityId = cityId;
		this.gender = gender;
		this.creationDate = creationDate;
	}

	public String firstName() {
		return firstName;
	}

	public String lastName() {
		return lastName;
	}

	public Literal birthday() {
		return birthday;
	}

	public String locationIp() {
		return locationIp;
	}

	public String browserUsed() {
		return browserUsed;
	}

	public IRI cityId() {
		return cityId;
	}

	public String gender() {
		return gender;
	}

	public Literal creationDate() {
		return creationDate;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		LdbcShortQuery1PersonProfileResult that = (LdbcShortQuery1PersonProfileResult) o;
		return Objects.equals(firstName, that.firstName)
				&& Objects.equals(lastName, that.lastName)
				&& Objects.equals(birthday, that.birthday)
				&& Objects.equals(locationIp, that.locationIp)
				&& Objects.equals(browserUsed, that.browserUsed)
				&& Objects.equals(cityId, that.cityId)
				&& Objects.equals(gender, that.gender)
				&& Objects.equals(creationDate, that.creationDate);
	}

	@Override
	public String toString() {
		return "LdbcShortQuery1PersonProfileResult{" +
				"firstName='" + firstName + '\'' +
				", lastName='" + lastName + '\'' +
				", birthday=" + birthday +
				", locationIp='" + locationIp + '\'' +
				", browserUsed='" + browserUsed + '\'' +
				", cityId=" + cityId +
				", gender='" + gender + '\'' +
				", creationDate=" + creationDate +
				'}';
	}
}
