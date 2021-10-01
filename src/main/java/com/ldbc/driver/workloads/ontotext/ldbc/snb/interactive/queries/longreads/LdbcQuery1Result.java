package com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive.queries.longreads;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;

import java.util.Objects;

public class LdbcQuery1Result {
	private final IRI friendId;
	private final String friendLastName;
	private final int distanceFromPerson;
	private final Literal friendBirthday;
	private final Literal friendCreationDate;
	private final String friendGender;
	private final String friendBrowserUsed;
	private final String friendLocationIp;
	private final String friendEmails;
	private final String friendLanguages;
	private final String friendCityName;
	private final String friendUniversities;
	private final String friendCompanies;

	public LdbcQuery1Result(
			IRI friendId,
			String friendLastName,
			int distanceFromPerson,
			Literal friendBirthday,
			Literal friendCreationDate,
			String friendGender,
			String friendBrowserUsed,
			String friendLocationIp,
			String friendEmails,
			String friendLanguages,
			String friendCityName,
			String friendUniversities,
			String friendCompanies) {
		this.friendId = friendId;
		this.friendLastName = friendLastName;
		this.distanceFromPerson = distanceFromPerson;
		this.friendBirthday = friendBirthday;
		this.friendCreationDate = friendCreationDate;
		this.friendGender = friendGender;
		this.friendBrowserUsed = friendBrowserUsed;
		this.friendLocationIp = friendLocationIp;
		this.friendEmails = friendEmails;
		this.friendLanguages = friendLanguages;
		this.friendCityName = friendCityName;
		this.friendUniversities = friendUniversities;
		this.friendCompanies = friendCompanies;
	}

	public IRI friendId() {
		return friendId;
	}

	public String friendLastName() {
		return friendLastName;
	}

	public int distanceFromPerson() {
		return distanceFromPerson;
	}

	public Literal friendBirthday() {
		return friendBirthday;
	}

	public Literal friendCreationDate() {
		return friendCreationDate;
	}

	public String friendGender() {
		return friendGender;
	}

	public String friendBrowserUsed() {
		return friendBrowserUsed;
	}

	public String friendLocationIp() {
		return friendLocationIp;
	}

	public String friendEmails() {
		return friendEmails;
	}

	public String friendLanguages() {
		return friendLanguages;
	}

	public String friendCityName() {
		return friendCityName;
	}

	public String friendUniversities() {
		return friendUniversities;
	}

	public String friendCompanies() {
		return friendCompanies;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		LdbcQuery1Result other = (LdbcQuery1Result) o;

		if (distanceFromPerson != other.distanceFromPerson) return false;
		if (!Objects.equals(friendBirthday, other.friendBirthday)) return false;
		if (!Objects.equals(friendCreationDate, other.friendCreationDate)) return false;
		if (!Objects.equals(friendId, other.friendId)) return false;
		if (!Objects.equals(friendBrowserUsed, other.friendBrowserUsed))
			return false;
		if (!Objects.equals(friendCityName, other.friendCityName))
			return false;
		if (!Objects.equals(friendCompanies, other.friendCompanies))
			return false;
		if (!Objects.equals(friendEmails, other.friendEmails))
			return false;
		if (!Objects.equals(friendGender, other.friendGender))
			return false;
		if (!Objects.equals(friendLanguages, other.friendLanguages))
			return false;
		if (!Objects.equals(friendLastName, other.friendLastName))
			return false;
		if (!Objects.equals(friendLocationIp, other.friendLocationIp))
			return false;
		return Objects.equals(friendUniversities, other.friendUniversities);
	}

	@Override
	public String toString() {
		return "LdbcQuery1Result{" +
				"friendId=" + friendId +
				", friendLastName='" + friendLastName + '\'' +
				", distanceFromPerson=" + distanceFromPerson +
				", friendBirthday=" + friendBirthday +
				", friendCreationDate=" + friendCreationDate +
				", friendGender='" + friendGender + '\'' +
				", friendBrowserUsed='" + friendBrowserUsed + '\'' +
				", friendLocationIp='" + friendLocationIp + '\'' +
				", friendEmails=" + friendEmails +
				", friendLanguages=" + friendLanguages +
				", friendCityName='" + friendCityName + '\'' +
				", friendUniversities=" + friendUniversities +
				", friendCompanies=" + friendCompanies +
				'}';
	}
}
