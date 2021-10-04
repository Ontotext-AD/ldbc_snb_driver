package com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive.queries.writes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkloadConfiguration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.lang.String.format;

public class LdbcUpdate1AddPerson extends Operation<LdbcNoResult> {

	private static final ObjectMapper objectMapper = new ObjectMapper();
	public static final int TYPE = 1001;
	public static final String PERSON_ID = "%personId%";
	public static final String PERSON_FIRST_NAME = "%personFirstName%";
	public static final String PERSON_LAST_NAME = "%personLastName%";
	public static final String GENDER = "%gender%";
	public static final String BIRTHDAY = "%birthday%";
	public static final String CREATION_DATE = "%creationDate%";
	public static final String LOCATION_IP = "%locationIP%";
	public static final String BROWSER_USED = "%browserUsed%";
	public static final String CITY_ID = "%cityId%";
	public static final String LANGUAGES = "%languages%";
	public static final String EMAILS = "%emails%";
	public static final String TAG_IDS = "%tagIds%";
	public static final String STUDY_AT = "%studyAt%";
	public static final String WORK_AT = "%workAt%";

	private final IRI personId;
	private final String personFirstName;
	private final String personLastName;
	private final String gender;
	private final Literal birthday;
	private final Literal creationDate;
	private final String locationIp;
	private final String browserUsed;
	private final IRI cityId;
	private final String languages;
	private final String emails;
	private final IRI tagIds;
	private final List<Organization> studyAt;
	private final List<Organization> workAt;

	public LdbcUpdate1AddPerson(IRI personId,
								String personFirstName,
								String personLastName,
								String gender,
								Literal birthday,
								Literal creationDate,
								String locationIp,
								String browserUsed,
								IRI cityId,
								String languages,
								String emails,
								IRI tagIds,
								List<Organization> studyAt,
								List<Organization> workAt) {
		this.personId = personId;
		this.personFirstName = personFirstName;
		this.personLastName = personLastName;
		this.gender = gender;
		this.birthday = birthday;
		this.creationDate = creationDate;
		this.locationIp = locationIp;
		this.browserUsed = browserUsed;
		this.cityId = cityId;
		this.languages = languages;
		this.emails = emails;
		this.tagIds = tagIds;
		this.studyAt = studyAt;
		this.workAt = workAt;
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

	public String gender() {
		return gender;
	}

	public Literal birthday() {
		return birthday;
	}

	public Literal creationDate() {
		return creationDate;
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

	public String languages() {
		return languages;
	}

	public String emails() {
		return emails;
	}

	public IRI tagIds() {
		return tagIds;
	}

	public List<Organization> studyAt() {
		return studyAt;
	}

	public List<Organization> workAt() {
		return workAt;
	}

	@Override
	public int type() {
		return TYPE;
	}

	@Override
	public Map<String, Object> parameterMap() {
		return ImmutableMap.<String, Object>builder()
				.put(PERSON_ID, personId)
				.put(PERSON_FIRST_NAME, personFirstName)
				.put(PERSON_LAST_NAME, personLastName)
				.put(GENDER, gender)
				.put(BIRTHDAY, birthday)
				.put(CREATION_DATE, creationDate)
				.put(LOCATION_IP, locationIp)
				.put(BROWSER_USED, browserUsed)
				.put(CITY_ID, cityId)
				.put(LANGUAGES, languages)
				.put(EMAILS, emails)
				.put(TAG_IDS, tagIds)
				.put(STUDY_AT, studyAt)
				.put(WORK_AT, workAt)
				.build();
	}

	@Override
	public LdbcNoResult marshalResult(String serializedOperationResult) throws SerializingMarshallingException {
		return LdbcNoResult.INSTANCE;
	}

	@Override
	public String serializeResult(Object operationResultInstance) throws SerializingMarshallingException {
		try {
			return objectMapper.writeValueAsString(
					LdbcSnbInteractiveWorkloadConfiguration.WRITE_OPERATION_NO_RESULT_DEFAULT_RESULT);
		} catch (IOException e) {
			throw new SerializingMarshallingException(format("Error while trying to serialize result\n%s",
					operationResultInstance), e);
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		LdbcUpdate1AddPerson that = (LdbcUpdate1AddPerson) o;
		return Objects.equals(personId, that.personId)
				&& Objects.equals(personFirstName, that.personFirstName)
				&& Objects.equals(personLastName, that.personLastName)
				&& Objects.equals(gender, that.gender)
				&& Objects.equals(birthday, that.birthday)
				&& Objects.equals(creationDate, that.creationDate)
				&& Objects.equals(locationIp, that.locationIp)
				&& Objects.equals(browserUsed, that.browserUsed)
				&& Objects.equals(cityId, that.cityId)
				&& Objects.equals(languages, that.languages)
				&& Objects.equals(emails, that.emails)
				&& Objects.equals(tagIds, that.tagIds)
				&& Objects.equals(studyAt, that.studyAt)
				&& Objects.equals(workAt, that.workAt);
	}

	@Override
	public int hashCode() {
		return Objects.hash(personId, personFirstName, personLastName, gender, birthday, creationDate, locationIp,
				browserUsed, cityId, languages, emails, tagIds, studyAt, workAt);
	}

	@Override
	public String toString() {
		return "LdbcUpdate1AddPerson{" +
				"personId=" + personId +
				", personFirstName='" + personFirstName + '\'' +
				", personLastName='" + personLastName + '\'' +
				", gender='" + gender + '\'' +
				", birthday=" + birthday +
				", creationDate=" + creationDate +
				", locationIp='" + locationIp + '\'' +
				", browserUsed='" + browserUsed + '\'' +
				", cityId=" + cityId +
				", languages='" + languages + '\'' +
				", emails='" + emails + '\'' +
				", tagIds=" + tagIds +
				", studyAt=" + studyAt +
				", workAt=" + workAt +
				'}';
	}

	public static class Organization {

		private final IRI organizationId;
		private final Literal year;

		public Organization(IRI organizationId, Literal year) {
			this.organizationId = organizationId;
			this.year = year;
		}

		public IRI organizationId() {
			return organizationId;
		}

		public Literal year() {
			return year;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			Organization that = (Organization) o;
			return Objects.equals(organizationId, that.organizationId)
					&& Objects.equals(year, that.year);
		}

		// todo: fix hashCode
		@Override
		public int hashCode() {
			return Objects.hash(organizationId, year);
		}

		@Override
		public String toString() {
			return "Organization{" +
					"organizationId=" + organizationId +
					", year=" + year +
					'}';
		}
	}
}
