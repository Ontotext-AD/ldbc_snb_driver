package com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;
import com.ldbc.driver.workloads.common.LdbcUtils;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.lang.String.format;

public class LdbcQuery1 extends Operation<List<LdbcQuery1Result>> {
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static final int TYPE = 1;
	public static final String PERSON_ID = "%Person%";
	public static final String FIRST_NAME = "%Name%";

	private final String personId;
	private final String firstName;

	public LdbcQuery1(long personId, String firstName) {
		this.personId = LdbcUtils.appendZeroAtStart(personId);
		this.firstName = firstName;
	}

	public String personId() {
		return personId;
	}

	public String firstName() {
		return firstName;
	}

	@Override
	public Map<String, Object> parameterMap() {
		return ImmutableMap.<String, Object>builder()
				.put(PERSON_ID, personId)
				.put(FIRST_NAME, firstName)
				.build();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		LdbcQuery1 that = (LdbcQuery1) o;

		if (!Objects.equals(personId, that.personId)) {
			return false;
		}
        return Objects.equals(firstName, that.firstName);
    }

	@Override
	public int hashCode() {
		int result = 31 * personId.hashCode();
		result = 31 * result + (firstName != null ? firstName.hashCode() : 0);
		return result;
	}

    @Override
    public String toString() {
        return "LdbcQuery1{" +
                "personId=" + personId +
                ", firstName='" + firstName + '\'' +
                '}';
    }

	@Override
	public List<LdbcQuery1Result> marshalResult(String serializedResults) throws SerializingMarshallingException {
		List<List<Object>> resultsAsList;
		try {
			resultsAsList = OBJECT_MAPPER.readValue(serializedResults, new TypeReference<List<List<Object>>>() {
			});
		} catch (IOException e) {
			throw new SerializingMarshallingException(
					format("Error parsing serialized results\n%s", serializedResults), e);
		}

		List<LdbcQuery1Result> results = new ArrayList<>();
		for (List<Object> resultAsList : resultsAsList) {
			IRI friendId = LdbcUtils.createIRI((String) resultAsList.get(0));
			String friendLastName = (String) resultAsList.get(1);
			int distanceFromPerson = (Integer) resultAsList.get(2);
			Literal friendBirthday = LdbcUtils.createLiteral((String) resultAsList.get(3));
			Literal friendCreationDate = LdbcUtils.createLiteral((String) resultAsList.get(4));
			String friendGender = (String) resultAsList.get(5);
			String friendBrowserUsed = (String) resultAsList.get(6);
			String friendLocationIp = (String) resultAsList.get(7);
			String friendEmails = (String) resultAsList.get(8);
			String friendLanguages = (String) resultAsList.get(9);
			String friendCityName = (String) resultAsList.get(10);
			String friendUniversities = (String) resultAsList.get(11);
			String friendCompanies = (String) resultAsList.get(12);

			results.add(new LdbcQuery1Result(
					friendId,
					friendLastName,
					distanceFromPerson,
					friendBirthday,
					friendCreationDate,
					friendGender,
					friendBrowserUsed,
					friendLocationIp,
					friendEmails,
					friendLanguages,
					friendCityName,
					friendUniversities,
					friendCompanies));
		}

		return results;
	}

	@Override
	public String serializeResult(Object resultsObject) throws SerializingMarshallingException {
		List<LdbcQuery1Result> results = (List<LdbcQuery1Result>) resultsObject;

		List<List<Object>> resultsFields = new ArrayList<>();
		for (LdbcQuery1Result result : results) {
			List<Object> resultFields = new ArrayList<>();
			resultFields.add(result.friendId());
			resultFields.add(result.friendLastName());
			resultFields.add(result.distanceFromPerson());
			resultFields.add(result.friendBirthday());
			resultFields.add(result.friendCreationDate());
			resultFields.add(result.friendGender());
			resultFields.add(result.friendBrowserUsed());
			resultFields.add(result.friendLocationIp());
			resultFields.add(result.friendEmails());
			resultFields.add(result.friendLanguages());
			resultFields.add(result.friendCityName());
			resultFields.add(result.friendUniversities());
			resultFields.add(result.friendCompanies());
			resultsFields.add(resultFields);
		}

		try {
			return OBJECT_MAPPER.writeValueAsString(resultsFields);
		} catch (IOException e) {
			throw new SerializingMarshallingException(
					format("Error while trying to serialize result\n%s", results.toString()), e);
		}
	}

	@Override
	public int type() {
		return TYPE;
	}
}
