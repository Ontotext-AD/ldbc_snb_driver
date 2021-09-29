package com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;
import com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive.readers.LdbcUtils;
import org.eclipse.rdf4j.model.IRI;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.lang.String.format;

public class LdbcQuery11 extends Operation<List<LdbcQuery11Result>> {
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static final int TYPE = 11;
	public static final String PERSON_ID = "%Person%";
	public static final String COUNTRY_NAME = "%Country%";
	public static final String WORK_FROM_YEAR = "%Date0%";

	private final String personId;
	private final String countryName;
	private final String workFromYear;

	public LdbcQuery11(long personId, String countryName, int workFromYear) {
		this.personId = LdbcUtils.appendZeroAtStart(personId);
		this.countryName = countryName;
		this.workFromYear = String.valueOf(workFromYear);
	}

	public String personId() {
		return personId;
	}

	public String countryName() {
		return countryName;
	}

	public String workFromYear() {
		return workFromYear;
	}

	@Override
	public Map<String, Object> parameterMap() {
		return ImmutableMap.<String, Object>builder()
				.put(PERSON_ID, personId)
				.put(COUNTRY_NAME, countryName)
				.put(WORK_FROM_YEAR, workFromYear)
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

		LdbcQuery11 that = (LdbcQuery11) o;


        if (!Objects.equals(personId, that.personId)) {
            return false;
        }
		if (workFromYear != that.workFromYear) {
			return false;
		}
		return Objects.equals(countryName, that.countryName);
	}

	@Override
	public int hashCode() {
		int result = 31 * personId.hashCode();
		result = 31 * result + (countryName != null ? countryName.hashCode() : 0);
		result = 31 * result + (workFromYear != null ? workFromYear.hashCode() : 0);
		return result;
	}

	@Override
	public String toString() {
		return "LdbcQuery11{" +
				"personId=" + personId +
				", countryName='" + countryName + '\'' +
				", workFromYear=" + workFromYear +
				'}';
	}

	@Override
	public List<LdbcQuery11Result> marshalResult(String serializedResults) throws SerializingMarshallingException {
		List<List<Object>> resultsAsList;
		try {
			resultsAsList = OBJECT_MAPPER.readValue(serializedResults, new TypeReference<List<List<Object>>>() {
			});
		} catch (IOException e) {
			throw new SerializingMarshallingException(
					format("Error while parsing serialized results\n%s", serializedResults), e);
		}

		List<LdbcQuery11Result> results = new ArrayList<>();
		for (List<Object> resultAsList : resultsAsList) {
			IRI friendId = LdbcUtils.createIRI(resultAsList.get(0));
			String personFirstName = (String) resultAsList.get(1);
			String personLastName = (String) resultAsList.get(2);
			String organizationName = (String) resultAsList.get(3);
			int organizationWorkFromYear = ((Number) resultAsList.get(4)).intValue();

			results.add(new LdbcQuery11Result(
					friendId,
					personFirstName,
					personLastName,
					organizationName,
					organizationWorkFromYear
			));
		}

		return results;
	}

	@Override
	public String serializeResult(Object resultsObject) throws SerializingMarshallingException {
		List<LdbcQuery11Result> results = (List<LdbcQuery11Result>) resultsObject;
		List<List<Object>> resultsFields = new ArrayList<>();
		for (LdbcQuery11Result result : results) {
			List<Object> resultFields = new ArrayList<>();
			resultFields.add(result.personId());
			resultFields.add(result.personFirstName());
			resultFields.add(result.personLastName());
			resultFields.add(result.organizationName());
			resultFields.add(result.organizationWorkFromYear());
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
