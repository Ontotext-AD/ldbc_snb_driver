package com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;
import org.eclipse.rdf4j.model.IRI;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.lang.String.format;

public class LdbcQuery10 extends Operation<List<LdbcQuery10Result>> {
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static final int TYPE = 10;
	public static final String PERSON_ID = "%Person%";
	public static final String MONTH = "%HS1%";

	private final String personId;
	private final String month;

	public LdbcQuery10(long personId, int month) {
		this.personId = LdbcUtils.appendZeroAtStart(personId);
		this.month = String.valueOf(month);
	}

	public String personId() {
		return personId;
	}

	public String month() {
		return month;
	}

	@Override
	public Map<String, Object> parameterMap() {
		return ImmutableMap.<String, Object>builder()
				.put(PERSON_ID, personId)
				.put(MONTH, month)
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

		LdbcQuery10 that = (LdbcQuery10) o;

		if (!month.equals(that.month)) {
			return false;
		}
		return Objects.equals(personId, that.personId);
	}

	@Override
	public int hashCode() {
		int result = 31 * personId.hashCode();
		result = 31 * result + (month != null ? month.hashCode() : 0);
		return result;
	}

	@Override
	public String toString() {
		return "LdbcQuery10{" +
				"personId=" + personId +
				", month=" + month +
				'}';
	}

	@Override
	public List<LdbcQuery10Result> marshalResult(String serializedResults) throws SerializingMarshallingException {
		List<List<Object>> resultsAsList;
		try {
			resultsAsList = OBJECT_MAPPER.readValue(serializedResults, new TypeReference<List<List<Object>>>() {
			});
		} catch (IOException e) {
			throw new SerializingMarshallingException(
					format("Error while parsing serialized results\n%s", serializedResults), e);
		}

		List<LdbcQuery10Result> results = new ArrayList<>();
		for (List<Object> resultAsList : resultsAsList) {
			IRI friendId = LdbcUtils.createIRI((String) resultAsList.get(0));
			String personFirstName = (String) resultAsList.get(1);
			String personLastName = (String) resultAsList.get(2);
			int commonInterestScore = ((Number) resultAsList.get(3)).intValue();
			String personGender = (String) resultAsList.get(4);
			String personCityName = (String) resultAsList.get(5);

			results.add(new LdbcQuery10Result(
					friendId,
					personFirstName,
					personLastName,
					commonInterestScore,
					personGender,
					personCityName
			));
		}

		return results;
	}

	@Override
	public String serializeResult(Object resultsObject) throws SerializingMarshallingException {
		List<LdbcQuery10Result> results = (List<LdbcQuery10Result>) resultsObject;
		List<List<Object>> resultsFields = new ArrayList<>();
		for (int i = 0; i < results.size(); i++) {
			LdbcQuery10Result result = results.get(i);
			List<Object> resultFields = new ArrayList<>();
			resultFields.add(result.personId());
			resultFields.add(result.personFirstName());
			resultFields.add(result.personLastName());
			resultFields.add(result.commonInterestScore());
			resultFields.add(result.personGender());
			resultFields.add(result.personCityName());
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