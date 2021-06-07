package com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.lang.String.format;

public class LdbcQuery5 extends Operation<List<LdbcQuery5Result>> {
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static final int TYPE = 5;
	public static final String PERSON_ID = "%Person%";
	public static final String MIN_DATE = "%Date0%";

	private final String personId;
	private final String minDate;

	public LdbcQuery5(long personId, Date minDate) {
		this.personId = LdbcUtils.appendZeroAtStart(personId);
		this.minDate = LdbcUtils.convertToDateAsString(minDate);
	}

	public String personId() {
		return personId;
	}

	public String minDate() {
		return minDate;
	}

	@Override
	public Map<String, Object> parameterMap() {
		return ImmutableMap.<String, Object>builder()
				.put(PERSON_ID, personId)
				.put(MIN_DATE, minDate)
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

		LdbcQuery5 that = (LdbcQuery5) o;

        if (!Objects.equals(personId, that.personId)) {
            return false;
        }
        return Objects.equals(minDate, that.minDate);
    }

	@Override
	public int hashCode() {
		int result = 31 * personId.hashCode();
		result = 31 * result + (minDate != null ? minDate.hashCode() : 0);
		return result;
	}

	@Override
	public String toString() {
		return "LdbcQuery5{" +
				"personId=" + personId +
				", minDate=" + minDate +
				'}';
	}

	@Override
	public List<LdbcQuery5Result> marshalResult(String serializedResults) throws SerializingMarshallingException {
		List<List<Object>> resultsAsList;
		try {
			resultsAsList = OBJECT_MAPPER.readValue(serializedResults, new TypeReference<List<List<Object>>>() {
			});
		} catch (IOException e) {
			throw new SerializingMarshallingException(
					format("Error while parsing serialized results\n%s", serializedResults), e);
		}

		List<LdbcQuery5Result> results = new ArrayList<>();
		for (List<Object> resultAsList : resultsAsList) {
			String forumTitle = (String) resultAsList.get(0);
			int postCount = ((Number) resultAsList.get(1)).intValue();

			results.add(new LdbcQuery5Result(
					forumTitle,
					postCount
			));
		}

		return results;
	}

	@Override
	public String serializeResult(Object resultsObject) throws SerializingMarshallingException {
		List<LdbcQuery5Result> results = (List<LdbcQuery5Result>) resultsObject;
		List<List<Object>> resultsFields = new ArrayList<>();
		for (int i = 0; i < results.size(); i++) {
			LdbcQuery5Result result = results.get(i);
			List<Object> resultFields = new ArrayList<>();
			resultFields.add(result.forumTitle());
			resultFields.add(result.postCount());
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
