package com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive.queries.longreads;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;
import com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive.readers.LdbcUtils;

import java.io.IOException;
import java.util.*;

import static java.lang.String.format;

public class LdbcQuery4 extends Operation<List<LdbcQuery4Result>> {
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static final int TYPE = 4;
	public static final String PERSON_ID = "%Person%";
	public static final String START_DATE = "%Date0%";
	public static final String DURATION_DAYS = "%Duration%";

	private final String personId;
	private final String startDate;
	private final int durationDays;

	public LdbcQuery4(long personId, Date startDate, int durationDays) {
		this.personId = LdbcUtils.appendZeroAtStart(personId);
		this.startDate = LdbcUtils.convertToDateAsString(startDate);
		this.durationDays = durationDays;
	}

	public String personId() {
		return personId;
	}

	public String startDate() {
		return startDate;
	}

	public int durationDays() {
		return durationDays;
	}

	@Override
	public Map<String, Object> parameterMap() {
		return ImmutableMap.<String, Object>builder()
				.put(PERSON_ID, personId)
				.put(START_DATE, startDate)
				.put(DURATION_DAYS, durationDays)
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

		LdbcQuery4 that = (LdbcQuery4) o;

		if (durationDays != that.durationDays) {
			return false;
		}

        if (!Objects.equals(personId, that.personId)) {
            return false;
        }
        return Objects.equals(startDate, that.startDate);
    }

	@Override
	public int hashCode() {
		int result = 31 * personId.hashCode();
		result = 31 * result + (startDate != null ? startDate.hashCode() : 0);
		result = 31 * result + durationDays;
		return result;
	}

	@Override
	public String toString() {
		return "LdbcQuery4{" +
				"personId=" + personId +
				", startDate=" + startDate +
				", durationDays=" + durationDays +
				'}';
	}

	@Override
	public List<LdbcQuery4Result> marshalResult(String serializedResults) throws SerializingMarshallingException {
		List<List<Object>> resultsAsList;
		try {
			resultsAsList = OBJECT_MAPPER.readValue(serializedResults, new TypeReference<List<List<Object>>>() {
			});
		} catch (IOException e) {
			throw new SerializingMarshallingException(
					format("Error while parsing serialized results\n%s", serializedResults), e);
		}

		List<LdbcQuery4Result> results = new ArrayList<>();
		for (List<Object> resultAsList : resultsAsList) {
			String tagName = (String) resultAsList.get(0);
			int tagCount = ((Number) resultAsList.get(1)).intValue();

			results.add(new LdbcQuery4Result(
					tagName,
					tagCount
			));
		}

		return results;
	}

	@Override
	public String serializeResult(Object resultsObject) throws SerializingMarshallingException {
		List<LdbcQuery4Result> results = (List<LdbcQuery4Result>) resultsObject;
		List<List<Object>> resultsFields = new ArrayList<>();
		for (LdbcQuery4Result result : results) {
			List<Object> resultFields = new ArrayList<>();
			resultFields.add(result.tagName());
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