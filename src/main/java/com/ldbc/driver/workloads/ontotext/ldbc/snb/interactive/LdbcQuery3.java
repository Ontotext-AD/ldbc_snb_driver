package com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;
import com.ldbc.driver.workloads.common.LdbcUtils;
import org.eclipse.rdf4j.model.IRI;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.lang.String.format;

public class LdbcQuery3 extends Operation<List<LdbcQuery3Result>> {
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static final int TYPE = 3;
	public static final String PERSON_ID = "%Person%";
	public static final String COUNTRY_X_NAME = "%Country1%";
	public static final String COUNTRY_Y_NAME = "%Country2%";
	public static final String START_DATE = "%Date0%";
	public static final String DURATION_DAYS = "%Duration%";

	private final String personId;
	private final String countryXName;
	private final String countryYName;
	private final String startDate;
	private final int durationDays;

	public LdbcQuery3(long personId, String countryXName, String countryYName, Date startDate, int durationDays) {
		this.personId = LdbcUtils.appendZeroAtStart(personId);
		this.countryXName = countryXName;
		this.countryYName = countryYName;
		this.startDate = LdbcUtils.convertToDateAsString(startDate);
		this.durationDays = durationDays;
	}

	public String personId() {
		return personId;
	}

	public String countryXName() {
		return countryXName;
	}

	public String countryYName() {
		return countryYName;
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
				.put(COUNTRY_X_NAME, countryXName)
				.put(COUNTRY_Y_NAME, countryYName)
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

		LdbcQuery3 that = (LdbcQuery3) o;

		if (durationDays != that.durationDays) {
			return false;
		}

        if (!Objects.equals(personId, that.personId)) {
            return false;
        }
		if (!Objects.equals(countryXName, that.countryXName)) {
			return false;
		}
		if (!Objects.equals(countryYName, that.countryYName)) {
			return false;
		}
        return Objects.equals(startDate, that.startDate);
    }

	@Override
	public int hashCode() {
		int result = 31 * personId.hashCode();
		result = 31 * result + (countryXName != null ? countryXName.hashCode() : 0);
		result = 31 * result + (countryYName != null ? countryYName.hashCode() : 0);
		result = 31 * result + (startDate != null ? startDate.hashCode() : 0);
		result = 31 * result + durationDays;
		return result;
	}

	@Override
	public String toString() {
		return "LdbcQuery3{" +
				"personId=" + personId +
				", countryXName='" + countryXName + '\'' +
				", countryYName='" + countryYName + '\'' +
				", startDate=" + startDate +
				", durationDays=" + durationDays +
				'}';
	}

	@Override
	public List<LdbcQuery3Result> marshalResult(String serializedResults) throws SerializingMarshallingException {
		List<List<Object>> resultsAsList;
		try {
			resultsAsList = OBJECT_MAPPER.readValue(serializedResults, new TypeReference<List<List<Object>>>() {
			});
		} catch (IOException e) {
			throw new SerializingMarshallingException(
					format("Error while parsing serialized results\n%s", serializedResults), e);
		}

		List<LdbcQuery3Result> results = new ArrayList<>();
		for (List<Object> resultAsList : resultsAsList) {
			IRI friendId = LdbcUtils.createIRI((String) resultAsList.get(0));
			String personFirstName = (String) resultAsList.get(1);
			String personLastName = (String) resultAsList.get(2);
			long xCount = ((Number) resultAsList.get(3)).longValue();
			long yCount = ((Number) resultAsList.get(4)).longValue();
			long count = ((Number) resultAsList.get(5)).longValue();

			results.add(new LdbcQuery3Result(
					friendId,
					personFirstName,
					personLastName,
					xCount,
					yCount,
					count
			));
		}

		return results;
	}

	@Override
	public String serializeResult(Object resultsObject) throws SerializingMarshallingException {
		List<LdbcQuery3Result> results = (List<LdbcQuery3Result>) resultsObject;
		List<List<Object>> resultsFields = new ArrayList<>();
		for (int i = 0; i < results.size(); i++) {
			LdbcQuery3Result result = results.get(i);
			List<Object> resultFields = new ArrayList<>();
			resultFields.add(result.personId());
			resultFields.add(result.personFirstName());
			resultFields.add(result.personLastName());
			resultFields.add(result.xCount());
			resultFields.add(result.yCount());
			resultFields.add(result.count());
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