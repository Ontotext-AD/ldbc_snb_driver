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
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.lang.String.format;

public class LdbcQuery2 extends Operation<List<LdbcQuery2Result>> {
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static final int TYPE = 2;
	public static final String PERSON_ID = "%Person%";
	public static final String MAX_DATE = "%Date0%";

	private final String personId;
	private final String maxDate;

	public LdbcQuery2(long personId, Date maxDate) {
		this.personId = LdbcUtils.appendZeroAtStart(personId);
		this.maxDate = LdbcUtils.convertToDateAsString(maxDate);
	}

	public String personId() {
		return personId;
	}

	public String maxDate() {
		return maxDate;
	}

	@Override
	public Map<String, Object> parameterMap() {
		return ImmutableMap.<String, Object>builder()
				.put(PERSON_ID, personId)
				.put(MAX_DATE, maxDate)
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

		LdbcQuery2 that = (LdbcQuery2) o;

		if (!Objects.equals(personId, that.personId)) {
			return false;
		}
        return Objects.equals(maxDate, that.maxDate);
    }

	@Override
	public int hashCode() {
        int result = 31 * personId.hashCode();
		result = 31 * result + (maxDate != null ? maxDate.hashCode() : 0);
		return result;
	}

	@Override
	public String toString() {
		return "LdbcQuery2{" +
				"personId=" + personId +
				", maxDate=" + maxDate +
				'}';
	}

	@Override
	public List<LdbcQuery2Result> marshalResult(String serializedResults) throws SerializingMarshallingException {
		List<List<Object>> resultsAsList;
		try {
			resultsAsList = OBJECT_MAPPER.readValue(serializedResults, new TypeReference<List<List<Object>>>() {
			});
		} catch (IOException e) {
			throw new SerializingMarshallingException(
					format("Error parsing serialized result\n%s", serializedResults), e);
		}

		List<LdbcQuery2Result> results = new ArrayList<>();
		for (List<Object> resultAsList : resultsAsList) {
			IRI friendId = LdbcUtils.createIRI(resultAsList.get(0));
			String personFirstName = (String) resultAsList.get(1);
			String personLastName = (String) resultAsList.get(2);
			IRI messageId = LdbcUtils.createIRI(resultAsList.get(3));
			String messageContent = (String) resultAsList.get(4);
			Literal messageCreationDate = LdbcUtils.createLiteral(resultAsList.get(5));

			results.add(new LdbcQuery2Result(
					friendId,
					personFirstName,
					personLastName,
					messageId,
					messageContent,
					messageCreationDate
			));
		}

		return results;
	}

	@Override
	public String serializeResult(Object resultsObject) throws SerializingMarshallingException {
		List<LdbcQuery2Result> results = (List<LdbcQuery2Result>) resultsObject;
		List<List<Object>> resultsFields = new ArrayList<>();
		for (LdbcQuery2Result result : results) {
			List<Object> resultFields = new ArrayList<>();
			resultFields.add(result.personId());
			resultFields.add(result.personFirstName());
			resultFields.add(result.personLastName());
			resultFields.add(result.messageId());
			resultFields.add(result.messageContent());
			resultFields.add(result.messageCreationDate());
			resultsFields.add(resultFields);
		}

		try {
			return OBJECT_MAPPER.writeValueAsString(resultsFields);
		} catch (IOException e) {
			throw new SerializingMarshallingException(
					format("Error while trying to serialize result%n%s", results.toString()), e);
		}
	}

	@Override
	public int type() {
		return TYPE;
	}
}
