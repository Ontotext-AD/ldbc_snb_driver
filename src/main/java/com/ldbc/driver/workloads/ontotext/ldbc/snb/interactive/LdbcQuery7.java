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

public class LdbcQuery7 extends Operation<List<LdbcQuery7Result>> {
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static final int TYPE = 7;
	public static final String PERSON_ID = "%Person%";

	private final String personId;

	public LdbcQuery7(long personId) {
		this.personId = LdbcUtils.appendZeroAtStart(personId);
	}

	public String personId() {
		return personId;
	}

	@Override
	public Map<String, Object> parameterMap() {
		return ImmutableMap.<String, Object>builder()
				.put(PERSON_ID, personId)
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

		LdbcQuery7 that = (LdbcQuery7) o;

        return Objects.equals(personId, that.personId);
	}

	@Override
	public int hashCode() {
        return 31 * personId.hashCode();
	}

	@Override
	public String toString() {
		return "LdbcQuery7{" +
				"personId=" + personId +
				'}';
	}

	@Override
	public List<LdbcQuery7Result> marshalResult(String serializedResult) throws SerializingMarshallingException {
		List<List<Object>> resultsAsList;
		try {
			resultsAsList = OBJECT_MAPPER.readValue(serializedResult, new TypeReference<List<List<Object>>>() {
			});
		} catch (IOException e) {
			throw new SerializingMarshallingException(
					format("Error parsing serialized result\n%s", serializedResult), e);
		}

		List<LdbcQuery7Result> result = new ArrayList<>();
		for (List<Object> row : resultsAsList) {
			IRI friendId = LdbcUtils.createIRI((String) row.get(0));
			String personFirstName = (String) row.get(1);
			String personLastName = (String) row.get(2);
			Literal likeCreationDate = LdbcUtils.createLiteral((String) row.get(3));
			IRI messageId = LdbcUtils.createIRI((String) row.get(4));
			String messageContent = (String) row.get(5);
			int minutesLatency = ((Number) row.get(6)).intValue();
			boolean isNew = (Boolean) row.get(7);

			result.add(new LdbcQuery7Result(
					friendId,
					personFirstName,
					personLastName,
					likeCreationDate,
					messageId,
					messageContent,
					minutesLatency,
					isNew
			));
		}

		return result;
	}

	@Override
	public String serializeResult(Object resultsObject) throws SerializingMarshallingException {
		List<LdbcQuery7Result> results = (List<LdbcQuery7Result>) resultsObject;
		List<List<Object>> resultsFields = new ArrayList<>();
		for (int i = 0; i < results.size(); i++) {
			LdbcQuery7Result result = results.get(i);
			List<Object> resultFields = new ArrayList<>();
			resultFields.add(result.personId());
			resultFields.add(result.personFirstName());
			resultFields.add(result.personLastName());
			resultFields.add(result.likeCreationDate());
			resultFields.add(result.messageId());
			resultFields.add(result.messageContent());
			resultFields.add(result.minutesLatency());
			resultFields.add(result.isNew());
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
