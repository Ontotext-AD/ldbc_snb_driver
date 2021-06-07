package com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.lang.String.format;

public class LdbcQuery8 extends Operation<List<LdbcQuery8Result>> {
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static final int TYPE = 8;
	public static final String PERSON_ID = "%Person%";

	private final String personId;

	public LdbcQuery8(long personId) {
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

		LdbcQuery8 that = (LdbcQuery8) o;

        return Objects.equals(personId, that.personId);
    }

	@Override
	public int hashCode() {
        return 31 * personId.hashCode();
	}

	@Override
	public String toString() {
		return "LdbcQuery8{" +
				"personId=" + personId +
				'}';
	}

	@Override
	public List<LdbcQuery8Result> marshalResult(String serializedResults) throws SerializingMarshallingException {
		List<List<Object>> resultsAsList;
		try {
			resultsAsList = OBJECT_MAPPER.readValue(serializedResults, new TypeReference<List<List<Object>>>() {
			});
		} catch (IOException e) {
			throw new SerializingMarshallingException(
					format("Error while parsing serialized results\n%s", serializedResults), e);
		}

		List<LdbcQuery8Result> results = new ArrayList<>();
		for (List<Object> resultAsList : resultsAsList) {
			IRI friendId = LdbcUtils.createIRI((String) resultAsList.get(0));
			String personFirstName = (String) resultAsList.get(1);
			String personLastName = (String) resultAsList.get(2);
			Literal commentCreationDate = LdbcUtils.createLiteral((String) resultAsList.get(3));
			IRI commentId = LdbcUtils.createIRI((String) resultAsList.get(4));
			String commentContent = (String) resultAsList.get(5);

			results.add(new LdbcQuery8Result(
					friendId,
					personFirstName,
					personLastName,
					commentCreationDate,
					commentId,
					commentContent
			));
		}

		return results;
	}

	@Override
	public String serializeResult(Object resultsObject) throws SerializingMarshallingException {
		List<LdbcQuery8Result> results = (List<LdbcQuery8Result>) resultsObject;
		List<List<Object>> resultsFields = new ArrayList<>();
		for (int i = 0; i < results.size(); i++) {
			LdbcQuery8Result result = results.get(i);
			List<Object> resultFields = new ArrayList<>();
			resultFields.add(result.personId());
			resultFields.add(result.personFirstName());
			resultFields.add(result.personLastName());
			resultFields.add(result.commentCreationDate());
			resultFields.add(result.commentId());
			resultFields.add(result.commentContent());
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
