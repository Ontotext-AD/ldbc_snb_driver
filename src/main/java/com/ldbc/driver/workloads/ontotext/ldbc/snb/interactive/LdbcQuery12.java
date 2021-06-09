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
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.lang.String.format;

public class LdbcQuery12 extends Operation<List<LdbcQuery12Result>> {
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static final int TYPE = 12;
	public static final String PERSON_ID = "%Person%";
	public static final String TAG_CLASS_NAME = "%TagType%";

	private final String personId;
	private final String tagClassName;

	public LdbcQuery12(long personId, String tagClassName) {
		this.personId = LdbcUtils.appendZeroAtStart(personId);
		this.tagClassName = tagClassName;
	}

	public String personId() {
		return personId;
	}

	public String tagClassName() {
		return tagClassName;
	}

	@Override
	public Map<String, Object> parameterMap() {
		return ImmutableMap.<String, Object>builder()
				.put(PERSON_ID, personId)
				.put(TAG_CLASS_NAME, tagClassName)
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

		LdbcQuery12 that = (LdbcQuery12) o;

		if (!Objects.equals(personId, that.personId)) {
			return false;
		}
		return Objects.equals(tagClassName, that.tagClassName);
	}

	@Override
	public int hashCode() {
		int result = 31 * personId.hashCode();
		result = 31 * result + (tagClassName != null ? tagClassName.hashCode() : 0);
		return result;
	}

	@Override
	public String toString() {
		return "LdbcQuery12{" +
				"personId=" + personId +
				", tagClassName='" + tagClassName + '\'' +
				'}';
	}

	@Override
	public List<LdbcQuery12Result> marshalResult(String serializedResults) throws SerializingMarshallingException {
		List<List<Object>> resultsAsList;
		try {
			resultsAsList = OBJECT_MAPPER.readValue(serializedResults, new TypeReference<List<List<Object>>>() {
			});
		} catch (IOException e) {
			throw new SerializingMarshallingException(
					format("Error while parsing serialized results\n%s", serializedResults), e);
		}

		List<LdbcQuery12Result> results = new ArrayList<>();
		for (List<Object> resultAsList : resultsAsList) {
			IRI friendId = LdbcUtils.createIRI((String) resultAsList.get(0));
			String personFirstName = (String) resultAsList.get(1);
			String personLastName = (String) resultAsList.get(2);
			String tagNames = (String) resultAsList.get(3);
			int replyCount = ((Number) resultAsList.get(4)).intValue();

			results.add(new LdbcQuery12Result(
					friendId,
					personFirstName,
					personLastName,
					tagNames,
					replyCount
			));
		}

		return results;
	}

	@Override
	public String serializeResult(Object resultsObject) throws SerializingMarshallingException {
		List<LdbcQuery12Result> results = (List<LdbcQuery12Result>) resultsObject;
		List<List<Object>> resultsFields = new ArrayList<>();
		for (LdbcQuery12Result result : results) {
			List<Object> resultFields = new ArrayList<>();
			resultFields.add(result.personId());
			resultFields.add(result.personFirstName());
			resultFields.add(result.personLastName());
			resultFields.add(result.tagNames());
			resultFields.add(result.replyCount());
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
