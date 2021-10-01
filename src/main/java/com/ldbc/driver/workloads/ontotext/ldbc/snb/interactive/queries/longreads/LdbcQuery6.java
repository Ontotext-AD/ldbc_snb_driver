package com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive.queries.longreads;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;
import com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive.readers.LdbcUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.lang.String.format;

public class LdbcQuery6 extends Operation<List<LdbcQuery6Result>> {
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static final int TYPE = 6;
	public static final String PERSON_ID = "%Person%";
	public static final String TAG_NAME = "%Tag%";

	private final String personId;
	private final String tagName;

	public LdbcQuery6(long personId, String tagName) {
		this.personId = LdbcUtils.appendZeroAtStart(personId);
		this.tagName = tagName;
	}

	public String personId() {
		return personId;
	}

	public String tagName() {
		return tagName;
	}

	@Override
	public Map<String, Object> parameterMap() {
		return ImmutableMap.<String, Object>builder()
				.put(PERSON_ID, personId)
				.put(TAG_NAME, tagName)
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

		LdbcQuery6 that = (LdbcQuery6) o;

        if (!Objects.equals(personId, that.personId)) {
            return false;
        }
        return Objects.equals(tagName, that.tagName);
    }

	@Override
	public int hashCode() {
		int result = 31 * personId.hashCode();
		result = 31 * result + (tagName != null ? tagName.hashCode() : 0);
		return result;
	}

	@Override
	public String toString() {
		return "LdbcQuery6{" +
				"personId=" + personId +
				", tagName='" + tagName + '\'' +
				'}';
	}

	@Override
	public List<LdbcQuery6Result> marshalResult(String serializedResults) throws SerializingMarshallingException {
		List<List<Object>> resultsAsList;
		try {
			resultsAsList = OBJECT_MAPPER.readValue(serializedResults, new TypeReference<List<List<Object>>>() {
			});
		} catch (IOException e) {
			throw new SerializingMarshallingException(
					format("Error while parsing serialized results\n%s", serializedResults), e);
		}

		List<LdbcQuery6Result> results = new ArrayList<>();
		for (List<Object> resultAsList : resultsAsList) {
			String tagName = (String) resultAsList.get(0);
			int tagCount = ((Number) resultAsList.get(1)).intValue();

			results.add(new LdbcQuery6Result(
					tagName,
					tagCount
			));
		}

		return results;
	}

	@Override
	public String serializeResult(Object resultsObject) throws SerializingMarshallingException {
		List<LdbcQuery6Result> results = (List<LdbcQuery6Result>) resultsObject;
		List<List<Object>> resultsFields = new ArrayList<>();
		for (int i = 0; i < results.size(); i++) {
			LdbcQuery6Result result = results.get(i);
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
