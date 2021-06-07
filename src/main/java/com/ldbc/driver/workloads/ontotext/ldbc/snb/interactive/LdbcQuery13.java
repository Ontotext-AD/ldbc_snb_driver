package com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.lang.String.format;

public class LdbcQuery13 extends Operation<LdbcQuery13Result> {
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static final int TYPE = 13;
	public static final String PERSON1_ID = "%Person1%";
	public static final String PERSON2_ID = "%Person2%";

	private final String person1Id;
	private final String person2Id;

	public LdbcQuery13(long person1Id, long person2Id) {
		this.person1Id = LdbcUtils.appendZeroAtStart(person1Id);
		this.person2Id = LdbcUtils.appendZeroAtStart(person2Id);
	}

	public String person1Id() {
		return person1Id;
	}

	public String person2Id() {
		return person2Id;
	}

	@Override
	public Map<String, Object> parameterMap() {
		return ImmutableMap.<String, Object>builder()
				.put(PERSON1_ID, person1Id)
				.put(PERSON2_ID, person2Id)
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

		LdbcQuery13 that = (LdbcQuery13) o;

        if (!Objects.equals(person1Id, that.person1Id)) {
            return false;
        }
        return Objects.equals(person2Id, that.person2Id);
	}

	@Override
	public int hashCode() {
		int result = 31 * person1Id.hashCode();
		result = 31 * result + (person2Id != null ? person2Id.hashCode() : 0);
		return result;
	}

	@Override
	public String toString() {
		return "LdbcQuery13{" +
				"person1Id=" + person1Id +
				", person2Id=" + person2Id +
				'}';
	}

	@Override
	public LdbcQuery13Result marshalResult(String serializedResult) throws SerializingMarshallingException {
		List<Object> resultAsList;
		try {
			resultAsList = OBJECT_MAPPER.readValue(serializedResult, new TypeReference<List<Object>>() {
			});
		} catch (IOException e) {
			throw new SerializingMarshallingException(
					format("Error while parsing serialized results\n%s", serializedResult), e);
		}

		int shortestPathLength = ((Number) resultAsList.get(0)).intValue();
		return new LdbcQuery13Result(shortestPathLength);
	}

	@Override
	public String serializeResult(Object resultObject) throws SerializingMarshallingException {
		LdbcQuery13Result result = (LdbcQuery13Result) resultObject;
		List<Object> resultFields = new ArrayList<>();
		resultFields.add(result.shortestPathLength());

		try {
			return OBJECT_MAPPER.writeValueAsString(resultFields);
		} catch (IOException e) {
			throw new SerializingMarshallingException(
					format("Error while trying to serialize result\n%s", result.toString()), e);
		}
	}

	@Override
	public int type() {
		return TYPE;
	}
}
