package com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive.queries.shortreads;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;
import com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive.readers.LdbcUtils;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.lang.String.format;

public class LdbcShortQuery1PersonProfile extends Operation<LdbcShortQuery1PersonProfileResult> {

	private static final ObjectMapper objectMapper = new ObjectMapper();
	public static final int TYPE = 101;
	public static final String PERSON_ID = "%personId%";

	private final String personId;

	public LdbcShortQuery1PersonProfile(long personId) {
		this.personId = LdbcUtils.appendZeroAtStart(personId);
	}

	public String personId() {
		return personId;
	}

	@Override
	public int type() {
		return TYPE;
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
		LdbcShortQuery1PersonProfile that = (LdbcShortQuery1PersonProfile) o;
		return Objects.equals(personId, that.personId);
	}

	@Override
	public int hashCode() {
		return 31 * personId.hashCode();
	}

	@Override
	public LdbcShortQuery1PersonProfileResult marshalResult(String serializedOperationResult) throws SerializingMarshallingException {
		List<Object> resultAsList;
		try {
			resultAsList = objectMapper.readValue(serializedOperationResult, new TypeReference<List<Object>>() {
			});
		} catch (IOException e) {
			throw new SerializingMarshallingException(format("Error while parsing serialized results\n%s",
					serializedOperationResult), e);
		}

		String firstName = (String) resultAsList.get(0);
		String lastName = (String) resultAsList.get(1);
		Literal birthday = LdbcUtils.createLiteral(resultAsList.get(2));
		String locationIp = (String) resultAsList.get(3);
		String browserUsed = (String) resultAsList.get(4);
		IRI cityId = LdbcUtils.createIRI(resultAsList.get(5));
		String gender = (String) resultAsList.get(6);
		Literal creationDate = LdbcUtils.createLiteral(resultAsList.get(7));

		return new LdbcShortQuery1PersonProfileResult(
				firstName,
				lastName,
				birthday,
				locationIp,
				browserUsed,
				cityId,
				gender,
				creationDate
		);
	}

	@Override
	public String serializeResult(Object operationResultInstance) throws SerializingMarshallingException {
		LdbcShortQuery1PersonProfileResult result = (LdbcShortQuery1PersonProfileResult) operationResultInstance;

		List<Object> resultFields = new ArrayList<>();
		resultFields.add(result.firstName());
		resultFields.add(result.lastName());
		resultFields.add(result.birthday());
		resultFields.add(result.locationIp());
		resultFields.add(result.browserUsed());
		resultFields.add(result.cityId());
		resultFields.add(result.gender());
		resultFields.add(result.creationDate());

		try {
			return objectMapper.writeValueAsString(resultFields);
		} catch (IOException e) {
			throw new SerializingMarshallingException(format("Error while trying to serialize result\n%s", result), e);
		}
	}

	@Override
	public String toString() {
		return "LdbcShortQuery1PersonProfile{" +
				"personId='" + personId + '\'' +
				'}';
	}
}