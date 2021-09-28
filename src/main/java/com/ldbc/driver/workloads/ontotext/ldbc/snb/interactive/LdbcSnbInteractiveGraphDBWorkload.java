package com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;
import com.ldbc.driver.Workload;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.workloads.common.LdbcUtils;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery2PersonPosts;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery3PersonFriends;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery7MessageReplies;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveDbValidationParametersFilter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkloadConfiguration;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.Equator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

public class LdbcSnbInteractiveGraphDBWorkload extends Workload {

	public static final String ERROR_WHILE_TRYING_TO_SERIALIZE_RESULT_N_S = "Error while trying to serialize result%n%s";

	@Override
	public Map<Integer, Class<? extends Operation>> operationTypeToClassMapping() {
		Map<Integer, Class<? extends Operation>> operationTypeToClassMapping = new HashMap<>();
		operationTypeToClassMapping.put(LdbcQuery1.TYPE, LdbcQuery1.class);
		operationTypeToClassMapping.put(LdbcQuery2.TYPE, LdbcQuery2.class);
		operationTypeToClassMapping.put(LdbcQuery3.TYPE, LdbcQuery3.class);
		operationTypeToClassMapping.put(LdbcQuery4.TYPE, LdbcQuery4.class);
		operationTypeToClassMapping.put(LdbcQuery5.TYPE, LdbcQuery5.class);
		operationTypeToClassMapping.put(LdbcQuery6.TYPE, LdbcQuery6.class);
		operationTypeToClassMapping.put(LdbcQuery7.TYPE, LdbcQuery7.class);
		operationTypeToClassMapping.put(LdbcQuery8.TYPE, LdbcQuery8.class);
		operationTypeToClassMapping.put(LdbcQuery9.TYPE, LdbcQuery9.class);
		operationTypeToClassMapping.put(LdbcQuery10.TYPE, LdbcQuery10.class);
		operationTypeToClassMapping.put(LdbcQuery11.TYPE, LdbcQuery11.class);
		operationTypeToClassMapping.put(LdbcQuery12.TYPE, LdbcQuery12.class);
		operationTypeToClassMapping.put(LdbcQuery13.TYPE, LdbcQuery13.class);
		operationTypeToClassMapping.put(LdbcQuery14.TYPE, LdbcQuery14.class);
		return operationTypeToClassMapping;
	}

	@Override
	public void onInit(Map<String, String> params) throws WorkloadException {
		List<String> compulsoryKeys = Lists.newArrayList(
				LdbcSnbInteractiveWorkloadConfiguration.PARAMETERS_DIRECTORY);

        compulsoryKeys.addAll(LdbcSnbInteractiveWorkloadConfiguration.LONG_READ_OPERATION_ENABLE_KEYS);

		Set<String> missingPropertyParameters =
				LdbcSnbInteractiveWorkloadConfiguration.missingParameters(params, compulsoryKeys);
		if (!missingPropertyParameters.isEmpty()) {
			throw new WorkloadException(format("Workload could not initialize due to missing parameters: %s",
					missingPropertyParameters));
		}

		getQueries(params);

        super.onInit(params);
	}

	@Override
	public DbValidationParametersFilter dbValidationParametersFilter(Integer requiredValidationParameterCount) {
		final Set<Class<?>> multiResultOperations = Sets.newHashSet(
				LdbcQuery1.class,
				LdbcQuery2.class,
                LdbcQuery3.class,
				LdbcQuery4.class,
				LdbcQuery5.class,
				LdbcQuery6.class,
				LdbcQuery7.class,
				LdbcQuery8.class,
				LdbcQuery9.class,
				LdbcQuery10.class,
				LdbcQuery11.class,
				LdbcQuery12.class,
				LdbcQuery14.class
		);

		int operationTypeCount = enabledLongReadOperationTypes.size() + enabledWriteOperationTypes.size();
		long minimumResultCountPerOperationType = Math.max(
				1,
				Math.round( Math.floor(
						requiredValidationParameterCount.doubleValue() / (double) operationTypeCount) )
		);

		final Map<Class<?>, Long> remainingRequiredResultsPerLongReadType = new HashMap<>();
		long resultCountsAssignedForLongReadTypesSoFar = 0;
        for (Class longReadOperationType : enabledLongReadOperationTypes) {
            remainingRequiredResultsPerLongReadType.put(longReadOperationType, minimumResultCountPerOperationType);
            resultCountsAssignedForLongReadTypesSoFar =
                    resultCountsAssignedForLongReadTypesSoFar + minimumResultCountPerOperationType;
        }

		return new LdbcSnbInteractiveDbValidationParametersFilter(
				multiResultOperations,
				0L,
				new HashMap<>(),
				remainingRequiredResultsPerLongReadType,
				new HashSet<>()
		);
	}

	@Override
	public long maxExpectedInterleaveAsMilli() {
		return TimeUnit.HOURS.toMillis(1);
	}

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
	private static final TypeReference<List<Object>> TYPE_REFERENCE = new TypeReference<List<Object>>() {
	};

	@Override
	public String serializeOperation(Operation operation) throws SerializingMarshallingException {
		switch (operation.type()) {
			case LdbcQuery1.TYPE: {
				LdbcQuery1 ldbcQuery = (LdbcQuery1) operation;
				List<Object> operationAsList = new ArrayList<>();
				operationAsList.add(ldbcQuery.getClass().getName());
				operationAsList.add(ldbcQuery.personId());
				operationAsList.add(ldbcQuery.firstName());
				try {
					return OBJECT_MAPPER.writeValueAsString(operationAsList);
				} catch (IOException e) {
					throw new SerializingMarshallingException(
							format(ERROR_WHILE_TRYING_TO_SERIALIZE_RESULT_N_S, operationAsList), e);
				}
			}
			case LdbcQuery2.TYPE: {
				LdbcQuery2 ldbcQuery = (LdbcQuery2) operation;
				List<Object> operationAsList = new ArrayList<>();
				operationAsList.add(ldbcQuery.getClass().getName());
				operationAsList.add(ldbcQuery.personId());
				operationAsList.add(ldbcQuery.maxDate());
				try {
					return OBJECT_MAPPER.writeValueAsString(operationAsList);
				} catch (IOException e) {
					throw new SerializingMarshallingException(
							format(ERROR_WHILE_TRYING_TO_SERIALIZE_RESULT_N_S, operationAsList), e);
				}
			}
			case LdbcQuery3.TYPE: {
				LdbcQuery3 ldbcQuery = (LdbcQuery3) operation;
				List<Object> operationAsList = new ArrayList<>();
				operationAsList.add(ldbcQuery.getClass().getName());
				operationAsList.add(ldbcQuery.personId());
				operationAsList.add(ldbcQuery.countryXName());
				operationAsList.add(ldbcQuery.countryYName());
				operationAsList.add(ldbcQuery.startDate());
				operationAsList.add(ldbcQuery.durationDays());
				try {
					return OBJECT_MAPPER.writeValueAsString(operationAsList);
				} catch (IOException e) {
					throw new SerializingMarshallingException(
							format(ERROR_WHILE_TRYING_TO_SERIALIZE_RESULT_N_S, operationAsList), e);
				}
			}
			case LdbcQuery4.TYPE: {
				LdbcQuery4 ldbcQuery = (LdbcQuery4) operation;
				List<Object> operationAsList = new ArrayList<>();
				operationAsList.add(ldbcQuery.getClass().getName());
				operationAsList.add(ldbcQuery.personId());
				operationAsList.add(ldbcQuery.startDate());
				operationAsList.add(ldbcQuery.durationDays());
				try {
					return OBJECT_MAPPER.writeValueAsString(operationAsList);
				} catch (IOException e) {
					throw new SerializingMarshallingException(
							format(ERROR_WHILE_TRYING_TO_SERIALIZE_RESULT_N_S, operationAsList), e);
				}
			}
			case LdbcQuery5.TYPE: {
				LdbcQuery5 ldbcQuery = (LdbcQuery5) operation;
				List<Object> operationAsList = new ArrayList<>();
				operationAsList.add(ldbcQuery.getClass().getName());
				operationAsList.add(ldbcQuery.personId());
				operationAsList.add(ldbcQuery.minDate());
				try {
					return OBJECT_MAPPER.writeValueAsString(operationAsList);
				} catch (IOException e) {
					throw new SerializingMarshallingException(
							format(ERROR_WHILE_TRYING_TO_SERIALIZE_RESULT_N_S, operationAsList), e);
				}
			}
			case LdbcQuery6.TYPE: {
				LdbcQuery6 ldbcQuery = (LdbcQuery6) operation;
				List<Object> operationAsList = new ArrayList<>();
				operationAsList.add(ldbcQuery.getClass().getName());
				operationAsList.add(ldbcQuery.personId());
				operationAsList.add(ldbcQuery.tagName());
				try {
					return OBJECT_MAPPER.writeValueAsString(operationAsList);
				} catch (IOException e) {
					throw new SerializingMarshallingException(
							format(ERROR_WHILE_TRYING_TO_SERIALIZE_RESULT_N_S, operationAsList), e);
				}
			}
			case LdbcQuery7.TYPE: {
				LdbcQuery7 ldbcQuery = (LdbcQuery7) operation;
				List<Object> operationAsList = new ArrayList<>();
				operationAsList.add(ldbcQuery.getClass().getName());
				operationAsList.add(ldbcQuery.personId());
				try {
					return OBJECT_MAPPER.writeValueAsString(operationAsList);
				} catch (IOException e) {
					throw new SerializingMarshallingException(
							format(ERROR_WHILE_TRYING_TO_SERIALIZE_RESULT_N_S, operationAsList), e);
				}
			}
			case LdbcQuery8.TYPE: {
				LdbcQuery8 ldbcQuery = (LdbcQuery8) operation;
				List<Object> operationAsList = new ArrayList<>();
				operationAsList.add(ldbcQuery.getClass().getName());
				operationAsList.add(ldbcQuery.personId());
				try {
					return OBJECT_MAPPER.writeValueAsString(operationAsList);
				} catch (IOException e) {
					throw new SerializingMarshallingException(
							format(ERROR_WHILE_TRYING_TO_SERIALIZE_RESULT_N_S, operationAsList), e);
				}
			}
			case LdbcQuery9.TYPE: {
				LdbcQuery9 ldbcQuery = (LdbcQuery9) operation;
				List<Object> operationAsList = new ArrayList<>();
				operationAsList.add(ldbcQuery.getClass().getName());
				operationAsList.add(ldbcQuery.personId());
				operationAsList.add(ldbcQuery.maxDate());
				try {
					return OBJECT_MAPPER.writeValueAsString(operationAsList);
				} catch (IOException e) {
					throw new SerializingMarshallingException(
							format(ERROR_WHILE_TRYING_TO_SERIALIZE_RESULT_N_S, operationAsList), e);
				}
			}
			case LdbcQuery10.TYPE: {
				LdbcQuery10 ldbcQuery = (LdbcQuery10) operation;
				List<Object> operationAsList = new ArrayList<>();
				operationAsList.add(ldbcQuery.getClass().getName());
				operationAsList.add(ldbcQuery.personId());
				operationAsList.add(ldbcQuery.month());
				try {
					return OBJECT_MAPPER.writeValueAsString(operationAsList);
				} catch (IOException e) {
					throw new SerializingMarshallingException(
							format(ERROR_WHILE_TRYING_TO_SERIALIZE_RESULT_N_S, operationAsList), e);
				}
			}
			case LdbcQuery11.TYPE: {
				LdbcQuery11 ldbcQuery = (LdbcQuery11) operation;
				List<Object> operationAsList = new ArrayList<>();
				operationAsList.add(ldbcQuery.getClass().getName());
				operationAsList.add(ldbcQuery.personId());
				operationAsList.add(ldbcQuery.countryName());
				operationAsList.add(ldbcQuery.workFromYear());
				try {
					return OBJECT_MAPPER.writeValueAsString(operationAsList);
				} catch (IOException e) {
					throw new SerializingMarshallingException(
							format(ERROR_WHILE_TRYING_TO_SERIALIZE_RESULT_N_S, operationAsList), e);
				}
			}
			case LdbcQuery12.TYPE: {
				LdbcQuery12 ldbcQuery = (LdbcQuery12) operation;
				List<Object> operationAsList = new ArrayList<>();
				operationAsList.add(ldbcQuery.getClass().getName());
				operationAsList.add(ldbcQuery.personId());
				operationAsList.add(ldbcQuery.tagClassName());
				try {
					return OBJECT_MAPPER.writeValueAsString(operationAsList);
				} catch (IOException e) {
					throw new SerializingMarshallingException(
							format(ERROR_WHILE_TRYING_TO_SERIALIZE_RESULT_N_S, operationAsList), e);
				}
			}
			case LdbcQuery13.TYPE: {
				LdbcQuery13 ldbcQuery = (LdbcQuery13) operation;
				List<Object> operationAsList = new ArrayList<>();
				operationAsList.add(ldbcQuery.getClass().getName());
				operationAsList.add(ldbcQuery.person1Id());
				operationAsList.add(ldbcQuery.person2Id());
				try {
					return OBJECT_MAPPER.writeValueAsString(operationAsList);
				} catch (IOException e) {
					throw new SerializingMarshallingException(
							format(ERROR_WHILE_TRYING_TO_SERIALIZE_RESULT_N_S, operationAsList), e);
				}
			}
			case LdbcQuery14.TYPE: {
				LdbcQuery14 ldbcQuery = (LdbcQuery14) operation;
				List<Object> operationAsList = new ArrayList<>();
				operationAsList.add(ldbcQuery.getClass().getName());
				operationAsList.add(ldbcQuery.person1Id());
				operationAsList.add(ldbcQuery.person2Id());
				try {
					return OBJECT_MAPPER.writeValueAsString(operationAsList);
				} catch (IOException e) {
					throw new SerializingMarshallingException(
							format(ERROR_WHILE_TRYING_TO_SERIALIZE_RESULT_N_S, operationAsList), e);
				}
			}

			default: {
				throw new SerializingMarshallingException(
						format(
								"Workload does not know how to serialize operation%nWorkload: %s%nOperation Type: " +
										"%s%nOperation: %s",
								getClass().getName(),
								operation.getClass().getName(),
								operation));
			}
		}
	}

	@Override
	public Operation marshalOperation(String serializedOperation) throws SerializingMarshallingException {
		List<Object> operationAsList;
		try {
			operationAsList = OBJECT_MAPPER.readValue(serializedOperation, TYPE_REFERENCE);
		} catch (IOException e) {
			throw new SerializingMarshallingException(
					format("Error while parsing serialized results%n%s", serializedOperation), e);
		}

		String operationTypeName = (String) operationAsList.get(0);
		if (operationTypeName.equals(LdbcQuery1.class.getName())) {
			long personId = LdbcUtils.convertToLong(operationAsList.get(1));
			String firstName = (String) operationAsList.get(2);
			return new LdbcQuery1(personId, firstName);
		}

		if (operationTypeName.equals(LdbcQuery2.class.getName())) {
			long personId = LdbcUtils.convertToLong(operationAsList.get(1));
			Date maxDate = LdbcUtils.convertDateTimeStringToDate(operationAsList.get(2));
			return new LdbcQuery2(personId, maxDate);
		}

		if (operationTypeName.equals(LdbcQuery3.class.getName())) {
			long personId = LdbcUtils.convertToLong(operationAsList.get(1));
			String countryXName = (String) operationAsList.get(2);
			String countryYName = (String) operationAsList.get(3);
			Date startDate = LdbcUtils.convertDateTimeStringToDate(operationAsList.get(4));
			int durationDays = ((Number) operationAsList.get(5)).intValue();
			return new LdbcQuery3(personId, countryXName, countryYName, startDate, durationDays);
		}

		if (operationTypeName.equals(LdbcQuery4.class.getName())) {
			long personId = LdbcUtils.convertToLong(operationAsList.get(1));
			Date startDate = LdbcUtils.convertDateTimeStringToDate(operationAsList.get(2));
			int durationDays = ((Number) operationAsList.get(3)).intValue();
			return new LdbcQuery4(personId, startDate, durationDays);
		}

		if (operationTypeName.equals(LdbcQuery5.class.getName())) {
			long personId = LdbcUtils.convertToLong(operationAsList.get(1));
			Date minDate = LdbcUtils.convertDateTimeStringToDate(operationAsList.get(2));
			return new LdbcQuery5(personId, minDate);
		}

		if (operationTypeName.equals(LdbcQuery6.class.getName())) {
			long personId = LdbcUtils.convertToLong(operationAsList.get(1));
			String tagName = (String) operationAsList.get(2);
			return new LdbcQuery6(personId, tagName);
		}

		if (operationTypeName.equals(LdbcQuery7.class.getName())) {
			long personId = LdbcUtils.convertToLong(operationAsList.get(1));
			return new LdbcQuery7(personId);
		}

		if (operationTypeName.equals(LdbcQuery8.class.getName())) {
			long personId = LdbcUtils.convertToLong(operationAsList.get(1));
			return new LdbcQuery8(personId);
		}

		if (operationTypeName.equals(LdbcQuery9.class.getName())) {
			long personId = LdbcUtils.convertToLong(operationAsList.get(1));
			Date maxDate = LdbcUtils.convertDateTimeStringToDate(operationAsList.get(2));
			return new LdbcQuery9(personId, maxDate);
		}

		if (operationTypeName.equals(LdbcQuery10.class.getName())) {
			long personId = LdbcUtils.convertToLong(operationAsList.get(1));
			int month = LdbcUtils.convertToInt(operationAsList.get(2));
			return new LdbcQuery10(personId, month);
		}

		if (operationTypeName.equals(LdbcQuery11.class.getName())) {
			long personId = LdbcUtils.convertToLong(operationAsList.get(1));
			String countryName = (String) operationAsList.get(2);
			int workFromYear = LdbcUtils.convertToInt(operationAsList.get(3));
			return new LdbcQuery11(personId, countryName, workFromYear);
		}

		if (operationTypeName.equals(LdbcQuery12.class.getName())) {
			long personId = LdbcUtils.convertToLong(operationAsList.get(1));
			String tagClassName = (String) operationAsList.get(2);
			return new LdbcQuery12(personId, tagClassName);
		}

		if (operationTypeName.equals(LdbcQuery13.class.getName())) {
			long person1Id = LdbcUtils.convertToLong(operationAsList.get(1));
			long person2Id = LdbcUtils.convertToLong(operationAsList.get(2));
			return new LdbcQuery13(person1Id, person2Id);
		}

		if (operationTypeName.equals(LdbcQuery14.class.getName())) {
			long person1Id = LdbcUtils.convertToLong(operationAsList.get(1));
			long person2Id = LdbcUtils.convertToLong(operationAsList.get(2));
			return new LdbcQuery14(person1Id, person2Id);
		}

		throw new SerializingMarshallingException(
				format(
						"Workload does not know how to marshal operation\nWorkload: %s\nAssumed Operation Type: " +
								"%s\nSerialized Operation: %s",
						getClass().getName(),
						operationTypeName,
						serializedOperation));
	}

    @Override
    protected BENCHMARK_MODE getBenchmarkMode() {
        return BENCHMARK_MODE.GRAPHDB_BENCHMARK_MODE;
    }

    private static final Equator<LdbcQuery14Result> LDBC_QUERY_14_RESULT_EQUATOR = new Equator<LdbcQuery14Result>() {
		@Override
		public boolean equate(LdbcQuery14Result result1, LdbcQuery14Result result2) {
			return result1.equals(result2);
		}

		@Override
		public int hash(LdbcQuery14Result result) {
			return 1;
		}
	};

	@Override
	public boolean resultsEqual(Operation operation, Object result1, Object result2) {
		if (null == result1 || null == result2) {
			return false;
		} else if (operation.type() == LdbcQuery14.TYPE) {
			List<LdbcQuery14Result> typedResults1 = (List<LdbcQuery14Result>) result1;
			Map<Double, List<LdbcQuery14Result>> results1ByWeight = new HashMap<>();
			for (LdbcQuery14Result typedResult : typedResults1) {
				List<LdbcQuery14Result> resultByWeight = results1ByWeight.get(typedResult.pathWeight());
				if (null == resultByWeight) {
					resultByWeight = new ArrayList<>();
				}
				resultByWeight.add(typedResult);
				results1ByWeight.put(typedResult.pathWeight(), resultByWeight);
			}

			List<LdbcQuery14Result> typedResults2 = (List<LdbcQuery14Result>) result2;
			Map<Double, List<LdbcQuery14Result>> results2ByWeight = new HashMap<>();
			for (LdbcQuery14Result typedResult : typedResults2) {
				List<LdbcQuery14Result> resultByWeight = results2ByWeight.get(typedResult.pathWeight());
				if (null == resultByWeight) {
					resultByWeight = new ArrayList<>();
				}
				resultByWeight.add(typedResult);
				results2ByWeight.put(typedResult.pathWeight(), resultByWeight);
			}

			// compare set of keys
			if (!results1ByWeight.keySet().equals(results2ByWeight.keySet())) {
				return false;
			}
			// convert list of lists to set of lists & compare contains all for set of lists for each key
			for (Double weight : results1ByWeight.keySet()) {
				if (results1ByWeight.get(weight).size() != results2ByWeight.get(weight).size()) {
					return false;
				}

				if (!CollectionUtils
						.isEqualCollection(results1ByWeight.get(weight), results2ByWeight.get(weight),
								LDBC_QUERY_14_RESULT_EQUATOR)) {
					return false;
				}
			}

			return true;
		} else {
			return result1.equals(result2);
		}
	}

	private void getQueries(Map<String, String> params) {
	}
}
