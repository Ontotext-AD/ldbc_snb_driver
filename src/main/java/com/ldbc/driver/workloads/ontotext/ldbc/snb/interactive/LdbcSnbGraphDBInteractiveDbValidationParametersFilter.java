package com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive;

import com.ldbc.driver.Operation;
import com.ldbc.driver.Workload;
import com.ldbc.driver.Workload.DbValidationParametersFilter;

import java.util.Map;
import java.util.Set;

public class LdbcSnbGraphDBInteractiveDbValidationParametersFilter implements DbValidationParametersFilter {

	private final Set<Class> multiResultOperations;
	private final Map<Class, Long> remainingRequiredResultsPerWriteType;
	private final Map<Class, Long> remainingRequiredResultsPerLongReadType;
	private final Set<Class> enabledShortReadOperationTypes;
	private long writeAddPersonOperationCount;
	private int uncompletedShortReads;

	LdbcSnbGraphDBInteractiveDbValidationParametersFilter(Set<Class> multiResultOperations,
			long writeAddPersonOperationCount,
			Map<Class, Long> remainingRequiredResultsPerWriteType,
			Map<Class, Long> remainingRequiredResultsPerLongReadType,
			Set<Class> enabledShortReadOperationTypes) {
		this.multiResultOperations = multiResultOperations;
		this.writeAddPersonOperationCount = writeAddPersonOperationCount;
		this.remainingRequiredResultsPerWriteType = remainingRequiredResultsPerWriteType;
		this.remainingRequiredResultsPerLongReadType = remainingRequiredResultsPerLongReadType;
		this.enabledShortReadOperationTypes = enabledShortReadOperationTypes;
		this.uncompletedShortReads = 0;
	}

	@Override
	public boolean useOperation(Operation operation) {
		return false;
	}

	@Override
	public Workload.DbValidationParametersFilterResult useOperationAndResultForValidation(Operation operation, Object operationResult) {
		return null;
	}
}
