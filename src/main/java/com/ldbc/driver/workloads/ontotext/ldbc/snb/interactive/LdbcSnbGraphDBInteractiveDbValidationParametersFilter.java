package com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive;

import java.util.Map;
import java.util.Set;

public class LdbcSnbGraphDBInteractiveDbValidationParametersFilter {

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
}
