package com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive;

import com.ldbc.driver.Operation;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkloadConfiguration;
import com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive.queries.longreads.LdbcQuery1;
import com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive.queries.longreads.LdbcQuery2;
import com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive.queries.longreads.LdbcQuery3;
import com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive.queries.longreads.LdbcQuery4;
import com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive.queries.longreads.LdbcQuery5;
import com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive.queries.longreads.LdbcQuery6;
import com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive.queries.longreads.LdbcQuery7;
import com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive.queries.longreads.LdbcQuery8;
import com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive.queries.longreads.LdbcQuery9;
import com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive.queries.longreads.LdbcQuery10;
import com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive.queries.longreads.LdbcQuery11;
import com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive.queries.longreads.LdbcQuery12;
import com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive.queries.longreads.LdbcQuery13;
import com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive.queries.longreads.LdbcQuery14;


import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Arrays;

import static java.lang.String.format;

public class LdbcSnbInteractiveGraphDBWorkloadConfiguration {

	public static final String LDBC_GRAPHDB_INTERACTIVE_PACKAGE_PREFIX = "com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive.";
	public static final String LONG_READ_QUERIES_PACKAGE_PREFIX = "queries.longreads.";
	public static final String SHORT_READ_QUERIES_PACKAGE_PREFIX = "queries.shortreads.";
	public static final String WRITE_QUERIES_PACKAGE_PREFIX = "queries.writes.";

	public static final String LDBC_SNB_QUERY_DIR = "queryDir";

	 static Set<String> missingParameters(Map<String, String> properties, Iterable<String> compulsoryPropertyKeys) {
		Set<String> missingPropertyKeys = new HashSet<>();
		for (String compulsoryKey : compulsoryPropertyKeys) {
			if (null == properties.get(compulsoryKey)) {
				missingPropertyKeys.add(compulsoryKey);
			}
		}
		return missingPropertyKeys;
	}

	 static Map<Integer,Class<? extends Operation>> operationTypeToClassMapping() {
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

	static String removePrefix(String original, String prefix) {
		return (!original.contains(prefix)) ? original : original
				.substring(original.lastIndexOf(prefix) + prefix.length());
	}

	static String removeSuffix(String original, String suffix) {
		return (!original.contains(suffix)) ? original : original.substring(0, original.lastIndexOf(suffix));
	}

	static boolean isValidParser(String parserString) throws WorkloadException {
		try {
			LdbcSnbInteractiveWorkloadConfiguration.UpdateStreamParser parser = LdbcSnbInteractiveWorkloadConfiguration.
					UpdateStreamParser.valueOf(parserString);
			Set<LdbcSnbInteractiveWorkloadConfiguration.UpdateStreamParser> validParsers = new HashSet<>();
			validParsers.addAll(Arrays.asList(LdbcSnbInteractiveWorkloadConfiguration.UpdateStreamParser.values()));
			return validParsers.contains(parser);
		} catch (IllegalArgumentException e) {
			throw new WorkloadException(format("Unsupported parser value: %s", parserString), e);
		}
	}
}
