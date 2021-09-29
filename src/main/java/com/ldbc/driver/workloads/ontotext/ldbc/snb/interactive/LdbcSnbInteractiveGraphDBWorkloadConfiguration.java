package com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive;

import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkloadConfiguration;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.lang.String.format;

public class LdbcSnbInteractiveGraphDBWorkloadConfiguration {
	public static final String LDBC_GRAPHDB_INTERACTIVE_PACKAGE_PREFIX = "com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive.";
	public static final String LDBC_SNB_QUERY_DIR = "queryDir";

	public static Set<String> missingParameters(Map<String, String> properties, Iterable<String> compulsoryPropertyKeys) {
		Set<String> missingPropertyKeys = new HashSet<>();
		for (String compulsoryKey : compulsoryPropertyKeys) {
			if (null == properties.get(compulsoryKey)) {
				missingPropertyKeys.add(compulsoryKey);
			}
		}
		return missingPropertyKeys;
	}

	public static String removePrefix(String original, String prefix) {
		return (!original.contains(prefix)) ? original : original
				.substring(original.lastIndexOf(prefix) + prefix.length(), original.length());
	}

	public static String removeSuffix(String original, String suffix) {
		return (!original.contains(suffix)) ? original : original.substring(0, original.lastIndexOf(suffix));
	}

	public static boolean isValidParser(String parserString) throws WorkloadException {
		try {
			LdbcSnbInteractiveWorkloadConfiguration.UpdateStreamParser parser = LdbcSnbInteractiveWorkloadConfiguration.UpdateStreamParser.valueOf(parserString);
			Set<LdbcSnbInteractiveWorkloadConfiguration.UpdateStreamParser> validParsers = new HashSet<>();
			validParsers.addAll(Arrays.asList(LdbcSnbInteractiveWorkloadConfiguration.UpdateStreamParser.values()));
			return validParsers.contains(parser);
		} catch (IllegalArgumentException e) {
			throw new WorkloadException(format("Unsupported parser value: %s", parserString), e);
		}
	}
}
