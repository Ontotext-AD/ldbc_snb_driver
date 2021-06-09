package com.ldbc.driver.workloads.common;

import com.github.jsonldjava.shaded.com.google.common.base.CharMatcher;
import com.ldbc.driver.DbException;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

public class LdbcUtils {
	private static final ValueFactory VF = SimpleValueFactory.getInstance();

	public static String appendZeroAtStart(long personId) {
		String personIdAsString = String.valueOf(personId);
		int maxLength = 20;

		StringBuilder sb = new StringBuilder(personIdAsString);
		if (sb.length() <= maxLength) {
			while (sb.length() < maxLength) {
				sb.insert(0, '0');
			}
		}
		return sb.toString();
	}

	public static String applyParameters(String queryString, Map<String, Object> queryParams) {
		for (Map.Entry<String, Object> param : queryParams.entrySet()) {
			String value = param.getValue().toString().trim();
			queryString = queryString.replace(param.getKey(), value);
		}

		return queryString;
	}

	public static IRI createIRI(String str) {
		return VF.createIRI(str);
	}

	public static Literal createLiteral(String str) {
		return VF.createLiteral(str);
	}

	public static String convertToDateAsString(Date date) {
		return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
				.format(date);
	}

	public static void loadQueriesFromDirectory(Map<Integer, String> queries, String queryDir) throws DbException {
		if (StringUtils.isNoneEmpty(queryDir)) {
			File dir = new File(queryDir);
			if (dir.exists()) {
				if (dir.isDirectory()) {
					for (File file : dir.listFiles()) {
						int queryNumber = Integer.parseInt(CharMatcher.digit().retainFrom(file.getName()));
						queries.put(queryNumber, loadResource(file.toPath()));
					}
				} else {
					throw new DbException("The provided path is not directory");
				}
			} else {
				throw new DbException("The provided query directory does not exist");
			}
		} else {
			throw new DbException("Query directory is not provided");
		}
	}

	private static String loadResource(Path path) throws DbException {
		try {
			byte[] encoded = Files.readAllBytes(path);
			return new String(encoded, StandardCharsets.UTF_8);
		} catch (IOException e) {
			throw new DbException("Could not load query from file", e);
		}
	}
}
