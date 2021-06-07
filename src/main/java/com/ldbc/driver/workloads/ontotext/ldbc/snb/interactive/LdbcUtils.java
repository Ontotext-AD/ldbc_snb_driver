package com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive;

import org.apache.commons.io.IOUtils;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

import java.io.IOException;
import java.io.InputStream;
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
}
