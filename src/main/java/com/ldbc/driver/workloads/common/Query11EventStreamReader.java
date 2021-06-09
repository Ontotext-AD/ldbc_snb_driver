package com.ldbc.driver.workloads.common;


import com.ldbc.driver.Operation;
import com.ldbc.driver.Workload;
import com.ldbc.driver.csv.charseeker.CharSeeker;
import com.ldbc.driver.csv.charseeker.Extractors;
import com.ldbc.driver.csv.charseeker.Mark;
import com.ldbc.driver.generator.CsvEventStreamReaderBasicCharSeeker;
import com.ldbc.driver.generator.GeneratorException;
import com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive.LdbcQuery11;

import java.io.IOException;
import java.util.Iterator;

import static java.lang.String.format;

public class Query11EventStreamReader implements Iterator<Operation<?>> {
	private final Workload.BENCHMARK_MODE benchmarkMode;
	private final Iterator<Object[]> csvRows;

	public Query11EventStreamReader(Iterator<Object[]> csvRows) {
		this(csvRows, Workload.BENCHMARK_MODE.DEFAULT_BENCHMARK_MODE);
	}

	public Query11EventStreamReader(Iterator<Object[]> csvRows, Workload.BENCHMARK_MODE mode) {
		this.csvRows = csvRows;
		this.benchmarkMode = mode;
	}

	@Override
	public boolean hasNext() {
		return csvRows.hasNext();
	}

	@Override
	public Operation<?> next() {
		Object[] rowAsObjects = csvRows.next();
		long personId = (long) rowAsObjects[0];
		String countryName = (String) rowAsObjects[1];
		int workFromYear = (int) rowAsObjects[2];
		Operation<?> operation =
				benchmarkMode == Workload.BENCHMARK_MODE.DEFAULT_BENCHMARK_MODE ?
						new com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery11(personId, countryName, workFromYear, com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery11.DEFAULT_LIMIT) :
						new LdbcQuery11(personId, countryName, workFromYear);
		operation.setDependencyTimeStamp(0);
		return operation;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException(format("%s does not support remove()", getClass().getSimpleName()));
	}

	public static class Query11Decoder implements CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> {
		/*
		personId|countryName|workFromYear
		2199032251700|Egypt|1995
		*/
		@Override
		public Object[] decodeEvent(CharSeeker charSeeker, Extractors extractors, int[] columnDelimiters, Mark mark)
				throws IOException {
			long personId;
			if (charSeeker.seek(mark, columnDelimiters)) {
				personId = charSeeker.extract(mark, extractors.long_()).longValue();
			} else {
				// if first column of next row contains nothing it means the file is finished
				return null;
			}

			String countryName;
			if (charSeeker.seek(mark, columnDelimiters)) {
				countryName = charSeeker.extract(mark, extractors.string()).value();
			} else {
				throw new GeneratorException("Error retrieving country name");
			}

			int year;
			if (charSeeker.seek(mark, columnDelimiters)) {
				year = charSeeker.extract(mark, extractors.int_()).intValue();
			} else {
				throw new GeneratorException("Error retrieving year");
			}

			return new Object[]{personId, countryName, year};
		}
	}
}
