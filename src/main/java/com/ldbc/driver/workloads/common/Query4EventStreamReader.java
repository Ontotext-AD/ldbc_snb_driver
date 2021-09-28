package com.ldbc.driver.workloads.common;


import com.ldbc.driver.Operation;
import com.ldbc.driver.Workload;
import com.ldbc.driver.csv.charseeker.CharSeeker;
import com.ldbc.driver.csv.charseeker.Extractors;
import com.ldbc.driver.csv.charseeker.Mark;
import com.ldbc.driver.generator.CsvEventStreamReaderBasicCharSeeker;
import com.ldbc.driver.generator.GeneratorException;
import com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive.LdbcQuery4;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;

import static java.lang.String.format;

public class Query4EventStreamReader implements Iterator<Operation> {
    private final Workload.BENCHMARK_MODE benchmarkMode;
	private final Iterator<Object[]> csvRows;

	public Query4EventStreamReader(Iterator<Object[]> csvRows) {
		this(csvRows, Workload.BENCHMARK_MODE.DEFAULT_BENCHMARK_MODE);
	}

	public Query4EventStreamReader(Iterator<Object[]> csvRows, Workload.BENCHMARK_MODE mode) {
		this.csvRows = csvRows;
		this.benchmarkMode = mode;
	}

	@Override
	public boolean hasNext() {
		return csvRows.hasNext();
	}

	@Override
	public Operation next() {
		Object[] rowAsObjects = csvRows.next();
		long personId = (long) rowAsObjects[0];
		Date startDate = (Date) rowAsObjects[1];
		int durationDays = (int) rowAsObjects[2];

		Operation operation =
				benchmarkMode == Workload.BENCHMARK_MODE.DEFAULT_BENCHMARK_MODE ?
						new com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery4(personId, startDate, durationDays, com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery4.DEFAULT_LIMIT) :
						new LdbcQuery4(personId, startDate, durationDays);
		operation.setDependencyTimeStamp(0);
		return operation;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException(format("%s does not support remove()", getClass().getSimpleName()));
	}

	public static class Query4Decoder implements CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> {
		/*
		personId|startDate|durationDays
		15393164964332|1349049600|34
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

			Date date;
			if (charSeeker.seek(mark, columnDelimiters)) {
				date = new Date(charSeeker.extract(mark, extractors.long_()).longValue());
			} else {
				throw new GeneratorException("Error retrieving date");
			}

			int durationDays;
			if (charSeeker.seek(mark, columnDelimiters)) {
				durationDays = charSeeker.extract(mark, extractors.int_()).intValue();
			} else {
				throw new GeneratorException("Error retrieving duration days");
			}

			return new Object[]{personId, date, durationDays};
		}
	}
}
