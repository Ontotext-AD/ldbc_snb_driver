package com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive.readers;

import com.ldbc.driver.Operation;
import com.ldbc.driver.csv.charseeker.CharSeeker;
import com.ldbc.driver.csv.charseeker.Extractors;
import com.ldbc.driver.csv.charseeker.Mark;
import com.ldbc.driver.generator.CsvEventStreamReaderBasicCharSeeker;
import com.ldbc.driver.generator.GeneratorException;
import com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive.queries.longreads.LdbcQuery5;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;

import static java.lang.String.format;

public class Query5EventStreamReader implements Iterator<Operation> {
	private final Iterator<Object[]> csvRows;

	public Query5EventStreamReader(Iterator<Object[]> csvRows) {
		this.csvRows = csvRows;
	}

	@Override
	public boolean hasNext() {
		return csvRows.hasNext();
	}

	@Override
	public Operation<?> next() {
		Object[] rowAsObjects = csvRows.next();
		long personId = (long) rowAsObjects[0];
		Date minDate = (Date) rowAsObjects[1];
		Operation<?> operation = new LdbcQuery5(personId, minDate);
		operation.setDependencyTimeStamp(0);
		return operation;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException(format("%s does not support remove()", getClass().getSimpleName()));
	}

	public static class Query5Decoder implements CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> {
		/*
		personId|minDate
		7696581543848|1343952000
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

			return new Object[]{personId, date};
		}
	}
}
