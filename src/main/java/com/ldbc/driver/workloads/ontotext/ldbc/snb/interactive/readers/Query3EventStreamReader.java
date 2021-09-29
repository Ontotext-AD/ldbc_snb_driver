package com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive.readers;


import com.ldbc.driver.Operation;
import com.ldbc.driver.Workload;
import com.ldbc.driver.csv.charseeker.CharSeeker;
import com.ldbc.driver.csv.charseeker.Extractors;
import com.ldbc.driver.csv.charseeker.Mark;
import com.ldbc.driver.generator.CsvEventStreamReaderBasicCharSeeker;
import com.ldbc.driver.generator.GeneratorException;
import com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive.LdbcQuery3;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;

import static java.lang.String.format;

public class Query3EventStreamReader implements Iterator<Operation> {
    private final Iterator<Object[]> csvRows;

	public Query3EventStreamReader(Iterator<Object[]> csvRows) {
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
        String countryXName = (String) rowAsObjects[3];
        String countryYName = (String) rowAsObjects[4];
        Date startDate = (Date) rowAsObjects[1];
        int durationDays = (int) rowAsObjects[2];

        Operation<?> operation = new LdbcQuery3(personId, countryXName, countryYName, startDate, durationDays);
		operation.setDependencyTimeStamp(0);
		return operation;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException(format("%s does not support remove()", getClass().getSimpleName()));
	}

	public static class Query3Decoder implements CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> {
		/*
		personId|startDate|durationDays|countryXName|countryYName
		7696581543848|1293840000|28|Egypt|Sri_Lanka
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

			String countryX;
			if (charSeeker.seek(mark, columnDelimiters)) {
				countryX = charSeeker.extract(mark, extractors.string()).value();
			} else {
				throw new GeneratorException("Error retrieving countryX");
			}

			String countryY;
			if (charSeeker.seek(mark, columnDelimiters)) {
				countryY = charSeeker.extract(mark, extractors.string()).value();
			} else {
				throw new GeneratorException("Error retrieving countryY");
			}

			return new Object[]{personId, date, durationDays, countryX, countryY};
		}
	}
}
