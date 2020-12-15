package com.ldbc.driver.workloads.ldbc.snb.bi;


import com.ldbc.driver.Operation;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.csv.charseeker.CharSeeker;
import com.ldbc.driver.csv.charseeker.CharSeekerParams;
import com.ldbc.driver.csv.charseeker.Extractors;
import com.ldbc.driver.csv.charseeker.Mark;
import com.ldbc.driver.generator.CsvEventStreamReaderBasicCharSeeker;
import com.ldbc.driver.generator.GeneratorException;
import com.ldbc.driver.generator.GeneratorFactory;

import java.io.IOException;
import java.io.InputStream;

public class BiQuery2EventStreamReader extends BaseEventStreamReader
{
    public BiQuery2EventStreamReader(
            InputStream parametersInputStream,
            CharSeekerParams charSeekerParams,
            GeneratorFactory gf ) throws WorkloadException
    {
        super( parametersInputStream, charSeekerParams, gf );
    }

    @Override
    Operation operationFromParameters( Object[] parameters )
    {
        return new LdbcSnbBiQuery2TagEvolution(
                (int) parameters[0],
                (int) parameters[1],
                (String) parameters[2],
                (int) parameters[3]
        );
    }

    @Override
    CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder()
    {
        return new CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]>()
        {
            @Override
            public Object[] decodeEvent( CharSeeker charSeeker, Extractors extractors, int[] columnDelimiters,
                    Mark mark )
                    throws IOException
            {
                int year;
                if ( charSeeker.seek( mark, columnDelimiters ) )
                {
                    year = charSeeker.extract( mark, extractors.int_() ).intValue();
                }
                else
                {
                    // if first column of next row contains nothing it means the file is finished
                    return null;
                }

                int month;
                if ( charSeeker.seek( mark, columnDelimiters ) )
                {
                    month = charSeeker.extract( mark, extractors.int_() ).intValue();
                }
                else
                {
                    throw new GeneratorException( "Error retrieving month" );
                }

                String tagClass;
                if ( charSeeker.seek( mark, columnDelimiters ) )
                {
                    tagClass = charSeeker.extract( mark, extractors.string() ).value();
                }
                else
                {
                    throw new GeneratorException( "Error retrieving tagClass name" );
                }

                return new Object[]{
                        year,
                        month,
                        tagClass,
                        LdbcSnbBiQuery2TagEvolution.DEFAULT_LIMIT
                };
            }
        };
    }

    @Override
    int columnCount()
    {
        return 3;
    }
}
