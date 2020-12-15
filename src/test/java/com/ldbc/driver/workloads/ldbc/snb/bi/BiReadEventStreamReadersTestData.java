package com.ldbc.driver.workloads.ldbc.snb.bi;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

public class BiReadEventStreamReadersTestData
{
    public static final SimpleDateFormat DATE_FORMAT;

    static
    {
        DATE_FORMAT = new SimpleDateFormat( "yyyy-MM-dd" );
        DATE_FORMAT.setTimeZone( TimeZone.getTimeZone( "GMT" ) );
    }

    public static final String QUERY_1_CSV_ROWS()
    {
        return "date\n"
               + "1441351591755\n"
               + "1441351591756";
    }

    public static final String QUERY_2_CSV_ROWS() throws ParseException
    {
        return "year|month|tagClass\n"
               + "1|2|tc1\n"
               + "3|4|tc2\n";
    }

    public static final String QUERY_3_CSV_ROWS() throws ParseException
    {
        return "tagClass|country\n"
               + "Writer|Cameroon\n"
               + "Writer|Colombia\n"
               + "Writer|Niger\n"
               + "Writer|Sweden\n";
    }

    public static final String QUERY_4_CSV_ROWS() throws ParseException
    {
        return "country\n"
               + "Kenya\n"
               + "Peru\n"
               + "Tunisia\n"
               + "Venezuela";
    }

    public static final String QUERY_5_CSV_ROWS()
    {
        return "tag\n"
               + "Justin_Timberlake\n"
               + "Josip_Broz_Tito\n"
               + "Barry_Manilow\n"
               + "Charles_Darwin";
    }

    public static final String QUERY_6_CSV_ROWS()
    {
        return "tag\n"
               + "Franz_Schubert\n"
               + "Bill_Clinton\n"
               + "Dante_Alighieri\n"
               + "Khalid_Sheikh_Mohammed";
    }

    public static final String QUERY_7_CSV_ROWS()
    {
        return "tag\n"
               + "Alanis_Morissette\n"
               + "\u00c9amon_de_Valera\n"
               + "Juhi_Chawla\n"
               + "Manuel_Noriega";
    }

    public static final String QUERY_8_CSV_ROWS()
    {
        return "tag|date\n"
               + "Franz_Schubert|1\n"
               + "Bill_Clinton|2\n"
               + "Dante_Alighieri|3\n"
               + "Khalid_Sheikh_Mohammed|4";
    }

    public static final String QUERY_9_CSV_ROWS()
    {
        return "begin|end\n"
               + "1441351591755|1441351591756\n"
               + "1441351591756|1441351591757";
    }

    public static final String QUERY_10_CSV_ROWS()
    {
        return "personId|country|tagClass|minPathDistance|maxPathDistance\n"
               + "1|Cameroon|Writer|1|1\n"
               + "2|Colombia|Writer|1|2\n"
               + "3|Niger|Writer|2|2\n"
               + "4|Sweden|Writer|1|4";
    }

    public static final String QUERY_11_CSV_ROWS()
    {
        return "country\n"
               + "Kenya\n"
               + "Peru\n"
               + "Tunisia\n"
               + "Venezuela";
    }

    public static final String QUERY_12_CSV_ROWS()
    {
        return "creationDate|lengthThreshold|languages\n"
               + "1441351591755|1|en\n"
               + "1441351591756|2|en;fr";
    }

    public static final String QUERY_13_CSV_ROWS()
    {
        return "country|endDate\n"
               + "Kenya|1\n"
               + "Peru|2\n"
               + "Tunisia|3\n"
               + "Venezuela|4";
    }

    public static final String QUERY_14_CSV_ROWS()
    {
        return "country1|country2\n"
               + "Germany|Pakistan\n"
               + "Germany|Russia\n"
               + "Germany|Vietnam\n"
               + "Germany|Philippines\n";
    }

    public static final String QUERY_15_CSV_ROWS()
    {
        return "person1Id|person2Id|startDate|endDate\n"
                + "1|2|1|2\n"
                + "3|4|3|4\n";
    }

    public static final String QUERY_16_CSV_ROWS()
    {
        return "tagA|dateA|tagB|dateB|maxKnowsLimit|limit\n"
                + "Franz_Liszt|2|Fernando_González|3|4|20\n"
                + "Dante_Alighieri|3|Franz_Schubert|9|4|10\n";
    }

    public static final String QUERY_17_CSV_ROWS()
    {
        return "tag|delta|limit\n"
                + "Dante_Alighieri|3|10\n"
                + "Franz_Liszt|5|20\n";
    }

    public static final String QUERY_18_CSV_ROWS()
    {
        return "person1Id|tag|limit\n"
                + "1|Dante_Alighieri|30\n"
                + "2|Franz_Schubert|40\n";
    }

    public static final String QUERY_19_CSV_ROWS()
    {
        return "city1Id|city2Id\n"
                + "1|2\n"
                + "2|3\n";
    }

    public static final String QUERY_20_CSV_ROWS()
    {
        return "company|person2Id\n"
                + "BigCorp|1\n"
                + "LargeComp|3\n";
    }
}
