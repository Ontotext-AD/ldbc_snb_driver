package com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive;

import com.ldbc.driver.Db;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.Operation;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.control.LoggingService;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.http.HTTPRepository;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static java.lang.String.format;

public class LdbcSnbInteractiveGraphDb extends Db {

//	private static String QUERY_1 = loadResource("./graphdb/query1.rq");
//	private static String QUERY_2 = loadResource("graphdb/query2.rq");
//	private static String QUERY_3 = loadResource("graphdb/query3.rq");
//	private static String QUERY_4 = loadResource("graphdb/query4.rq");
//	private static String QUERY_5 = loadResource("graphdb/query5.rq");
//	private static String QUERY_6 = loadResource("graphdb/query6.rq");
//	private static String QUERY_7 = loadResource("graphdb/query7.rq");
//	private static String QUERY_8 = loadResource("graphdb/query8.rq");
//	private static String QUERY_9 = loadResource("graphdb/query9.rq");
//	private static String QUERY_10 = loadResource("graphdb/query10.rq");
//	private static String QUERY_11 = loadResource("graphdb/query11.rq");
//	private static String QUERY_12 = loadResource("graphdb/query12.rq");
//	private static String QUERY_13 = loadResource("graphdb/query13.rq");
//	private static String QUERY_14 = loadResource("graphdb/query14.rq");

	private static String QUERY_1 = "PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" +
			"PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n" +
			"PREFIX sn: <http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" +
			"select ?fr ?last (min(?dist) as ?mindist) (group_concat(distinct ?email;\n" +
			"        separator=\", \") as ?emails) (group_concat(distinct ?lng;\n" +
			"        separator=\", \") as ?lngs) ?based (group_concat(distinct concat(?study_name, \" \", str(?study_year), \" \", ?study_country);\n" +
			"        separator=\", \") as ?studyAt) (group_concat(distinct concat(?work_name, \" \", str(?work_year), \" \", ?work_country);\n" +
			"        separator=\", \") as ?workAt) ?bday ?since ?gen ?browser ?locationIP\n" +
			"where {\n" +
			"    ?fr snvoc:email ?email .\n" +
			"    ?fr snvoc:speaks ?lng .\n" +
			"    {\n" +
			"        ?fr snvoc:studyAt ?study .\n" +
			"        ?study snvoc:classYear ?study_year .\n" +
			"        ?study snvoc:hasOrganisation ?study_org .\n" +
			"        ?study_org snvoc:isLocatedIn ?study_countryURI.\n" +
			"        ?study_countryURI foaf:name ?study_country .\n" +
			"        ?study_org foaf:name ?study_name .\n" +
			"    }\n" +
			"    {\n" +
			"        ?fr snvoc:workAt ?work .\n" +
			"        ?work snvoc:workFrom ?work_year .\n" +
			"        ?work snvoc:hasOrganisation ?work_org .\n" +
			"        ?work_org snvoc:isLocatedIn ?work_countryURI.\n" +
			"        ?work_countryURI foaf:name ?work_country .\n" +
			"        ?work_org foaf:name ?work_name .\n" +
			"    }\n" +
			"    ?fr a snvoc:Person .\n" +
			"    ?fr snvoc:firstName \"%Name%\" .\n" +
			"    ?fr snvoc:lastName ?last .\n" +
			"    ?fr snvoc:birthday ?bday .\n" +
			"    ?fr snvoc:isLocatedIn ?basedURI .\n" +
			"    ?basedURI foaf:name ?based .\n" +
			"    ?fr snvoc:creationDate ?since .\n" +
			"    ?fr snvoc:gender ?gen .\n" +
			"    ?fr snvoc:locationIP ?locationIP .\n" +
			"    ?fr snvoc:browserUsed ?browser .\n" +
			"    {\n" +
			"        {\n" +
			"            select distinct ?fr (1 as ?dist)\n" +
			"            where {\n" +
			"                sn:pers%Person% snvoc:knows/snvoc:hasPerson ?fr.\n" +
			"            }\n" +
			"        }\n" +
			"        union\n" +
			"        {\n" +
			"            select distinct ?fr (2 as ?dist)\n" +
			"            where {\n" +
			"                sn:pers%Person% snvoc:knows/snvoc:hasPerson ?fr2.\n" +
			"                ?fr2 snvoc:knows/snvoc:hasPerson ?fr.\n" +
			"                filter (?fr != sn:pers%Person%).\n" +
			"            }\n" +
			"        }\n" +
			"        union\n" +
			"        {\n" +
			"            select distinct ?fr (3 as ?dist)\n" +
			"            where {\n" +
			"                sn:pers%Person% snvoc:knows/snvoc:hasPerson ?fr2.\n" +
			"                ?fr2 snvoc:knows/snvoc:hasPerson ?fr3.\n" +
			"                ?fr3 snvoc:knows/snvoc:hasPerson ?fr.\n" +
			"                filter (?fr != sn:pers%Person%).\n" +
			"            }\n" +
			"        } .\n" +
			"    }\n" +
			"}\n" +
			"group by ?fr ?last ?bday ?since ?gen ?browser ?locationIP ?based\n" +
			"order by ?mindist ?last ?fr\n" +
			"limit 20";
	private static String QUERY_2 = "PREFIX sn: <http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" +
			"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" +
			"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
			"\n" +
			"select ?fr ?first ?last ?post ?content ?date\n" +
			"where {\n" +
			"\tsn:pers%Person% snvoc:knows/snvoc:hasPerson ?fr.\n" +
			"\t?fr snvoc:firstName ?first. \n" +
			"\t?fr snvoc:lastName ?last .\n" +
			"\t?post snvoc:hasCreator ?fr.\n" +
			"\t{ \n" +
			"\t  {?post snvoc:content ?content } \n" +
			"\t  union \n" +
			"\t  { ?post snvoc:imageFile ?content }\n" +
			"\t} .\n" +
			"\t?post snvoc:creationDate ?date.\n" +
			"\tfilter (?date <= \"%Date0%\"^^xsd:dateTime).\n" +
			"}\n" +
			"order by desc (?date) ?post\n" +
			"limit 20";
	private static String QUERY_3 = "PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" +
			"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
			"PREFIX dbpedia: <http://dbpedia.org/resource/>\n" +
			"PREFIX sn: <http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" +
			"\n" +
			"select distinct ?fr ?first ?last ?ct1 ?ct2 ((?ct1 + ?ct2) as ?sum)\n" +
			"where {\n" +
			"    {\n" +
			"        {\n" +
			"            select (count (*) as ?ct1)\n" +
			"            where {\n" +
			"                ?post snvoc:hasCreator ?fr .\n" +
			"                ?post snvoc:creationDate ?date .\n" +
			"                bind (\"%Date0%\"^^xsd:dateTime + \"P%Duration%D\"^^xsd:duration as ?dateAdded).\n" +
			"                filter (?date >= \"%Date0%\"^^xsd:dateTime && ?date < ?dateAdded) .\n" +
			"                ?post snvoc:isLocatedIn dbpedia:%Country1%.\n" +
			"            }\n" +
			"        }\n" +
			"        {\n" +
			"            select (count (*) as ?ct2)\n" +
			"            where {\n" +
			"                ?post2 snvoc:hasCreator ?fr .\n" +
			"                ?post2 snvoc:creationDate ?date2 .\n" +
			"                bind (\"%Date0%\"^^xsd:dateTime + \"P%Duration%D\"^^xsd:duration as ?dateAdded).\n" +
			"                filter (?date2 >= \"%Date0%\"^^xsd:dateTime && ?date2 < ?dateAdded) .\n" +
			"                ?post2 snvoc:isLocatedIn dbpedia:%Country2%.\n" +
			"            }\n" +
			"        }\n" +
			"        \n" +
			"        {\n" +
			"\t    sn:pers%Person% snvoc:knows/snvoc:hasPerson ?fr.\n" +
			"        } union {\n" +
			"            sn:pers%Person% snvoc:knows/snvoc:hasPerson ?fr1.\n" +
			"            ?fr1 snvoc:knows/snvoc:hasPerson ?fr.\n" +
			"            filter (?fr != sn:pers%Person%)\n" +
			"        }\n" +
			"        ?fr snvoc:firstName ?first .\n" +
			"        ?fr snvoc:lastName ?last .\n" +
			"        ?fr snvoc:isLocatedIn ?city .\n" +
			"        filter(!exists {\n" +
			"                ?city snvoc:isPartOf dbpedia:%Country1%\n" +
			"            }).\n" +
			"        filter(!exists {\n" +
			"                ?city snvoc:isPartOf dbpedia:%Country2%\n" +
			"            }).\n" +
			"    }\n" +
			"    filter (?ct1 > 0 && ?ct2 > 0) .\n" +
			"}\n" +
			"order by desc(6) ?fr\n" +
			"limit 20";
	private static String QUERY_4 = "PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" +
			"PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n" +
			"PREFIX sn: <http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" +
			"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
			"select ?tagname (count (*) as ?count) \n" +
			"where {\n" +
			"    ?post a snvoc:Post .\n" +
			"    ?post snvoc:hasCreator ?fr .\n" +
			"    ?post snvoc:hasTag ?tag .\n" +
			"    ?tag foaf:name ?tagname .\n" +
			"    ?post snvoc:creationDate ?date .\n" +
			"    sn:pers%Person% snvoc:knows/snvoc:hasPerson ?fr .\n" +
			"    bind (\"%Date0%\"^^xsd:dateTime + \"P%Duration%D\"^^xsd:duration as ?dateAdded).\n" +
			"    filter (?date >= \"%Date0%\"^^xsd:dateTime && ?date <= ?dateAdded) .\n" +
			"    filter (!exists {\n" +
			"            sn:pers%Person% snvoc:knows/snvoc:hasPerson ?fr2 .\n" +
			"            ?post2 snvoc:hasCreator ?fr2 .\n" +
			"            ?post2 snvoc:hasTag ?tag .\n" +
			"            ?post2 snvoc:creationDate ?date2 .\n" +
			"            filter (?date2 < \"%Date0%\"^^xsd:dateTime)\n" +
			"        })\n" +
			"}\n" +
			"group by ?tagname\n" +
			"order by desc(2) ?tagname\n" +
			"limit 10";
	private static String QUERY_5 = "PREFIX sn: <http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" +
			"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" +
			"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
			"select ?title (count (*) as ?count)\n" +
			"where {\n" +
			"    {\n" +
			"        select distinct ?fr\n" +
			"        where {\n" +
			"            {\n" +
			"                sn:pers%Person% snvoc:knows/snvoc:hasPerson ?fr.\n" +
			"            }\n" +
			"            union {\n" +
			"                sn:pers%Person% snvoc:knows/snvoc:hasPerson ?fr2.\n" +
			"                ?fr2 snvoc:knows/snvoc:hasPerson ?fr.\n" +
			"                filter (?fr != sn:pers%Person%)\n" +
			"            }\n" +
			"        }\n" +
			"    } \n" +
			"    ?group snvoc:hasMember ?mem .\n" +
			"    ?mem snvoc:hasPerson ?fr .\n" +
			"    ?mem snvoc:joinDate ?date .\n" +
			"    filter (?date >= \"%Date0%\"^^xsd:dateTime) .\n" +
			"    ?post snvoc:hasCreator ?fr .\n" +
			"    ?group snvoc:containerOf ?post .\n" +
			"    ?group snvoc:title ?title.\n" +
			"}\n" +
			"group by ?title\n" +
			"order by desc(2) ?title\n" +
			"limit 20";
	private static String QUERY_6 = "PREFIX sn: <http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" +
			"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" +
			"PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n" +
			"select ?tagname (count (*) as ?count)\n" +
			"where {\n" +
			"    {\n" +
			"        select distinct ?fr\n" +
			"        where {\n" +
			"            {\n" +
			"                sn:pers%Person% snvoc:knows/snvoc:hasPerson ?fr.\n" +
			"            }\n" +
			"            union {\n" +
			"                sn:pers%Person% snvoc:knows/snvoc:hasPerson ?fr2.\n" +
			"                ?fr2 snvoc:knows/snvoc:hasPerson ?fr.\n" +
			"                filter (?fr != sn:pers%Person%)\n" +
			"            }\n" +
			"        }\n" +
			"    } .\n" +
			"    ?post snvoc:hasCreator ?fr .\n" +
			"    ?post snvoc:hasTag ?tag1 .\n" +
			"    ?tag1 foaf:name ?tagname1 .\n" +
			"    filter (?tagname1 != \"%Tag%\") .\n" +
			"    ?post snvoc:hasTag ?tag .\n" +
			"    ?tag foaf:name ?tagname .\n" +
			"}\n" +
			"group by ?tagname\n" +
			"order by desc(2) ?tagname\n" +
			"limit 10";
	private static String QUERY_7 = "PREFIX sn: <http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" +
			"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" +
			"PREFIX ofn:<http://www.ontotext.com/sparql/functions/>\n" +
			"\n" +
			"select ?liker ?first ?last ?post ?content ?is_new ?lag ?ldt\n" +
			"where {\n" +
			"    ?post snvoc:hasCreator sn:pers%Person% .\n" +
			"    {\n" +
			"        {\n" +
			"            ?post snvoc:content ?content \n" +
			"        } union {\n" +
			"            ?post snvoc:imageFile ?content\n" +
			"        }\n" +
			"    } .\n" +
			"    ?lk snvoc:hasPost ?post .\n" +
			"    ?liker snvoc:likes ?lk .\n" +
			"    ?liker snvoc:firstName ?first .\n" +
			"    ?liker snvoc:lastName ?last .\n" +
			"    ?post snvoc:creationDate ?dt .\n" +
			"    ?lk snvoc:creationDate ?ldt .\n" +
			"    \n" +
			"    sn:pers%Person% snvoc:knows ?likerNode.\n" +
			"    bind(if (exists{?likerNode snvoc:hasPerson ?liker}, 0, 1) as ?is_new).\n" +
			"    bind(ofn:minutesBetween(?dt, ?ldt) as ?lag).\n" +
			"}\n" +
			"order by desc (?ldt) ?liker\n" +
			"limit 20";
	private static String QUERY_8 = "PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" +
			"PREFIX sn: <http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" +
			"select ?from ?first ?last ?dt ?rep ?content\n" +
			"where {\n" +
			"    {\n" +
			"        select ?rep ?dt\n" +
			"        where {\n" +
			"            ?post snvoc:hasCreator sn:pers%Person% .\n" +
			"            ?rep snvoc:replyOf ?post .\n" +
			"            ?rep snvoc:creationDate ?dt .\n" +
			"        }\n" +
			"        order by desc (?dt)\n" +
			"        limit 20\n" +
			"    } .\n" +
			"    ?rep snvoc:hasCreator ?from .\n" +
			"    ?from snvoc:firstName ?first .\n" +
			"    ?from snvoc:lastName ?last .\n" +
			"    ?rep snvoc:content ?content.\n" +
			"}\n" +
			"order by desc(?dt) ?rep";
	private static String QUERY_9 = "PREFIX sn: <http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" +
			"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" +
			"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
			"select ?fr ?first ?last ?post ?content ?date\n" +
			"\n" +
			"where {\n" +
			"    {\n" +
			"        select distinct ?fr\n" +
			"        where {\n" +
			"            {\n" +
			"                sn:pers%Person% snvoc:knows/snvoc:hasPerson ?fr.\n" +
			"            }\n" +
			"            union {\n" +
			"                sn:pers%Person% snvoc:knows/snvoc:hasPerson ?fr2.\n" +
			"                ?fr2 snvoc:knows/snvoc:hasPerson ?fr.\n" +
			"                filter (?fr != sn:pers%Person%)\n" +
			"            }\n" +
			"        }\n" +
			"    }\n" +
			"    ?fr snvoc:firstName ?first .\n" +
			"    ?fr snvoc:lastName ?last .\n" +
			"    ?post snvoc:hasCreator ?fr.\n" +
			"    ?post snvoc:creationDate ?date.\n" +
			"    filter (?date < \"%Date0%\"^^xsd:dateTime).\n" +
			"    {\n" +
			"        {\n" +
			"            ?post snvoc:content ?content\n" +
			"        } union {\n" +
			"            ?post snvoc:imageFile ?content\n" +
			"        }\n" +
			"    } .\n" +
			"}\n" +
			"order by desc (?date) ?post\n" +
			"limit 20";
	private static String QUERY_10 = "PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" +
			"PREFIX sn: <http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" +
			"PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n" +
			"select ?first ?last (2*count(distinct ?postIntr) - count(distinct ?postAll) as ?score) ?fof ?gender ?locationname\n" +
			"where {\n" +
			"    {\n" +
			"        select distinct ?fof\n" +
			"        where {\n" +
			"            sn:pers%Person% snvoc:knows/snvoc:hasPerson ?fr .\n" +
			"            ?fr snvoc:knows/snvoc:hasPerson ?fof .\n" +
			"            filter (?fof != sn:pers%Person%)\n" +
			"            minus {\n" +
			"                sn:pers%Person% snvoc:knows/snvoc:hasPerson ?fof \n" +
			"            } \n" +
			"        }\n" +
			"    } \n" +
			"    ?fof snvoc:firstName ?first .\n" +
			"    ?fof snvoc:lastName ?last .\n" +
			"    ?fof snvoc:gender ?gender .\n" +
			"    ?fof snvoc:birthday ?bday .\n" +
			"    ?fof snvoc:isLocatedIn ?based .\n" +
			"    ?based foaf:name ?locationname .\n" +
			"    filter (1 = if (Day(?bday) = %HS1%, if (Day(?bday) > 21, 1, 0),\n" +
			"            if (Month(?bday) = %HS1%, if (Day(?bday) < 22, 1, 0), 0)))\n" +
			"    optional {\n" +
			"        ?postIntr snvoc:hasCreator ?fof ;\n" +
			"                  snvoc:hasTag ?tag .\n" +
			"    }\n" +
			"    ?postAll snvoc:hasCreator ?fof ;\n" +
			"             snvoc:hasTag ?tag2.\n" +
			"    sn:pers%Person% snvoc:hasInterest ?tag .\n" +
			"}\n" +
			"group by ?fof ?first ?last ?gender ?locationname\n" +
			"order by desc(3) ?fof\n" +
			"limit 10";
	private static String QUERY_11 = "PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" +
			"PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n" +
			"PREFIX sn: <http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" +
			"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
			"\n" +
			"select ?first ?last ?startdate ?orgname ?fr\n" +
			"where {\n" +
			"    ?w snvoc:hasOrganisation ?org .\n" +
			"    ?org foaf:name ?orgname .\n" +
			"    ?org snvoc:isLocatedIn ?country.\n" +
			"    ?country foaf:name \"%Country%\" .\n" +
			"    ?fr snvoc:workAt ?w .\n" +
			"    ?w snvoc:workFrom ?startdate .\n" +
			"    filter (?startdate < \"%Date0%\"^^xsd:integer) .\n" +
			"    {\n" +
			"        select distinct ?fr\n" +
			"        where {\n" +
			"            {\n" +
			"                sn:pers%Person% snvoc:knows/snvoc:hasPerson ?fr.\n" +
			"            }\n" +
			"            union {\n" +
			"                sn:pers%Person% snvoc:knows/snvoc:hasPerson ?fr2.\n" +
			"                ?fr2 snvoc:knows/snvoc:hasPerson ?fr.\n" +
			"                filter (?fr != sn:pers%Person%)\n" +
			"            }\n" +
			"        }\n" +
			"    } .\n" +
			"    ?fr snvoc:firstName ?first .\n" +
			"    ?fr snvoc:lastName ?last .\n" +
			"}\n" +
			"order by ?startdate ?fr ?orgname\n" +
			"limit 10";
	private static String QUERY_12 = "PREFIX sn: <http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" +
			"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" +
			"PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n" +
			"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
			"select ?exp ?first ?last (group_concat(distinct ?tagname;separator=\", \") as ?tagnames) (count (*) as ?count)\n" +
			"where {\n" +
			"    sn:pers%Person% snvoc:knows/snvoc:hasPerson ?exp .\n" +
			"    ?exp snvoc:firstName ?first .\n" +
			"    ?exp snvoc:lastName ?last .\n" +
			"    ?reply snvoc:hasCreator ?exp .\n" +
			"    ?reply snvoc:replyOf ?org_post .\n" +
			"    filter (!exists {\n" +
			"            ?org_post snvoc:replyOf ?xx\n" +
			"        }) .\n" +
			"    ?org_post snvoc:hasTag ?tag .\n" +
			"    ?tag foaf:name ?tagname .\n" +
			"    ?tag a ?type.\n" +
			"    ?type rdfs:subClassOf* ?type1 .\n" +
			"    ?type1 rdfs:label \"%TagType%\" .\n" +
			"}\n" +
			"            group by ?exp ?first ?last\n" +
			"            order by desc(5) ?exp\n" +
			"            limit 20";
	private static String QUERY_13 = "PREFIX sn: <http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" +
			"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" +
			"select ?dist where\n" +
			"{\n" +
			"    {\n" +
			"        select distinct ?end (1 as ?dist)\n" +
			"        where {\n" +
			"            BIND (sn:pers%Person1% as ?start)\n" +
			"            BIND (sn:pers%Person2% as ?end)\n" +
			"\n" +
			"            ?start snvoc:knows/snvoc:hasPerson ?end.\n" +
			"        }\n" +
			"    }\n" +
			"    union\n" +
			"    {\n" +
			"        select distinct ?end (2 as ?dist)\n" +
			"        where {\n" +
			"            BIND (sn:pers%Person1% as ?start)\n" +
			"            BIND (sn:pers%Person2% as ?end)\n" +
			"            ?start snvoc:knows/snvoc:hasPerson ?fr2.\n" +
			"            ?fr2 snvoc:knows/snvoc:hasPerson ?end.\n" +
			"            filter (?fr != ?start).\n" +
			"        }\n" +
			"    }\n" +
			"    union\n" +
			"    {\n" +
			"        select distinct ?end (3 as ?dist)\n" +
			"        where {\n" +
			"            BIND (sn:pers%Person1% as ?start)\n" +
			"            BIND (sn:pers%Person2% as ?end)\n" +
			"            ?start snvoc:knows/snvoc:hasPerson ?fr2.\n" +
			"            ?fr2 snvoc:knows/snvoc:hasPerson ?fr3.\n" +
			"            ?fr3 snvoc:knows/snvoc:hasPerson ?end.\n" +
			"            filter (?fr != ?start).\n" +
			"        }\n" +
			"    } .\n" +
			"}\n" +
			"order by ?dist\n" +
			"limit 1";
	private static String QUERY_14 = "";

	private static class GraphDbClient {
		private final HTTPRepository repository;
		GraphDbClient(String connUrl) {
			repository = new HTTPRepository(connUrl);
		}

		List<BindingSet> execute(String queryString, Map<String, Object> queryParams) {
			queryString = LdbcUtils.applyParameters(queryString, queryParams);
			List<BindingSet> bindings = new ArrayList<>();
			try (RepositoryConnection conn = repository.getConnection()) {
				TupleQueryResult resultIter = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate();
				while (resultIter.hasNext()) {
					bindings.add(resultIter.next());
				}
			}
			return bindings;
		}

		public void close() {
			repository.shutDown();
		}
	}

	static class GraphDbConnectionState extends DbConnectionState {
		private final GraphDbClient graphDbClient;

		private GraphDbConnectionState(String connUrl) {
			graphDbClient = new GraphDbClient(connUrl);
		}

		public GraphDbClient getGraphDbClient() {
			return graphDbClient;
		}

		@Override
		public void close() throws IOException {
			graphDbClient.close();
		}
	}

	public enum SleepType {
		SLEEP,
		PARK,
		SPIN
	}

	public static final String SLEEP_DURATION_NANO_ARG = "ldbc.snb.interactive.db.sleep_duration_nano";
	public static final String SLEEP_TYPE_ARG = "ldbc.snb.interactive.db.sleep_type";

	private static long sleepDurationAsNano;
	private SleepType sleepType;

	private interface SleepFun {
		void sleep(Operation operation, long sleepNs);
	}

	private static SleepFun sleepFun;
	private GraphDbConnectionState connectionState = null;

	@Override
	protected void onInit(Map<String, String> params, LoggingService loggingService) throws DbException {
		String repositoryUrl = params.get("endpoint");
		if (null != repositoryUrl && !repositoryUrl.isEmpty()) {
			connectionState = new GraphDbConnectionState(repositoryUrl);
		} else {
			throw new DbException("Should provide repository url");
		}
		String sleepDurationAsNanoAsString = params.get(SLEEP_DURATION_NANO_ARG);
		if (null == sleepDurationAsNanoAsString) {
			sleepDurationAsNano = 0L;
		} else if (!sleepDurationAsNanoAsString.isEmpty()) {
			try {
				sleepDurationAsNano = Long.parseLong(sleepDurationAsNanoAsString);
			} catch (NumberFormatException e) {
				throw new DbException(format("Error encountered while trying to parse value [%s] for argument [%s]",
						sleepDurationAsNanoAsString, SLEEP_DURATION_NANO_ARG), e);
			}
		}
		String sleepTypeString = params.get(SLEEP_TYPE_ARG);
		if (null == sleepTypeString) {
			sleepType = SleepType.SPIN;
		} else {
			try {
				sleepType = SleepType.valueOf(params.get(SLEEP_TYPE_ARG));
			} catch (IllegalArgumentException e) {
				throw new DbException(format("Invalid sleep type: %s", sleepTypeString));
			}
		}

		if (0 == sleepDurationAsNano) {
			sleepFun = (operation, sleepNs) -> {
				// do nothing
			};
		} else {
			switch (sleepType) {
				case SLEEP:
					sleepFun = (operation, sleepNs) -> {
						try {
							Thread.sleep(TimeUnit.NANOSECONDS.toMillis(sleepNs));
						} catch (InterruptedException e) {
							// do nothing
						}
					};
					break;
				case PARK:
					sleepFun = (operation, sleepNs) -> LockSupport.parkNanos(sleepNs);
					break;
				case SPIN:
					sleepFun = (operation, sleepNs) -> {
						long endTimeAsNano = System.nanoTime() + sleepNs;
						while (System.nanoTime() < endTimeAsNano) {
							// busy wait
						}
					};
					break;
			}
		}

		params.put(SLEEP_DURATION_NANO_ARG, Long.toString(sleepDurationAsNano));
		params.put(SLEEP_TYPE_ARG, sleepType.name());

		// Long Reads
		registerOperationHandler(LdbcQuery1.class, LdbcQuery1Handler.class);
		registerOperationHandler(LdbcQuery2.class, LdbcQuery2Handler.class);
		registerOperationHandler(LdbcQuery3.class, LdbcQuery3Handler.class);
		registerOperationHandler(LdbcQuery4.class, LdbcQuery4Handler.class);
		registerOperationHandler(LdbcQuery5.class, LdbcQuery5Handler.class);
		registerOperationHandler(LdbcQuery6.class, LdbcQuery6Handler.class);
		registerOperationHandler(LdbcQuery7.class, LdbcQuery7Handler.class);
		registerOperationHandler(LdbcQuery8.class, LdbcQuery8Handler.class);
		registerOperationHandler(LdbcQuery9.class, LdbcQuery9Handler.class);
		registerOperationHandler(LdbcQuery10.class, LdbcQuery10Handler.class);
		registerOperationHandler(LdbcQuery11.class, LdbcQuery11Handler.class);
		registerOperationHandler(LdbcQuery12.class, LdbcQuery12Handler.class);
		registerOperationHandler(LdbcQuery13.class, LdbcQuery13Handler.class);
		registerOperationHandler(LdbcQuery14.class, LdbcQuery14Handler.class);
	}

	@Override
	protected void onClose() throws IOException {
	}

	@Override
	protected DbConnectionState getConnectionState() throws DbException {
		return connectionState;
	}

	private static void sleep(Operation operation, long sleepNs) {
		sleepFun.sleep(operation, sleepNs);
	}

    /*
    LONG READS
     */

	public static class LdbcQuery1Handler implements OperationHandler<LdbcQuery1, GraphDbConnectionState> {
		@Override
		public void executeOperation(LdbcQuery1 operation, GraphDbConnectionState dbConnectionState,
									 ResultReporter resultReporter) throws DbException {
			sleep(operation, sleepDurationAsNano);
			List<BindingSet> results = dbConnectionState.getGraphDbClient().execute(QUERY_1, operation.parameterMap());

			resultReporter.report(200, GraphDBLdbcSnbInteractiveOperationResultSets.read1Results(results), operation);
		}
	}

	public static class LdbcQuery2Handler implements OperationHandler<LdbcQuery2, GraphDbConnectionState> {
		@Override
		public void executeOperation(LdbcQuery2 operation, GraphDbConnectionState dbConnectionState,
									 ResultReporter resultReporter) throws DbException {
			sleep(operation, sleepDurationAsNano);
			List<BindingSet> results = dbConnectionState.getGraphDbClient().execute(QUERY_2, operation.parameterMap());
			resultReporter.report(0, GraphDBLdbcSnbInteractiveOperationResultSets.read2Results(results), operation);
		}
	}

	public static class LdbcQuery3Handler implements OperationHandler<LdbcQuery3, GraphDbConnectionState> {
		@Override
		public void executeOperation(LdbcQuery3 operation, GraphDbConnectionState dbConnectionState,
									 ResultReporter resultReporter) throws DbException {
			sleep(operation, sleepDurationAsNano);
			List<BindingSet> results = dbConnectionState.getGraphDbClient().execute(QUERY_3, operation.parameterMap());
			resultReporter.report(0, GraphDBLdbcSnbInteractiveOperationResultSets.read3Results(results), operation);
		}
	}

	public static class LdbcQuery4Handler implements OperationHandler<LdbcQuery4, GraphDbConnectionState> {
		@Override
		public void executeOperation(LdbcQuery4 operation, GraphDbConnectionState dbConnectionState,
									 ResultReporter resultReporter) throws DbException {
			sleep(operation, sleepDurationAsNano);
			List<BindingSet> results = dbConnectionState.getGraphDbClient().execute(QUERY_4, operation.parameterMap());

			resultReporter.report(0, GraphDBLdbcSnbInteractiveOperationResultSets.read4Results(results), operation);
		}
	}

	public static class LdbcQuery5Handler implements OperationHandler<LdbcQuery5, GraphDbConnectionState> {
		@Override
		public void executeOperation(LdbcQuery5 operation, GraphDbConnectionState dbConnectionState,
									 ResultReporter resultReporter) throws DbException {
			sleep(operation, sleepDurationAsNano);
			List<BindingSet> results = dbConnectionState.getGraphDbClient().execute(QUERY_5, operation.parameterMap());

			resultReporter.report(0, GraphDBLdbcSnbInteractiveOperationResultSets.read5Results(results), operation);
		}
	}

	public static class LdbcQuery6Handler implements OperationHandler<LdbcQuery6, GraphDbConnectionState> {
		@Override
		public void executeOperation(LdbcQuery6 operation, GraphDbConnectionState dbConnectionState,
									 ResultReporter resultReporter) throws DbException {
			sleep(operation, sleepDurationAsNano);
			List<BindingSet> results = dbConnectionState.getGraphDbClient().execute(QUERY_6, operation.parameterMap());

			resultReporter.report(0, GraphDBLdbcSnbInteractiveOperationResultSets.read6Results(results), operation);
		}
	}

	public static class LdbcQuery7Handler implements OperationHandler<LdbcQuery7, GraphDbConnectionState> {
		@Override
		public void executeOperation(LdbcQuery7 operation, GraphDbConnectionState dbConnectionState,
									 ResultReporter resultReporter) throws DbException {
			sleep(operation, sleepDurationAsNano);
			List<BindingSet> results = dbConnectionState.getGraphDbClient().execute(QUERY_7, operation.parameterMap());

			resultReporter.report(0, GraphDBLdbcSnbInteractiveOperationResultSets.read7Results(results), operation);
		}
	}

	public static class LdbcQuery8Handler implements OperationHandler<LdbcQuery8, GraphDbConnectionState> {
		@Override
		public void executeOperation(LdbcQuery8 operation, GraphDbConnectionState dbConnectionState,
									 ResultReporter resultReporter) throws DbException {
			sleep(operation, sleepDurationAsNano);
			List<BindingSet> results = dbConnectionState.getGraphDbClient().execute(QUERY_8, operation.parameterMap());

			resultReporter.report(0, GraphDBLdbcSnbInteractiveOperationResultSets.read8Results(results), operation);
		}
	}

	public static class LdbcQuery9Handler implements OperationHandler<LdbcQuery9, GraphDbConnectionState> {
		@Override
		public void executeOperation(LdbcQuery9 operation, GraphDbConnectionState dbConnectionState,
									 ResultReporter resultReporter) throws DbException {
			sleep(operation, sleepDurationAsNano);
			List<BindingSet> results = dbConnectionState.getGraphDbClient().execute(QUERY_9, operation.parameterMap());

			resultReporter.report(0, GraphDBLdbcSnbInteractiveOperationResultSets.read9Results(results), operation);
		}
	}

	public static class LdbcQuery10Handler implements OperationHandler<LdbcQuery10, GraphDbConnectionState> {
		@Override
		public void executeOperation(LdbcQuery10 operation, GraphDbConnectionState dbConnectionState,
									 ResultReporter resultReporter) throws DbException {
			sleep(operation, sleepDurationAsNano);
			List<BindingSet> results = dbConnectionState.getGraphDbClient().execute(QUERY_10, operation.parameterMap());

			resultReporter.report(0, GraphDBLdbcSnbInteractiveOperationResultSets.read10Results(results), operation);
		}
	}

	public static class LdbcQuery11Handler implements OperationHandler<LdbcQuery11, GraphDbConnectionState> {
		@Override
		public void executeOperation(LdbcQuery11 operation, GraphDbConnectionState dbConnectionState,
									 ResultReporter resultReporter) throws DbException {
			sleep(operation, sleepDurationAsNano);
			List<BindingSet> results = dbConnectionState.getGraphDbClient().execute(QUERY_11, operation.parameterMap());

			resultReporter.report(0, GraphDBLdbcSnbInteractiveOperationResultSets.read11Results(results), operation);
		}
	}

	public static class LdbcQuery12Handler implements OperationHandler<LdbcQuery12, GraphDbConnectionState> {
		@Override
		public void executeOperation(LdbcQuery12 operation, GraphDbConnectionState dbConnectionState,
									 ResultReporter resultReporter) throws DbException {
			sleep(operation, sleepDurationAsNano);
			List<BindingSet> results = dbConnectionState.getGraphDbClient().execute(QUERY_12, operation.parameterMap());

			resultReporter.report(0, GraphDBLdbcSnbInteractiveOperationResultSets.read12Results(results), operation);
		}
	}

	public static class LdbcQuery13Handler implements OperationHandler<LdbcQuery13, GraphDbConnectionState> {
		@Override
		public void executeOperation(LdbcQuery13 operation, GraphDbConnectionState dbConnectionState,
									 ResultReporter resultReporter) throws DbException {
			sleep(operation, sleepDurationAsNano);
			List<BindingSet> results = dbConnectionState.getGraphDbClient().execute(QUERY_13, operation.parameterMap());

			resultReporter.report(0, GraphDBLdbcSnbInteractiveOperationResultSets.read13Results(results), operation);
		}
	}

	public static class LdbcQuery14Handler implements OperationHandler<LdbcQuery14, GraphDbConnectionState> {
		@Override
		public void executeOperation(LdbcQuery14 operation, GraphDbConnectionState dbConnectionState,
									 ResultReporter resultReporter) throws DbException {
//			sleep(operation, sleepDurationAsNano);
//			List<BindingSet> results = dbConnectionState.getGraphDbClient().execute(QUERY_14, operation.parameterMap());
//
//			resultReporter.report(0, GraphDBLdbcSnbInteractiveOperationResultSets.read14Results(results), operation);
		}
	}
}
