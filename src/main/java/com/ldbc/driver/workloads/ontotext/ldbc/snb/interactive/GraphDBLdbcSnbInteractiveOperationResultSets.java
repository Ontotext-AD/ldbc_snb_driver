package com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.query.BindingSet;

import java.util.ArrayList;
import java.util.List;

public class GraphDBLdbcSnbInteractiveOperationResultSets {
	public static final String FIRST = "first";
	public static final String CONTENT = "content";
	public static final String COUNT = "count";
	public static final String TAGNAME = "tagname";
	public static final String LAST = "last";
	public static final String FR = "fr";

    /*
    LONG READS
     */

	public static List<LdbcQuery1Result> read1Results(List<BindingSet> bindingSets) {
		List<LdbcQuery1Result> results = new ArrayList<>();
		for (BindingSet binding : bindingSets) {
			IRI friendId = (IRI) binding.getValue(FR);
			String friendLastName = binding.getValue(LAST).stringValue();
			int distanceFromPerson = ((Literal) binding.getValue("mindist")).intValue();
			Literal friendBirthday = (Literal) binding.getValue("bday");
			Literal friendCreationDate = (Literal) binding.getValue("since");
			String friendGender = binding.getValue("gen").stringValue();
			String friendBrowserUsed = binding.getValue("browser").stringValue();
			String friendLocationIp = binding.getValue("locationIP").stringValue();
			String friendEmails = binding.getValue("emails").stringValue();
			String friendLanguages = binding.getValue("lngs").stringValue();
			String friendCityName = binding.getValue("based").stringValue();
			String friendUniversities = binding.getValue("studyAt").stringValue();
			String friendCompanies = binding.getValue("workAt").stringValue();

			results.add(new LdbcQuery1Result(
					friendId,
					friendLastName,
					distanceFromPerson,
					friendBirthday,
					friendCreationDate,
					friendGender,
					friendBrowserUsed,
					friendLocationIp,
					friendEmails,
					friendLanguages,
					friendCityName,
					friendUniversities,
					friendCompanies));
		}
		return results;
	}

	public static List<LdbcQuery2Result> read2Results(List<BindingSet> bindingSets) {
		List<LdbcQuery2Result> results = new ArrayList<>();
		for (BindingSet binding : bindingSets) {
			IRI friendId = (IRI) binding.getValue(FR);
			String friendFirstName = binding.getValue(FIRST).stringValue();
			String friendLastName = binding.getValue(LAST).stringValue();
			IRI msgId = (IRI) binding.getValue("post");
			String msgContent = binding.getValue(CONTENT).stringValue();
			Literal messageCreationDate = (Literal) binding.getValue("date");

			results.add(new LdbcQuery2Result(
					friendId,
					friendFirstName,
					friendLastName,
					msgId,
					msgContent,
					messageCreationDate));
		}
		return results;
	}

	public static List<LdbcQuery3Result> read3Results(List<BindingSet> bindingSets) {
		List<LdbcQuery3Result> results = new ArrayList<>();
		for (BindingSet binding : bindingSets) {
			IRI friendId = (IRI) binding.getValue(FR);
			String friendFirstName = binding.getValue(FIRST).stringValue();
			String friendLastName = binding.getValue(LAST).stringValue();
			int ct1 = ((Literal) binding.getValue("ct1")).intValue();
			int ct2 = ((Literal) binding.getValue("ct2")).intValue();
			int sum = ((Literal) binding.getValue("sum")).intValue();

			results.add(new LdbcQuery3Result(
					friendId,
					friendFirstName,
					friendLastName,
					ct1,
					ct2,
					sum));
		}
		return results;
	}

	public static List<LdbcQuery4Result> read4Results(List<BindingSet> bindingSets) {
		List<LdbcQuery4Result> results = new ArrayList<>();
		for (BindingSet binding : bindingSets) {
			String tagname = binding.getValue(TAGNAME).stringValue();
			int count = ((Literal) binding.getValue(COUNT)).intValue();

			results.add(new LdbcQuery4Result(tagname, count));
		}
		return results;
	}

	public static List<LdbcQuery5Result> read5Results(List<BindingSet> bindingSets) {
		List<LdbcQuery5Result> results = new ArrayList<>();
		for (BindingSet binding : bindingSets) {
			String title = binding.getValue("title").stringValue();
			int count = ((Literal) binding.getValue(COUNT)).intValue();

			results.add(new LdbcQuery5Result(title, count));
		}
		return results;
	}

	public static List<LdbcQuery6Result> read6Results(List<BindingSet> bindingSets) {
		List<LdbcQuery6Result> results = new ArrayList<>();
		for (BindingSet binding : bindingSets) {
			String tagname = binding.getValue(TAGNAME).stringValue();
			int count = ((Literal) binding.getValue(COUNT)).intValue();

			results.add(new LdbcQuery6Result(tagname, count));
		}
		return results;
	}

	public static List<LdbcQuery7Result> read7Results(List<BindingSet> bindingSets) {
		List<LdbcQuery7Result> results = new ArrayList<>();
		for (BindingSet binding : bindingSets) {
			IRI liker = (IRI) binding.getValue("liker");
			String friendFirstName = binding.getValue(FIRST).stringValue();
			String friendLastName = binding.getValue(LAST).stringValue();
			Literal likeCreationDate = (Literal) binding.getValue("ldt");
			IRI postId = (IRI) binding.getValue("post");
			String content = binding.getValue(CONTENT).stringValue();
			int lag = ((Literal) binding.getValue("lag")).intValue();
			int isNew = ((Literal) binding.getValue("is_new")).intValue();

			results.add(new LdbcQuery7Result(
					liker,
					friendFirstName,
					friendLastName,
					likeCreationDate,
					postId,
					content,
					lag,
					isNew != 0));
		}
		return results;
	}

	public static List<LdbcQuery8Result> read8Results(List<BindingSet> bindingSets) {
		List<LdbcQuery8Result> results = new ArrayList<>();
		for (BindingSet binding : bindingSets) {
			IRI from = (IRI) binding.getValue("from");
			String friendFirstName = binding.getValue(FIRST).stringValue();
			String friendLastName = binding.getValue(LAST).stringValue();
			Literal dt = (Literal) binding.getValue("dt");
			IRI rep = (IRI) binding.getValue("rep");
			String content = binding.getValue(CONTENT).stringValue();

			results.add(new LdbcQuery8Result(
					from,
					friendFirstName,
					friendLastName,
					dt,
					rep,
					content));
		}
		return results;
	}

	public static List<LdbcQuery9Result> read9Results(List<BindingSet> bindingSets) {
		List<LdbcQuery9Result> results = new ArrayList<>();
		for (BindingSet binding : bindingSets) {
			IRI friendId = (IRI) binding.getValue(FR);
			String friendFirstName = binding.getValue(FIRST).stringValue();
			String friendLastName = binding.getValue(LAST).stringValue();
			IRI postId = (IRI) binding.getValue("post");
			String content = binding.getValue(CONTENT).stringValue();
			Literal date = (Literal) binding.getValue("date");

			results.add(new LdbcQuery9Result(
					friendId,
					friendFirstName,
					friendLastName,
					postId,
					content,
					date));
		}
		return results;
	}

	public static List<LdbcQuery10Result> read10Results(List<BindingSet> bindingSets) {
		List<LdbcQuery10Result> results = new ArrayList<>();
		for (BindingSet binding : bindingSets) {
			IRI fofId = (IRI) binding.getValue("fof");
			String friendFirstName = binding.getValue(FIRST).stringValue();
			String friendLastName = binding.getValue(LAST).stringValue();
			int commonInterestCnt = ((Literal) binding.getValue("score")).intValue();
			String gender = binding.getValue("gender").stringValue();
			String locationname = binding.getValue("locationname").stringValue();

			results.add(new LdbcQuery10Result(
					fofId,
					friendFirstName,
					friendLastName,
					commonInterestCnt,
					gender,
					locationname));
		}
		return results;
	}

	public static List<LdbcQuery11Result> read11Results(List<BindingSet> bindingSets) {
		List<LdbcQuery11Result> results = new ArrayList<>();
		for (BindingSet binding : bindingSets) {
			IRI friendId = (IRI) binding.getValue(FR);
			String friendFirstName = binding.getValue(FIRST).stringValue();
			String friendLastName = binding.getValue(LAST).stringValue();
			int startdate = ((Literal) binding.getValue("startdate")).intValue();
			String orgname = binding.getValue("orgname").stringValue();

			results.add(new LdbcQuery11Result(
					friendId,
					friendFirstName,
					friendLastName,
					orgname, startdate));
		}
		return results;
	}

	public static List<LdbcQuery12Result> read12Results(List<BindingSet> bindingSets) {
		List<LdbcQuery12Result> results = new ArrayList<>();
		for (BindingSet binding : bindingSets) {
			IRI exp = (IRI) binding.getValue("exp");
			String friendFirstName = binding.getValue(FIRST).stringValue();
			String friendLastName = binding.getValue(LAST).stringValue();
			int count = ((Literal) binding.getValue(COUNT)).intValue();
			String tagnames = binding.getValue("tagnames").stringValue();

			results.add(new LdbcQuery12Result(
					exp,
					friendFirstName,
					friendLastName,
					tagnames,
					count));
		}
		return results;
	}

	public static LdbcQuery13Result read13Results(List<BindingSet> bindingSets) {
		int dist = -1;
		if (bindingSets.size() == 1) {
			dist = ((Literal) bindingSets.get(0).getValue("dist")).intValue();
		}
		return new LdbcQuery13Result(dist);
	}

	public static List<LdbcQuery14Result> read14Results(List<BindingSet> bindingSets) {
		List<LdbcQuery14Result> results = new ArrayList<>();
		for (BindingSet binding : bindingSets) {
			List<IRI> idx = new ArrayList<>();
			IRI target = (IRI) binding.getValue("start");
			if (target != null) {
				idx.add(target);
			}
			IRI fr2 = (IRI) binding.getValue("end");
			if (fr2 != null) {
				idx.add(fr2);
			}

			double pathWeight = ((Literal) binding.getValue("pathWeight")).doubleValue();

			results.add(new LdbcQuery14Result(
					idx,
					pathWeight));
		}
		return results;
	}
}
