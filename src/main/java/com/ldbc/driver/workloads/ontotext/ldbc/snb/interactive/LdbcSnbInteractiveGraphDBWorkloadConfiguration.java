package com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive;

import com.ldbc.driver.Operation;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfile;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery2PersonPosts;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery3PersonFriends;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery4MessageContent;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery5MessageCreator;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery6MessageForum;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery7MessageReplies;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkloadConfiguration;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate1AddPerson;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate2AddPostLike;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate3AddCommentLike;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate4AddForum;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate5AddForumMembership;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate6AddPost;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate7AddComment;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate8AddFriendship;
import com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive.queries.longreads.LdbcQuery1;
import com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive.queries.longreads.LdbcQuery2;
import com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive.queries.longreads.LdbcQuery3;
import com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive.queries.longreads.LdbcQuery4;
import com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive.queries.longreads.LdbcQuery5;
import com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive.queries.longreads.LdbcQuery6;
import com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive.queries.longreads.LdbcQuery7;
import com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive.queries.longreads.LdbcQuery8;
import com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive.queries.longreads.LdbcQuery9;
import com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive.queries.longreads.LdbcQuery10;
import com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive.queries.longreads.LdbcQuery11;
import com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive.queries.longreads.LdbcQuery12;
import com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive.queries.longreads.LdbcQuery13;
import com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive.queries.longreads.LdbcQuery14;


import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Arrays;

import static java.lang.String.format;

public class LdbcSnbInteractiveGraphDBWorkloadConfiguration {

	public static final String LDBC_GRAPHDB_INTERACTIVE_PACKAGE_PREFIX = "com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive.";
	public static final String LONG_READ_QUERIES_PACKAGE_PREFIX = "queries.longreads.";
	public static final String SHORT_READ_QUERIES_PACKAGE_PREFIX = "queries.shortreads.";
	public static final String WRITE_QUERIES_PACKAGE_PREFIX = "queries.writes.";

	public static final String LDBC_SNB_QUERY_DIR = "queryDir";
	public static final int WRITE_OPERATION_NO_RESULT_DEFAULT_RESULT = -1;

	static Set<String> missingParameters(Map<String, String> properties, Iterable<String> compulsoryPropertyKeys) {
		Set<String> missingPropertyKeys = new HashSet<>();
		for (String compulsoryKey : compulsoryPropertyKeys) {
			if (null == properties.get(compulsoryKey)) {
				missingPropertyKeys.add(compulsoryKey);
			}
		}
		return missingPropertyKeys;
	}

	 static Map<Integer,Class<? extends Operation>> operationTypeToClassMapping() {
		 Map<Integer, Class<? extends Operation>> operationTypeToClassMapping = new HashMap<>();
		 operationTypeToClassMapping.put(LdbcQuery1.TYPE, LdbcQuery1.class);
		 operationTypeToClassMapping.put(LdbcQuery2.TYPE, LdbcQuery2.class);
		 operationTypeToClassMapping.put(LdbcQuery3.TYPE, LdbcQuery3.class);
		 operationTypeToClassMapping.put(LdbcQuery4.TYPE, LdbcQuery4.class);
		 operationTypeToClassMapping.put(LdbcQuery5.TYPE, LdbcQuery5.class);
		 operationTypeToClassMapping.put(LdbcQuery6.TYPE, LdbcQuery6.class);
		 operationTypeToClassMapping.put(LdbcQuery7.TYPE, LdbcQuery7.class);
		 operationTypeToClassMapping.put(LdbcQuery8.TYPE, LdbcQuery8.class);
		 operationTypeToClassMapping.put(LdbcQuery9.TYPE, LdbcQuery9.class);
		 operationTypeToClassMapping.put(LdbcQuery10.TYPE, LdbcQuery10.class);
		 operationTypeToClassMapping.put(LdbcQuery11.TYPE, LdbcQuery11.class);
		 operationTypeToClassMapping.put(LdbcQuery12.TYPE, LdbcQuery12.class);
		 operationTypeToClassMapping.put(LdbcQuery13.TYPE, LdbcQuery13.class);
		 operationTypeToClassMapping.put(LdbcQuery14.TYPE, LdbcQuery14.class);
		 //така е и в оригиналното ldbc:
		 // operationTypeToClassMapping.put( LdbcShortQuery1PersonProfile.TYPE, LdbcShortQuery1PersonProfile.class );
		  operationTypeToClassMapping.put( LdbcShortQuery2PersonPosts.TYPE, LdbcShortQuery2PersonPosts.class );
		 // operationTypeToClassMapping.put( LdbcShortQuery3PersonFriends.TYPE, LdbcShortQuery3PersonFriends.class );
		 // operationTypeToClassMapping.put( LdbcShortQuery4MessageContent.TYPE, LdbcShortQuery4MessageContent.class );
		 // operationTypeToClassMapping.put( LdbcShortQuery5MessageCreator.TYPE, LdbcShortQuery5MessageCreator.class );
		 // operationTypeToClassMapping.put( LdbcShortQuery6MessageForum.TYPE, LdbcShortQuery6MessageForum.class );
		 // operationTypeToClassMapping.put( LdbcShortQuery7MessageReplies.TYPE, LdbcShortQuery7MessageReplies.class );
		 // operationTypeToClassMapping.put( LdbcUpdate1AddPerson.TYPE, LdbcUpdate1AddPerson.class );
		 // operationTypeToClassMapping.put( LdbcUpdate2AddPostLike.TYPE, LdbcUpdate2AddPostLike.class );
		 // operationTypeToClassMapping.put( LdbcUpdate3AddCommentLike.TYPE, LdbcUpdate3AddCommentLike.class );
		 // operationTypeToClassMapping.put( LdbcUpdate4AddForum.TYPE, LdbcUpdate4AddForum.class );
		 // operationTypeToClassMapping.put( LdbcUpdate5AddForumMembership.TYPE, LdbcUpdate5AddForumMembership.class );
		 // operationTypeToClassMapping.put( LdbcUpdate6AddPost.TYPE, LdbcUpdate6AddPost.class );
		 // operationTypeToClassMapping.put( LdbcUpdate7AddComment.TYPE, LdbcUpdate7AddComment.class );
		 // operationTypeToClassMapping.put( LdbcUpdate8AddFriendship.TYPE, LdbcUpdate8AddFriendship.class );
		 return operationTypeToClassMapping;
	}

	static String removePrefix(String original, String prefix) {
		return (!original.contains(prefix)) ? original : original
				.substring(original.lastIndexOf(prefix) + prefix.length());
	}

	static String removeSuffix(String original, String suffix) {
		return (!original.contains(suffix)) ? original : original.substring(0, original.lastIndexOf(suffix));
	}

	static boolean isValidParser(String parserString) throws WorkloadException {
		try {
			LdbcSnbInteractiveWorkloadConfiguration.UpdateStreamParser parser = LdbcSnbInteractiveWorkloadConfiguration.
					UpdateStreamParser.valueOf(parserString);
			Set<LdbcSnbInteractiveWorkloadConfiguration.UpdateStreamParser> validParsers = new HashSet<>();
			validParsers.addAll(Arrays.asList(LdbcSnbInteractiveWorkloadConfiguration.UpdateStreamParser.values()));
			return validParsers.contains(parser);
		} catch (IllegalArgumentException e) {
			throw new WorkloadException(format("Unsupported parser value: %s", parserString), e);
		}
	}
}
