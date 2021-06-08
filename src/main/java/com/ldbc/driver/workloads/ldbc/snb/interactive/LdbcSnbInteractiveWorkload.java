package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.*;
import com.ldbc.driver.*;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.Equator;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

public class LdbcSnbInteractiveWorkload extends Workload
{
    @Override
    public Map<Integer,Class<? extends Operation<?>>> operationTypeToClassMapping()
    {
        return LdbcSnbInteractiveWorkloadConfiguration.operationTypeToClassMapping();
    }

    @Override
    public void onInit(Map<String, String> params) throws WorkloadException {
        List<String> compulsoryKeys = Lists.newArrayList(
                LdbcSnbInteractiveWorkloadConfiguration.PARAMETERS_DIRECTORY);

        compulsoryKeys.addAll(LdbcSnbInteractiveWorkloadConfiguration.LONG_READ_OPERATION_ENABLE_KEYS);
        compulsoryKeys.addAll(LdbcSnbInteractiveWorkloadConfiguration.WRITE_OPERATION_ENABLE_KEYS);
        compulsoryKeys.addAll(LdbcSnbInteractiveWorkloadConfiguration.SHORT_READ_OPERATION_ENABLE_KEYS);

        Set<String> missingPropertyParameters =
                LdbcSnbInteractiveWorkloadConfiguration.missingParameters(params, compulsoryKeys);
        if (!missingPropertyParameters.isEmpty()) {
            throw new WorkloadException(format("Workload could not initialize due to missing parameters: %s",
                    missingPropertyParameters));
        }
        super.onInit(params);
    }

    @Override
    public DbValidationParametersFilter dbValidationParametersFilter( Integer requiredValidationParameterCount )
    {
        final Set<Class<?>> multiResultOperations = Sets.newHashSet(
                LdbcShortQuery2PersonPosts.class,
                LdbcShortQuery3PersonFriends.class,
                LdbcShortQuery7MessageReplies.class,
                LdbcQuery1.class,
                LdbcQuery2.class,
//                LdbcQuery3.class,
                LdbcQuery4.class,
                LdbcQuery5.class,
                LdbcQuery6.class,
                LdbcQuery7.class,
                LdbcQuery8.class,
                LdbcQuery9.class,
                LdbcQuery10.class,
                LdbcQuery11.class,
                LdbcQuery12.class,
                LdbcQuery14.class
        );

        int operationTypeCount = enabledLongReadOperationTypes.size() + enabledWriteOperationTypes.size();
        long minimumResultCountPerOperationType = Math.max(
                1,
                Math.round( Math.floor(
                        requiredValidationParameterCount.doubleValue() / (double) operationTypeCount) )
        );

        long writeAddPersonOperationCount = (enabledWriteOperationTypes.contains( LdbcUpdate1AddPerson.class ))
                                            ? minimumResultCountPerOperationType
                                            : 0;

        final Map<Class<?>,Long> remainingRequiredResultsPerUpdateType = new HashMap<>();
        long resultCountsAssignedForUpdateTypesSoFar = 0;
        for ( Class<?> updateOperationType : enabledWriteOperationTypes )
        {
            if ( updateOperationType.equals( LdbcUpdate1AddPerson.class ) )
            { continue; }
            remainingRequiredResultsPerUpdateType.put( updateOperationType, minimumResultCountPerOperationType );
            resultCountsAssignedForUpdateTypesSoFar =
                    resultCountsAssignedForUpdateTypesSoFar + minimumResultCountPerOperationType;
        }

        final Map<Class<?>,Long> remainingRequiredResultsPerLongReadType = new HashMap<>();
        long resultCountsAssignedForLongReadTypesSoFar = 0;
        for ( Class<?> longReadOperationType : enabledLongReadOperationTypes )
        {
            remainingRequiredResultsPerLongReadType.put( longReadOperationType, minimumResultCountPerOperationType );
            resultCountsAssignedForLongReadTypesSoFar =
                    resultCountsAssignedForLongReadTypesSoFar + minimumResultCountPerOperationType;
        }

        return new LdbcSnbInteractiveDbValidationParametersFilter(
                multiResultOperations,
                writeAddPersonOperationCount,
                remainingRequiredResultsPerUpdateType,
                remainingRequiredResultsPerLongReadType,
                enabledShortReadOperationTypes
        );
    }

    @Override
    public long maxExpectedInterleaveAsMilli()
    {
        return TimeUnit.HOURS.toMillis( 1 );
    }

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference TYPE_REFERENCE = new TypeReference<List<Object>>()
    {
    };

    @Override
    public String serializeOperation( Operation<?> operation ) throws SerializingMarshallingException
    {
        switch ( operation.type() )
        {
        case LdbcQuery1.TYPE:
        {
            LdbcQuery1 ldbcQuery = (LdbcQuery1) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add( ldbcQuery.getClass().getName() );
            operationAsList.add( ldbcQuery.personId() );
            operationAsList.add( ldbcQuery.firstName() );
            operationAsList.add( ldbcQuery.limit() );
            try
            {
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            catch ( IOException e )
            {
                throw new SerializingMarshallingException(
                        format( "Error while trying to serialize result\n%s", operationAsList.toString() ), e );
            }
        }
        case LdbcQuery2.TYPE:
        {
            LdbcQuery2 ldbcQuery = (LdbcQuery2) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add( ldbcQuery.getClass().getName() );
            operationAsList.add( ldbcQuery.personId() );
            operationAsList.add( ldbcQuery.maxDate().getTime() );
            operationAsList.add( ldbcQuery.limit() );
            try
            {
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            catch ( IOException e )
            {
                throw new SerializingMarshallingException(
                        format( "Error while trying to serialize result\n%s", operationAsList.toString() ), e );
            }
        }
        case LdbcQuery3.TYPE:
        {
            LdbcQuery3 ldbcQuery = (LdbcQuery3) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add( ldbcQuery.getClass().getName() );
            operationAsList.add( ldbcQuery.personId() );
            operationAsList.add( ldbcQuery.countryXName() );
            operationAsList.add( ldbcQuery.countryYName() );
            operationAsList.add( ldbcQuery.startDate().getTime() );
            operationAsList.add( ldbcQuery.durationDays() );
            operationAsList.add( ldbcQuery.limit() );
            try
            {
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            catch ( IOException e )
            {
                throw new SerializingMarshallingException(
                        format( "Error while trying to serialize result\n%s", operationAsList.toString() ), e );
            }
        }
        case LdbcQuery4.TYPE:
        {
            LdbcQuery4 ldbcQuery = (LdbcQuery4) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add( ldbcQuery.getClass().getName() );
            operationAsList.add( ldbcQuery.personId() );
            operationAsList.add( ldbcQuery.startDate().getTime() );
            operationAsList.add( ldbcQuery.durationDays() );
            operationAsList.add( ldbcQuery.limit() );
            try
            {
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            catch ( IOException e )
            {
                throw new SerializingMarshallingException(
                        format( "Error while trying to serialize result\n%s", operationAsList.toString() ), e );
            }
        }
        case LdbcQuery5.TYPE:
        {
            LdbcQuery5 ldbcQuery = (LdbcQuery5) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add( ldbcQuery.getClass().getName() );
            operationAsList.add( ldbcQuery.personId() );
            operationAsList.add( ldbcQuery.minDate().getTime() );
            operationAsList.add( ldbcQuery.limit() );
            try
            {
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            catch ( IOException e )
            {
                throw new SerializingMarshallingException(
                        format( "Error while trying to serialize result\n%s", operationAsList.toString() ), e );
            }
        }
        case LdbcQuery6.TYPE:
        {
            LdbcQuery6 ldbcQuery = (LdbcQuery6) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add( ldbcQuery.getClass().getName() );
            operationAsList.add( ldbcQuery.personId() );
            operationAsList.add( ldbcQuery.tagName() );
            operationAsList.add( ldbcQuery.limit() );
            try
            {
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            catch ( IOException e )
            {
                throw new SerializingMarshallingException(
                        format( "Error while trying to serialize result\n%s", operationAsList.toString() ), e );
            }
        }
        case LdbcQuery7.TYPE:
        {
            LdbcQuery7 ldbcQuery = (LdbcQuery7) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add( ldbcQuery.getClass().getName() );
            operationAsList.add( ldbcQuery.personId() );
            operationAsList.add( ldbcQuery.limit() );
            try
            {
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            catch ( IOException e )
            {
                throw new SerializingMarshallingException(
                        format( "Error while trying to serialize result\n%s", operationAsList.toString() ), e );
            }
        }
        case LdbcQuery8.TYPE:
        {
            LdbcQuery8 ldbcQuery = (LdbcQuery8) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add( ldbcQuery.getClass().getName() );
            operationAsList.add( ldbcQuery.personId() );
            operationAsList.add( ldbcQuery.limit() );
            try
            {
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            catch ( IOException e )
            {
                throw new SerializingMarshallingException(
                        format( "Error while trying to serialize result\n%s", operationAsList.toString() ), e );
            }
        }
        case LdbcQuery9.TYPE:
        {
            LdbcQuery9 ldbcQuery = (LdbcQuery9) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add( ldbcQuery.getClass().getName() );
            operationAsList.add( ldbcQuery.personId() );
            operationAsList.add( ldbcQuery.maxDate().getTime() );
            operationAsList.add( ldbcQuery.limit() );
            try
            {
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            catch ( IOException e )
            {
                throw new SerializingMarshallingException(
                        format( "Error while trying to serialize result\n%s", operationAsList.toString() ), e );
            }
        }
        case LdbcQuery10.TYPE:
        {
            LdbcQuery10 ldbcQuery = (LdbcQuery10) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add( ldbcQuery.getClass().getName() );
            operationAsList.add( ldbcQuery.personId() );
            operationAsList.add( ldbcQuery.month() );
            operationAsList.add( ldbcQuery.limit() );
            try
            {
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            catch ( IOException e )
            {
                throw new SerializingMarshallingException(
                        format( "Error while trying to serialize result\n%s", operationAsList.toString() ), e );
            }
        }
        case LdbcQuery11.TYPE:
        {
            LdbcQuery11 ldbcQuery = (LdbcQuery11) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add( ldbcQuery.getClass().getName() );
            operationAsList.add( ldbcQuery.personId() );
            operationAsList.add( ldbcQuery.countryName() );
            operationAsList.add( ldbcQuery.workFromYear() );
            operationAsList.add( ldbcQuery.limit() );
            try
            {
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            catch ( IOException e )
            {
                throw new SerializingMarshallingException(
                        format( "Error while trying to serialize result\n%s", operationAsList.toString() ), e );
            }
        }
        case LdbcQuery12.TYPE:
        {
            LdbcQuery12 ldbcQuery = (LdbcQuery12) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add( ldbcQuery.getClass().getName() );
            operationAsList.add( ldbcQuery.personId() );
            operationAsList.add( ldbcQuery.tagClassName() );
            operationAsList.add( ldbcQuery.limit() );
            try
            {
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            catch ( IOException e )
            {
                throw new SerializingMarshallingException(
                        format( "Error while trying to serialize result\n%s", operationAsList.toString() ), e );
            }
        }
        case LdbcQuery13.TYPE:
        {
            LdbcQuery13 ldbcQuery = (LdbcQuery13) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add( ldbcQuery.getClass().getName() );
            operationAsList.add( ldbcQuery.person1Id() );
            operationAsList.add( ldbcQuery.person2Id() );
            try
            {
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            catch ( IOException e )
            {
                throw new SerializingMarshallingException(
                        format( "Error while trying to serialize result\n%s", operationAsList.toString() ), e );
            }
        }
        case LdbcQuery14.TYPE:
        {
            LdbcQuery14 ldbcQuery = (LdbcQuery14) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add( ldbcQuery.getClass().getName() );
            operationAsList.add( ldbcQuery.person1Id() );
            operationAsList.add( ldbcQuery.person2Id() );
            try
            {
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            catch ( IOException e )
            {
                throw new SerializingMarshallingException(
                        format( "Error while trying to serialize result\n%s", operationAsList.toString() ), e );
            }
        }
        case LdbcShortQuery1PersonProfile.TYPE:
        {
            LdbcShortQuery1PersonProfile ldbcQuery = (LdbcShortQuery1PersonProfile) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add( ldbcQuery.getClass().getName() );
            operationAsList.add( ldbcQuery.personId() );
            try
            {
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            catch ( IOException e )
            {
                throw new SerializingMarshallingException(
                        format( "Error while trying to serialize result\n%s", operationAsList.toString() ), e );
            }
        }
        case LdbcShortQuery2PersonPosts.TYPE:
        {
            LdbcShortQuery2PersonPosts ldbcQuery = (LdbcShortQuery2PersonPosts) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add( ldbcQuery.getClass().getName() );
            operationAsList.add( ldbcQuery.personId() );
            operationAsList.add( ldbcQuery.limit() );
            try
            {
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            catch ( IOException e )
            {
                throw new SerializingMarshallingException(
                        format( "Error while trying to serialize result\n%s", operationAsList.toString() ), e );
            }
        }
        case LdbcShortQuery3PersonFriends.TYPE:
        {
            LdbcShortQuery3PersonFriends ldbcQuery = (LdbcShortQuery3PersonFriends) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add( ldbcQuery.getClass().getName() );
            operationAsList.add( ldbcQuery.personId() );
            try
            {
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            catch ( IOException e )
            {
                throw new SerializingMarshallingException(
                        format( "Error while trying to serialize result\n%s", operationAsList.toString() ), e );
            }
        }
        case LdbcShortQuery4MessageContent.TYPE:
        {
            LdbcShortQuery4MessageContent ldbcQuery = (LdbcShortQuery4MessageContent) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add( ldbcQuery.getClass().getName() );
            operationAsList.add( ldbcQuery.messageId() );
            try
            {
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            catch ( IOException e )
            {
                throw new SerializingMarshallingException(
                        format( "Error while trying to serialize result\n%s", operationAsList.toString() ), e );
            }
        }
        case LdbcShortQuery5MessageCreator.TYPE:
        {
            LdbcShortQuery5MessageCreator ldbcQuery = (LdbcShortQuery5MessageCreator) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add( ldbcQuery.getClass().getName() );
            operationAsList.add( ldbcQuery.messageId() );
            try
            {
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            catch ( IOException e )
            {
                throw new SerializingMarshallingException(
                        format( "Error while trying to serialize result\n%s", operationAsList.toString() ), e );
            }
        }
        case LdbcShortQuery6MessageForum.TYPE:
        {
            LdbcShortQuery6MessageForum ldbcQuery = (LdbcShortQuery6MessageForum) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add( ldbcQuery.getClass().getName() );
            operationAsList.add( ldbcQuery.messageId() );
            try
            {
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            catch ( IOException e )
            {
                throw new SerializingMarshallingException(
                        format( "Error while trying to serialize result\n%s", operationAsList.toString() ), e );
            }
        }
        case LdbcShortQuery7MessageReplies.TYPE:
        {
            LdbcShortQuery7MessageReplies ldbcQuery = (LdbcShortQuery7MessageReplies) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add( ldbcQuery.getClass().getName() );
            operationAsList.add( ldbcQuery.messageId() );
            try
            {
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            catch ( IOException e )
            {
                throw new SerializingMarshallingException(
                        format( "Error while trying to serialize result\n%s", operationAsList.toString() ), e );
            }
        }
        case LdbcUpdate1AddPerson.TYPE:
        {
            LdbcUpdate1AddPerson ldbcQuery = (LdbcUpdate1AddPerson) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add( ldbcQuery.getClass().getName() );
            operationAsList.add( ldbcQuery.personId() );
            operationAsList.add( ldbcQuery.personFirstName() );
            operationAsList.add( ldbcQuery.personLastName() );
            operationAsList.add( ldbcQuery.gender() );
            operationAsList.add( ldbcQuery.birthday().getTime() );
            operationAsList.add( ldbcQuery.creationDate().getTime() );
            operationAsList.add( ldbcQuery.locationIp() );
            operationAsList.add( ldbcQuery.browserUsed() );
            operationAsList.add( ldbcQuery.cityId() );
            operationAsList.add( ldbcQuery.languages() );
            operationAsList.add( ldbcQuery.emails() );
            operationAsList.add( ldbcQuery.tagIds() );
            Iterable<Map<String,Object>> studyAt = Lists.newArrayList( Iterables.transform( ldbcQuery.studyAt(),
                    new Function<LdbcUpdate1AddPerson.Organization,Map<String,Object>>()
                    {
                        @Override
                        public Map<String,Object> apply( LdbcUpdate1AddPerson.Organization organization )
                        {
                            Map<String,Object> organizationMap = new HashMap<>();
                            organizationMap.put( "id", organization.organizationId() );
                            organizationMap.put( "year", organization.year() );
                            return organizationMap;
                        }
                    } ) );
            operationAsList.add( studyAt );
            Iterable<Map<String,Object>> workAt = Lists.newArrayList( Iterables
                    .transform( ldbcQuery.workAt(), new Function<LdbcUpdate1AddPerson.Organization,Map<String,Object>>()
                    {
                        @Override
                        public Map<String,Object> apply( LdbcUpdate1AddPerson.Organization organization )
                        {
                            Map<String,Object> organizationMap = new HashMap<>();
                            organizationMap.put( "id", organization.organizationId() );
                            organizationMap.put( "year", organization.year() );
                            return organizationMap;
                        }
                    } ) );
            operationAsList.add( workAt );
            try
            {
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            catch ( IOException e )
            {
                throw new SerializingMarshallingException(
                        format( "Error while trying to serialize result\n%s", operationAsList.toString() ), e );
            }
        }
        case LdbcUpdate2AddPostLike.TYPE:
        {
            LdbcUpdate2AddPostLike ldbcQuery = (LdbcUpdate2AddPostLike) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add( ldbcQuery.getClass().getName() );
            operationAsList.add( ldbcQuery.personId() );
            operationAsList.add( ldbcQuery.postId() );
            operationAsList.add( ldbcQuery.creationDate().getTime() );
            try
            {
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            catch ( IOException e )
            {
                throw new SerializingMarshallingException(
                        format( "Error while trying to serialize result\n%s", operationAsList.toString() ), e );
            }
        }
        case LdbcUpdate3AddCommentLike.TYPE:
        {
            LdbcUpdate3AddCommentLike ldbcQuery = (LdbcUpdate3AddCommentLike) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add( ldbcQuery.getClass().getName() );
            operationAsList.add( ldbcQuery.personId() );
            operationAsList.add( ldbcQuery.commentId() );
            operationAsList.add( ldbcQuery.creationDate().getTime() );
            try
            {
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            catch ( IOException e )
            {
                throw new SerializingMarshallingException(
                        format( "Error while trying to serialize result\n%s", operationAsList.toString() ), e );
            }
        }
        case LdbcUpdate4AddForum.TYPE:
        {
            LdbcUpdate4AddForum ldbcQuery = (LdbcUpdate4AddForum) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add( ldbcQuery.getClass().getName() );
            operationAsList.add( ldbcQuery.forumId() );
            operationAsList.add( ldbcQuery.forumTitle() );
            operationAsList.add( ldbcQuery.creationDate().getTime() );
            operationAsList.add( ldbcQuery.moderatorPersonId() );
            operationAsList.add( ldbcQuery.tagIds() );
            try
            {
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            catch ( IOException e )
            {
                throw new SerializingMarshallingException(
                        format( "Error while trying to serialize result\n%s", operationAsList.toString() ), e );
            }
        }
        case LdbcUpdate5AddForumMembership.TYPE:
        {
            LdbcUpdate5AddForumMembership ldbcQuery = (LdbcUpdate5AddForumMembership) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add( ldbcQuery.getClass().getName() );
            operationAsList.add( ldbcQuery.forumId() );
            operationAsList.add( ldbcQuery.personId() );
            operationAsList.add( ldbcQuery.joinDate().getTime() );
            try
            {
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            catch ( IOException e )
            {
                throw new SerializingMarshallingException(
                        format( "Error while trying to serialize result\n%s", operationAsList.toString() ), e );
            }
        }
        case LdbcUpdate6AddPost.TYPE:
        {
            LdbcUpdate6AddPost ldbcQuery = (LdbcUpdate6AddPost) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add( ldbcQuery.getClass().getName() );
            operationAsList.add( ldbcQuery.postId() );
            operationAsList.add( ldbcQuery.imageFile() );
            operationAsList.add( ldbcQuery.creationDate().getTime() );
            operationAsList.add( ldbcQuery.locationIp() );
            operationAsList.add( ldbcQuery.browserUsed() );
            operationAsList.add( ldbcQuery.language() );
            operationAsList.add( ldbcQuery.content() );
            operationAsList.add( ldbcQuery.length() );
            operationAsList.add( ldbcQuery.authorPersonId() );
            operationAsList.add( ldbcQuery.forumId() );
            operationAsList.add( ldbcQuery.countryId() );
            operationAsList.add( ldbcQuery.tagIds() );
            try
            {
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            catch ( IOException e )
            {
                throw new SerializingMarshallingException(
                        format( "Error while trying to serialize result\n%s", operationAsList.toString() ), e );
            }
        }
        case LdbcUpdate7AddComment.TYPE:
        {
            LdbcUpdate7AddComment ldbcQuery = (LdbcUpdate7AddComment) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add( ldbcQuery.getClass().getName() );
            operationAsList.add( ldbcQuery.commentId() );
            operationAsList.add( ldbcQuery.creationDate() );
            operationAsList.add( ldbcQuery.locationIp() );
            operationAsList.add( ldbcQuery.browserUsed() );
            operationAsList.add( ldbcQuery.content() );
            operationAsList.add( ldbcQuery.length() );
            operationAsList.add( ldbcQuery.authorPersonId() );
            operationAsList.add( ldbcQuery.countryId() );
            operationAsList.add( ldbcQuery.replyToPostId() );
            operationAsList.add( ldbcQuery.replyToCommentId() );
            operationAsList.add( ldbcQuery.tagIds() );
            try
            {
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            catch ( IOException e )
            {
                throw new SerializingMarshallingException(
                        format( "Error while trying to serialize result\n%s", operationAsList.toString() ), e );
            }
        }
        case LdbcUpdate8AddFriendship.TYPE:
        {
            LdbcUpdate8AddFriendship ldbcQuery = (LdbcUpdate8AddFriendship) operation;
            List<Object> operationAsList = new ArrayList<>();
            operationAsList.add( ldbcQuery.getClass().getName() );
            operationAsList.add( ldbcQuery.person1Id() );
            operationAsList.add( ldbcQuery.person2Id() );
            operationAsList.add( ldbcQuery.creationDate().getTime() );
            try
            {
                return OBJECT_MAPPER.writeValueAsString( operationAsList );
            }
            catch ( IOException e )
            {
                throw new SerializingMarshallingException(
                        format( "Error while trying to serialize result\n%s", operationAsList.toString() ), e );
            }
        }
        default:
        {
            throw new SerializingMarshallingException(
                    format(
                            "Workload does not know how to serialize operation\nWorkload: %s\nOperation Type: " +
                            "%s\nOperation: %s",
                            getClass().getName(),
                            operation.getClass().getName(),
                            operation ) );
        }
        }
    }

    @Override
    public Operation<?> marshalOperation( String serializedOperation ) throws SerializingMarshallingException
    {
        List<Object> operationAsList;
        try
        {
            operationAsList = OBJECT_MAPPER.readValue( serializedOperation, TYPE_REFERENCE );
        }
        catch ( IOException e )
        {
            throw new SerializingMarshallingException(
                    format( "Error while parsing serialized results\n%s", serializedOperation ), e );
        }

        String operationTypeName = (String) operationAsList.get( 0 );
        if ( operationTypeName.equals( LdbcQuery1.class.getName() ) )
        {
            long personId = ((Number) operationAsList.get( 1 )).longValue();
            String firstName = (String) operationAsList.get( 2 );
            int limit = ((Number) operationAsList.get( 3 )).intValue();
            return new LdbcQuery1( personId, firstName, limit );
        }

        if ( operationTypeName.equals( LdbcQuery2.class.getName() ) )
        {
            long personId = ((Number) operationAsList.get( 1 )).longValue();
            Date maxDate = new Date( ((Number) operationAsList.get( 2 )).longValue() );
            int limit = ((Number) operationAsList.get( 3 )).intValue();
            return new LdbcQuery2( personId, maxDate, limit );
        }

        if ( operationTypeName.equals( LdbcQuery3.class.getName() ) )
        {
            long personId = ((Number) operationAsList.get( 1 )).longValue();
            String countryXName = (String) operationAsList.get( 2 );
            String countryYName = (String) operationAsList.get( 3 );
            Date startDate = new Date( ((Number) operationAsList.get( 4 )).longValue() );
            int durationDays = ((Number) operationAsList.get( 5 )).intValue();
            int limit = ((Number) operationAsList.get( 6 )).intValue();
            return new LdbcQuery3( personId, countryXName, countryYName, startDate, durationDays, limit );
        }

        if ( operationTypeName.equals( LdbcQuery4.class.getName() ) )
        {
            long personId = ((Number) operationAsList.get( 1 )).longValue();
            Date startDate = new Date( ((Number) operationAsList.get( 2 )).longValue() );
            int durationDays = ((Number) operationAsList.get( 3 )).intValue();
            int limit = ((Number) operationAsList.get( 4 )).intValue();
            return new LdbcQuery4( personId, startDate, durationDays, limit );
        }

        if ( operationTypeName.equals( LdbcQuery5.class.getName() ) )
        {
            long personId = ((Number) operationAsList.get( 1 )).longValue();
            Date minDate = new Date( ((Number) operationAsList.get( 2 )).longValue() );
            int limit = ((Number) operationAsList.get( 3 )).intValue();
            return new LdbcQuery5( personId, minDate, limit );
        }

        if ( operationTypeName.equals( LdbcQuery6.class.getName() ) )
        {
            long personId = ((Number) operationAsList.get( 1 )).longValue();
            String tagName = (String) operationAsList.get( 2 );
            int limit = ((Number) operationAsList.get( 3 )).intValue();
            return new LdbcQuery6( personId, tagName, limit );
        }

        if ( operationTypeName.equals( LdbcQuery7.class.getName() ) )
        {
            long personId = ((Number) operationAsList.get( 1 )).longValue();
            int limit = ((Number) operationAsList.get( 2 )).intValue();
            return new LdbcQuery7( personId, limit );
        }

        if ( operationTypeName.equals( LdbcQuery8.class.getName() ) )
        {
            long personId = ((Number) operationAsList.get( 1 )).longValue();
            int limit = ((Number) operationAsList.get( 2 )).intValue();
            return new LdbcQuery8( personId, limit );
        }

        if ( operationTypeName.equals( LdbcQuery9.class.getName() ) )
        {
            long personId = ((Number) operationAsList.get( 1 )).longValue();
            Date maxDate = new Date( ((Number) operationAsList.get( 2 )).longValue() );
            int limit = ((Number) operationAsList.get( 3 )).intValue();
            return new LdbcQuery9( personId, maxDate, limit );
        }

        if ( operationTypeName.equals( LdbcQuery10.class.getName() ) )
        {
            long personId = ((Number) operationAsList.get( 1 )).longValue();
            int month = ((Number) operationAsList.get( 2 )).intValue();
            int limit = ((Number) operationAsList.get( 3 )).intValue();
            return new LdbcQuery10( personId, month, limit );
        }

        if ( operationTypeName.equals( LdbcQuery11.class.getName() ) )
        {
            long personId = ((Number) operationAsList.get( 1 )).longValue();
            String countryName = (String) operationAsList.get( 2 );
            int workFromYear = ((Number) operationAsList.get( 3 )).intValue();
            int limit = ((Number) operationAsList.get( 4 )).intValue();
            return new LdbcQuery11( personId, countryName, workFromYear, limit );
        }

        if ( operationTypeName.equals( LdbcQuery12.class.getName() ) )
        {
            long personId = ((Number) operationAsList.get( 1 )).longValue();
            String tagClassName = (String) operationAsList.get( 2 );
            int limit = ((Number) operationAsList.get( 3 )).intValue();
            return new LdbcQuery12( personId, tagClassName, limit );
        }

        if ( operationTypeName.equals( LdbcQuery13.class.getName() ) )
        {
            long person1Id = ((Number) operationAsList.get( 1 )).longValue();
            long person2Id = ((Number) operationAsList.get( 2 )).longValue();
            return new LdbcQuery13( person1Id, person2Id );
        }

        if ( operationTypeName.equals( LdbcQuery14.class.getName() ) )
        {
            long person1Id = ((Number) operationAsList.get( 1 )).longValue();
            long person2Id = ((Number) operationAsList.get( 2 )).longValue();
            return new LdbcQuery14( person1Id, person2Id );
        }

        if ( operationTypeName.equals( LdbcShortQuery1PersonProfile.class.getName() ) )
        {
            long personId = ((Number) operationAsList.get( 1 )).longValue();
            return new LdbcShortQuery1PersonProfile( personId );
        }

        if ( operationTypeName.equals( LdbcShortQuery2PersonPosts.class.getName() ) )
        {
            long personId = ((Number) operationAsList.get( 1 )).longValue();
            int limit = ((Number) operationAsList.get( 2 )).intValue();
            return new LdbcShortQuery2PersonPosts( personId, limit );
        }

        if ( operationTypeName.equals( LdbcShortQuery3PersonFriends.class.getName() ) )
        {
            long personId = ((Number) operationAsList.get( 1 )).longValue();
            return new LdbcShortQuery3PersonFriends( personId );
        }

        if ( operationTypeName.equals( LdbcShortQuery4MessageContent.class.getName() ) )
        {
            long messageId = ((Number) operationAsList.get( 1 )).longValue();
            return new LdbcShortQuery4MessageContent( messageId );
        }

        if ( operationTypeName.equals( LdbcShortQuery5MessageCreator.class.getName() ) )
        {
            long messageId = ((Number) operationAsList.get( 1 )).longValue();
            return new LdbcShortQuery5MessageCreator( messageId );
        }

        if ( operationTypeName.equals( LdbcShortQuery6MessageForum.class.getName() ) )
        {
            long messageId = ((Number) operationAsList.get( 1 )).longValue();
            return new LdbcShortQuery6MessageForum( messageId );
        }

        if ( operationTypeName.equals( LdbcShortQuery7MessageReplies.class.getName() ) )
        {
            long messageId = ((Number) operationAsList.get( 1 )).longValue();
            return new LdbcShortQuery7MessageReplies( messageId );
        }

        if ( operationTypeName.equals( LdbcUpdate1AddPerson.class.getName() ) )
        {
            long personId = ((Number) operationAsList.get( 1 )).longValue();
            String personFirstName = (String) operationAsList.get( 2 );
            String personLastName = (String) operationAsList.get( 3 );
            String gender = (String) operationAsList.get( 4 );
            Date birthday = new Date( ((Number) operationAsList.get( 5 )).longValue() );
            Date creationDate = new Date( ((Number) operationAsList.get( 6 )).longValue() );
            String locationIp = (String) operationAsList.get( 7 );
            String browserUsed = (String) operationAsList.get( 8 );
            long cityId = ((Number) operationAsList.get( 9 )).longValue();
            List<String> languages = (List<String>) operationAsList.get( 10 );
            List<String> emails = (List<String>) operationAsList.get( 11 );
            List<Long> tagIds = Lists.newArrayList(
                    Iterables.transform( (List<Number>) operationAsList.get( 12 ), new Function<Number,Long>()
                    {
                        @Override
                        public Long apply( Number number )
                        {
                            return number.longValue();
                        }
                    } ) );
            List<Map<String,Object>> studyAtList = (List<Map<String,Object>>) operationAsList.get( 13 );
            List<LdbcUpdate1AddPerson.Organization> studyAt = Lists.newArrayList( Iterables
                    .transform( studyAtList, new Function<Map<String,Object>,LdbcUpdate1AddPerson.Organization>()
                    {
                        @Override
                        public LdbcUpdate1AddPerson.Organization apply( Map<String,Object> input )
                        {
                            long organizationId = ((Number) input.get( "id" )).longValue();
                            int year = ((Number) input.get( "year" )).intValue();
                            return new LdbcUpdate1AddPerson.Organization( organizationId, year );
                        }
                    } ) );
            List<Map<String,Object>> workAtList = (List<Map<String,Object>>) operationAsList.get( 14 );
            List<LdbcUpdate1AddPerson.Organization> workAt = Lists.newArrayList( Iterables
                    .transform( workAtList, new Function<Map<String,Object>,LdbcUpdate1AddPerson.Organization>()
                    {
                        @Override
                        public LdbcUpdate1AddPerson.Organization apply( Map<String,Object> input )
                        {
                            long organizationId = ((Number) input.get( "id" )).longValue();
                            int year = ((Number) input.get( "year" )).intValue();
                            return new LdbcUpdate1AddPerson.Organization( organizationId, year );
                        }
                    } ) );

            return new LdbcUpdate1AddPerson( personId, personFirstName, personLastName, gender, birthday, creationDate,
                    locationIp, browserUsed, cityId, languages, emails, tagIds, studyAt, workAt );
        }

        if ( operationTypeName.equals( LdbcUpdate2AddPostLike.class.getName() ) )
        {
            long personId = ((Number) operationAsList.get( 1 )).longValue();
            long postId = ((Number) operationAsList.get( 2 )).longValue();
            Date creationDate = new Date( ((Number) operationAsList.get( 3 )).longValue() );

            return new LdbcUpdate2AddPostLike( personId, postId, creationDate );
        }

        if ( operationTypeName.equals( LdbcUpdate3AddCommentLike.class.getName() ) )
        {
            long personId = ((Number) operationAsList.get( 1 )).longValue();
            long commentId = ((Number) operationAsList.get( 2 )).longValue();
            Date creationDate = new Date( ((Number) operationAsList.get( 3 )).longValue() );

            return new LdbcUpdate3AddCommentLike( personId, commentId, creationDate );
        }

        if ( operationTypeName.equals( LdbcUpdate4AddForum.class.getName() ) )
        {
            long forumId = ((Number) operationAsList.get( 1 )).longValue();
            String forumTitle = (String) operationAsList.get( 2 );
            Date creationDate = new Date( ((Number) operationAsList.get( 3 )).longValue() );
            long moderatorPersonId = ((Number) operationAsList.get( 4 )).longValue();
            List<Long> tagIds = Lists.newArrayList(
                    Iterables.transform( (List<Number>) operationAsList.get( 5 ), new Function<Number,Long>()
                    {
                        @Override
                        public Long apply( Number number )
                        {
                            return number.longValue();
                        }
                    } ) );

            return new LdbcUpdate4AddForum( forumId, forumTitle, creationDate, moderatorPersonId, tagIds );
        }


        if ( operationTypeName.equals( LdbcUpdate5AddForumMembership.class.getName() ) )
        {
            long forumId = ((Number) operationAsList.get( 1 )).longValue();
            long personId = ((Number) operationAsList.get( 2 )).longValue();
            Date creationDate = new Date( ((Number) operationAsList.get( 3 )).longValue() );

            return new LdbcUpdate5AddForumMembership( forumId, personId, creationDate );
        }

        if ( operationTypeName.equals( LdbcUpdate6AddPost.class.getName() ) )
        {
            long postId = ((Number) operationAsList.get( 1 )).longValue();
            String imageFile = (String) operationAsList.get( 2 );
            Date creationDate = new Date( ((Number) operationAsList.get( 3 )).longValue() );
            String locationIp = (String) operationAsList.get( 4 );
            String browserUsed = (String) operationAsList.get( 5 );
            String language = (String) operationAsList.get( 6 );
            String content = (String) operationAsList.get( 7 );
            int length = ((Number) operationAsList.get( 8 )).intValue();
            long authorPersonId = ((Number) operationAsList.get( 9 )).longValue();
            long forumId = ((Number) operationAsList.get( 10 )).longValue();
            long countryId = ((Number) operationAsList.get( 11 )).longValue();
            List<Long> tagIds = Lists.newArrayList(
                    Iterables.transform( (List<Number>) operationAsList.get( 12 ), new Function<Number,Long>()
                    {
                        @Override
                        public Long apply( Number number )
                        {
                            return number.longValue();
                        }
                    } ) );

            return new LdbcUpdate6AddPost( postId, imageFile, creationDate, locationIp, browserUsed, language, content,
                    length, authorPersonId, forumId, countryId, tagIds );
        }

        if ( operationTypeName.equals( LdbcUpdate7AddComment.class.getName() ) )
        {
            long commentId = ((Number) operationAsList.get( 1 )).longValue();
            Date creationDate = new Date( ((Number) operationAsList.get( 2 )).longValue() );
            String locationIp = (String) operationAsList.get( 3 );
            String browserUsed = (String) operationAsList.get( 4 );
            String content = (String) operationAsList.get( 5 );
            int length = ((Number) operationAsList.get( 6 )).intValue();
            long authorPersonId = ((Number) operationAsList.get( 7 )).longValue();
            long countryId = ((Number) operationAsList.get( 8 )).longValue();
            long replyToPostId = ((Number) operationAsList.get( 9 )).longValue();
            long replyToCommentId = ((Number) operationAsList.get( 10 )).longValue();
            List<Long> tagIds = Lists.newArrayList(
                    Iterables.transform( (List<Number>) operationAsList.get( 11 ), new Function<Number,Long>()
                    {
                        @Override
                        public Long apply( Number number )
                        {
                            return number.longValue();
                        }
                    } ) );

            return new LdbcUpdate7AddComment( commentId, creationDate, locationIp, browserUsed, content, length,
                    authorPersonId, countryId, replyToPostId, replyToCommentId, tagIds );
        }

        if ( operationTypeName.equals( LdbcUpdate8AddFriendship.class.getName() ) )
        {
            long person1Id = ((Number) operationAsList.get( 1 )).longValue();
            long person2Id = ((Number) operationAsList.get( 2 )).longValue();
            Date creationDate = new Date( ((Number) operationAsList.get( 3 )).longValue() );

            return new LdbcUpdate8AddFriendship( person1Id, person2Id, creationDate );
        }

        throw new SerializingMarshallingException(
                format(
                        "Workload does not know how to marshal operation\nWorkload: %s\nAssumed Operation Type: " +
                        "%s\nSerialized Operation: %s",
                        getClass().getName(),
                        operationTypeName,
                        serializedOperation ) );
    }

    private static final Equator<LdbcQuery14Result> LDBC_QUERY_14_RESULT_EQUATOR = new Equator<LdbcQuery14Result>()
    {
        @Override
        public boolean equate( LdbcQuery14Result result1, LdbcQuery14Result result2 )
        {
            return result1.equals( result2 );
        }

        @Override
        public int hash( LdbcQuery14Result result )
        {
            return 1;
        }
    };

    @Override
    public boolean resultsEqual( Operation<?> operation, Object result1, Object result2 ) throws WorkloadException
    {
        if ( null == result1 || null == result2 )
        {
            return false;
        }
        else if ( operation.type() == LdbcQuery14.TYPE )
        {
            // TODO can this logic not be moved to LdbcQuery14Result class and performed in equals() method?
            /*
            Group results by weight, because results with same weight can come in any order
                Convert
                   [(weight, [ids...]), ...]
                To
                   Map<weight, [(weight, [ids...])]>
             */
            List<LdbcQuery14Result> typedResults1 = (List<LdbcQuery14Result>) result1;
            Map<Double,List<LdbcQuery14Result>> results1ByWeight = new HashMap<>();
            for ( LdbcQuery14Result typedResult : typedResults1 )
            {
                List<LdbcQuery14Result> resultByWeight = results1ByWeight.get( typedResult.pathWeight() );
                if ( null == resultByWeight )
                {
                    resultByWeight = new ArrayList<>();
                }
                resultByWeight.add( typedResult );
                results1ByWeight.put( typedResult.pathWeight(), resultByWeight );
            }

            List<LdbcQuery14Result> typedResults2 = (List<LdbcQuery14Result>) result2;
            Map<Double,List<LdbcQuery14Result>> results2ByWeight = new HashMap<>();
            for ( LdbcQuery14Result typedResult : typedResults2 )
            {
                List<LdbcQuery14Result> resultByWeight = results2ByWeight.get( typedResult.pathWeight() );
                if ( null == resultByWeight )
                {
                    resultByWeight = new ArrayList<>();
                }
                resultByWeight.add( typedResult );
                results2ByWeight.put( typedResult.pathWeight(), resultByWeight );
            }

            /*
            Perform equality check
                - compare set of keys
                - convert list of lists to set of lists & compare contains all for set of lists for each key
             */
            // compare set of keys
            if (!results1ByWeight.keySet().equals(results2ByWeight.keySet()))
            {
                return false;
            }
            // convert list of lists to set of lists & compare contains all for set of lists for each key
            for ( Double weight : results1ByWeight.keySet() )
            {
                if ( results1ByWeight.get( weight ).size() != results2ByWeight.get( weight ).size() )
                {
                    return false;
                }

                if (!CollectionUtils
                        .isEqualCollection(results1ByWeight.get(weight), results2ByWeight.get(weight),
                                LDBC_QUERY_14_RESULT_EQUATOR))
                {
                    return false;
                }
            }

            return true;
        }
        else
        {
            return result1.equals( result2 );
        }
    }

    @Override
    protected BENCHMARK_MODE getMode() {
        return BENCHMARK_MODE.DEFAULT_BENCHMARK_MODE;
    }
}
