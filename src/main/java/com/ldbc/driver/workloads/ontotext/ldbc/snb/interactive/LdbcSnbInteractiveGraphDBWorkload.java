package com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;
import com.ldbc.driver.Workload;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.WorkloadStreams;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.csv.charseeker.BufferedCharSeeker;
import com.ldbc.driver.csv.charseeker.CharSeeker;
import com.ldbc.driver.csv.charseeker.Extractors;
import com.ldbc.driver.csv.charseeker.Mark;
import com.ldbc.driver.csv.charseeker.Readables;
import com.ldbc.driver.generator.CsvEventStreamReaderBasicCharSeeker;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.util.ClassLoaderHelper;
import com.ldbc.driver.util.ClassLoadingException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery2PersonPosts;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery3PersonFriends;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery7MessageReplies;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveDbValidationParametersFilter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkloadConfiguration;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.Equator;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.lang.String.format;

public class LdbcSnbInteractiveGraphDBWorkload extends Workload
{
    private final List<Closeable> readOperationFileReaders = new ArrayList<>();
    private File readOperation1File;
    private File readOperation2File;
    private File readOperation3File;
    private File readOperation4File;
    private File readOperation5File;
    private File readOperation6File;
    private File readOperation7File;
    private File readOperation8File;
    private File readOperation9File;
    private File readOperation10File;
    private File readOperation11File;
    private File readOperation12File;
    private File readOperation13File;
    private File readOperation14File;

    private long readOperation1InterleaveAsMilli;
    private long readOperation2InterleaveAsMilli;
    private long readOperation3InterleaveAsMilli;
    private long readOperation4InterleaveAsMilli;
    private long readOperation5InterleaveAsMilli;
    private long readOperation6InterleaveAsMilli;
    private long readOperation7InterleaveAsMilli;
    private long readOperation8InterleaveAsMilli;
    private long readOperation9InterleaveAsMilli;
    private long readOperation10InterleaveAsMilli;
    private long readOperation11InterleaveAsMilli;
    private long readOperation12InterleaveAsMilli;
    private long readOperation13InterleaveAsMilli;
    private long readOperation14InterleaveAsMilli;

    private long updateInterleaveAsMilli;
    private double compressionRatio;

    private Set<Class<?>> enabledLongReadOperationTypes;
    private LdbcSnbInteractiveWorkloadConfiguration.UpdateStreamParser parser;

    @Override
    public Map<Integer, Class<? extends Operation<?>>> operationTypeToClassMapping() {
        Map<Integer, Class<? extends Operation<?>>> operationTypeToClassMapping = new HashMap<>();
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
        return operationTypeToClassMapping;
    }

    @Override
    public void onInit(Map<String, String> params) throws WorkloadException {
        List<String> compulsoryKeys = Lists.newArrayList(
                LdbcSnbInteractiveWorkloadConfiguration.PARAMETERS_DIRECTORY);

        compulsoryKeys.addAll(LdbcSnbInteractiveWorkloadConfiguration.LONG_READ_OPERATION_ENABLE_KEYS
                .stream().map(curr -> "ontotext." + curr).collect(Collectors.toList()));

        Set<String> missingPropertyParameters =
                LdbcSnbInteractiveWorkloadConfiguration.missingParameters(params, compulsoryKeys);
        if (!missingPropertyParameters.isEmpty()) {
            throw new WorkloadException(format("Workload could not initialize due to missing parameters: %s",
                    missingPropertyParameters));
        }

        File parametersDir =
                new File(params.get(LdbcSnbInteractiveWorkloadConfiguration.PARAMETERS_DIRECTORY).trim());
        if (!parametersDir.exists()) {
            throw new WorkloadException(
                    format("Parameters directory does not exist: %s", parametersDir.getAbsolutePath()));
        }
        for (String readOperationParamsFilename :
                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_PARAMS_FILENAMES) {
            File readOperationParamsFile = new File(parametersDir, readOperationParamsFilename);
            if (!readOperationParamsFile.exists()) {
                throw new WorkloadException(
                        format("Read operation parameters file does not exist: %s",
                                readOperationParamsFile.getAbsolutePath()));
            }
        }
        readOperation1File =
                new File(parametersDir, LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_1_PARAMS_FILENAME);
        readOperation2File =
                new File(parametersDir, LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_2_PARAMS_FILENAME);
        readOperation3File =
                new File(parametersDir, LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_3_PARAMS_FILENAME);
        readOperation4File =
                new File(parametersDir, LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_4_PARAMS_FILENAME);
        readOperation5File =
                new File(parametersDir, LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_5_PARAMS_FILENAME);
        readOperation7File =
                new File(parametersDir, LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_7_PARAMS_FILENAME);
        readOperation8File =
                new File(parametersDir, LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_8_PARAMS_FILENAME);
        readOperation9File =
                new File(parametersDir, LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_9_PARAMS_FILENAME);
        readOperation6File =
                new File(parametersDir, LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_6_PARAMS_FILENAME);
        readOperation10File =
                new File(parametersDir, LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_10_PARAMS_FILENAME);
        readOperation11File =
                new File(parametersDir, LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_11_PARAMS_FILENAME);
        readOperation12File =
                new File(parametersDir, LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_12_PARAMS_FILENAME);
        readOperation13File =
                new File(parametersDir, LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_13_PARAMS_FILENAME);
        readOperation14File =
                new File(parametersDir, LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_14_PARAMS_FILENAME);

        enabledLongReadOperationTypes = new HashSet<>();
        List<String> graphdbLongReadOperationEnableKeys =
                LdbcSnbInteractiveWorkloadConfiguration.LONG_READ_OPERATION_ENABLE_KEYS
                        .stream().map(curr -> "ontotext." + curr).collect(Collectors.toList());
        for (String longReadOperationEnableKey : graphdbLongReadOperationEnableKeys) {
            String longReadOperationEnabledString = params.get(longReadOperationEnableKey).trim();
            boolean longReadOperationEnabled = Boolean.parseBoolean(longReadOperationEnabledString);
            String longReadOperationClassName =
                    "com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive." +
                            LdbcSnbInteractiveWorkloadConfiguration.removePrefix(
                                    LdbcSnbInteractiveWorkloadConfiguration.removeSuffix(
                                            longReadOperationEnableKey,
                                            LdbcSnbInteractiveWorkloadConfiguration.ENABLE_SUFFIX
                                    ),
                                    LdbcSnbInteractiveWorkloadConfiguration
                                            .LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX
                            );
            try {
                Class<?> longReadOperationClass = ClassLoaderHelper.loadClass(longReadOperationClassName);
                if (longReadOperationEnabled) {
                    enabledLongReadOperationTypes.add(longReadOperationClass);
                }
            } catch (ClassLoadingException e) {
                throw new WorkloadException(
                        format(
                                "Unable to load operation class for parameter: %s\nGuessed incorrect class name: %s",
                                longReadOperationEnableKey, longReadOperationClassName),
                        e
                );
            }
        }

        List<String> frequencyKeys =
                Lists.newArrayList(LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_FREQUENCY_KEYS);
        Set<String> missingFrequencyKeys = LdbcSnbInteractiveWorkloadConfiguration
                .missingParameters(params, frequencyKeys);
        if (!params.containsKey(LdbcSnbInteractiveWorkloadConfiguration.UPDATE_INTERLEAVE)) {
            // if UPDATE_INTERLEAVE is missing but writes are enabled it is an error
            throw new WorkloadException(
                    format("Workload could not initialize. Missing parameter: %s",
                            LdbcSnbInteractiveWorkloadConfiguration.UPDATE_INTERLEAVE));
        }
        updateInterleaveAsMilli =
                Integer.parseInt(params.get(LdbcSnbInteractiveWorkloadConfiguration.UPDATE_INTERLEAVE).trim());
        if (missingFrequencyKeys.isEmpty()) {
            // all frequency arguments were given, compute interleave based on frequencies
            params = LdbcSnbInteractiveWorkloadConfiguration.convertFrequenciesToInterleaves(params);
        } else {
            // if any frequencies are not set, there should be specified interleave times for read queries
            Set<String> missingInterleaveKeys = LdbcSnbInteractiveWorkloadConfiguration.missingParameters(
                    params,
                    LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_INTERLEAVE_KEYS
            );
            if (!missingInterleaveKeys.isEmpty()) {
                throw new WorkloadException(format(
                        "Workload could not initialize. One of the following groups of parameters should be set: %s " +
                                "or %s",
                        missingFrequencyKeys.toString(), missingInterleaveKeys.toString()));
            }
        }

        try {
            readOperation1InterleaveAsMilli = Long.parseLong(
                    params.get("ldbc.snb.interactive.LdbcQuery1_interleave").trim());
            readOperation2InterleaveAsMilli = Long.parseLong(
                    params.get(LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_2_INTERLEAVE_KEY).trim());
            readOperation3InterleaveAsMilli = Long.parseLong(
                    params.get(LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_3_INTERLEAVE_KEY).trim());
            readOperation4InterleaveAsMilli = Long.parseLong(
                    params.get(LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_4_INTERLEAVE_KEY).trim());
            readOperation5InterleaveAsMilli = Long.parseLong(
                    params.get(LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_5_INTERLEAVE_KEY).trim());
            readOperation6InterleaveAsMilli = Long.parseLong(
                    params.get(LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_6_INTERLEAVE_KEY).trim());
            readOperation7InterleaveAsMilli = Long.parseLong(
                    params.get(LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_7_INTERLEAVE_KEY).trim());
            readOperation8InterleaveAsMilli = Long.parseLong(
                    params.get(LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_8_INTERLEAVE_KEY).trim());
            readOperation9InterleaveAsMilli = Long.parseLong(
                    params.get(LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_9_INTERLEAVE_KEY).trim());
            readOperation10InterleaveAsMilli = Long.parseLong(
                    params.get(LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_10_INTERLEAVE_KEY).trim());
            readOperation11InterleaveAsMilli = Long.parseLong(
                    params.get(LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_11_INTERLEAVE_KEY).trim());
            readOperation12InterleaveAsMilli = Long.parseLong(
                    params.get(LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_12_INTERLEAVE_KEY).trim());
            readOperation13InterleaveAsMilli = Long.parseLong(
                    params.get(LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_13_INTERLEAVE_KEY).trim());
            readOperation14InterleaveAsMilli = Long.parseLong(
                    params.get(LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_14_INTERLEAVE_KEY).trim());
        } catch (NumberFormatException e) {
            throw new WorkloadException("Unable to parse one of the read operation interleave values", e);
        }

        String parserString = params.get(LdbcSnbInteractiveWorkloadConfiguration.UPDATE_STREAM_PARSER);
        if (null == parserString) {
            parserString = LdbcSnbInteractiveWorkloadConfiguration.DEFAULT_UPDATE_STREAM_PARSER.name();
        }
        if (!LdbcSnbInteractiveWorkloadConfiguration.isValidParser(parserString)) {
            throw new WorkloadException("Invalid parser: " + parserString);
        }
        this.parser = LdbcSnbInteractiveWorkloadConfiguration.UpdateStreamParser.valueOf(parserString.trim());
        this.compressionRatio = Double.parseDouble(
                params.get(ConsoleAndFileDriverConfiguration.TIME_COMPRESSION_RATIO_ARG).trim()
        );
    }

    @Override
    protected synchronized void onClose() throws IOException {
        for (Closeable readOperationFileReader : readOperationFileReaders) {
            readOperationFileReader.close();
        }
    }

    @Override
    protected WorkloadStreams getStreams(GeneratorFactory gf, boolean hasDbConnected) throws WorkloadException {
        long workloadStartTimeAsMilli = Long.MAX_VALUE;
        WorkloadStreams ldbcSnbInteractiveWorkloadStreams = new WorkloadStreams();
        List<Iterator<?>> asynchronousDependencyStreamsList = new ArrayList<>();
        List<Iterator<?>> asynchronousNonDependencyStreamsList = new ArrayList<>();
        Set<Class<? extends Operation<?>>> dependentAsynchronousOperationTypes = Sets.newHashSet();
        Set<Class<? extends Operation<?>>> dependencyAsynchronousOperationTypes = Sets.newHashSet();

        /* *******
         * *******
         * *******
         *  LONG READS
         * *******
         * *******
         * *******/

        /*
         * Create read operation streams, with specified interleaves
         */
        int bufferSize = 1024 * 1024;
        char columnDelimiter = '|';
        char arrayDelimiter = ';';
        char tupleDelimiter = ',';

        Iterator<Operation<?>> readOperation1Stream;
        {
            CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder =
                    new Query1EventStreamReader.Query1Decoder();
            Extractors extractors = new Extractors(arrayDelimiter, tupleDelimiter);
            CharSeeker charSeeker;
            try {
                charSeeker = new BufferedCharSeeker(
                        Readables.wrap(
                                new InputStreamReader(new FileInputStream(readOperation1File), Charsets.UTF_8)
                        ),
                        bufferSize
                );
            } catch (FileNotFoundException e) {
                throw new WorkloadException(
                        format("Unable to open parameters file: %s", readOperation1File.getAbsolutePath()),
                        e);
            }
            Mark mark = new Mark();
            // skip headers
            try {
                charSeeker.seek(mark, new int[]{columnDelimiter});
                charSeeker.seek(mark, new int[]{columnDelimiter});
            } catch (IOException e) {
                throw new WorkloadException(format("Unable to advance parameters file beyond headers: %s",
                        readOperation1File.getAbsolutePath()), e);
            }

            Iterator<Operation<?>> operation1StreamWithoutTimes = new Query1EventStreamReader(
                    gf.repeating(
                            new CsvEventStreamReaderBasicCharSeeker<>(
                                    charSeeker,
                                    extractors,
                                    mark,
                                    decoder,
                                    columnDelimiter
                            )
                    )
            );

            Iterator<Long> operation1StartTimes =
                    gf.incrementing(workloadStartTimeAsMilli + readOperation1InterleaveAsMilli,
                            readOperation1InterleaveAsMilli);

            readOperation1Stream = gf.assignStartTimes(
                    operation1StartTimes,
                    operation1StreamWithoutTimes
            );

            readOperationFileReaders.add(charSeeker);
        }

        Iterator<Operation<?>> readOperation2Stream;
        {
            CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder =
                    new Query2EventStreamReader.Query2Decoder();
            Extractors extractors = new Extractors(arrayDelimiter, tupleDelimiter);
            CharSeeker charSeeker;
            try {
                charSeeker = new BufferedCharSeeker(
                        Readables.wrap(
                                new InputStreamReader(new FileInputStream(readOperation2File), Charsets.UTF_8)
                        ),
                        bufferSize
                );
            } catch (FileNotFoundException e) {
                throw new WorkloadException(
                        format("Unable to open parameters file: %s", readOperation2File.getAbsolutePath()),
                        e);
            }
            Mark mark = new Mark();
            // skip headers
            try {
                charSeeker.seek(mark, new int[]{columnDelimiter});
                charSeeker.seek(mark, new int[]{columnDelimiter});
            } catch (IOException e) {
                throw new WorkloadException(format("Unable to advance parameters file beyond headers: %s",
                        readOperation2File.getAbsolutePath()), e);
            }

            Iterator<Operation<?>> operation2StreamWithoutTimes = new Query2EventStreamReader(
                    gf.repeating(
                            new CsvEventStreamReaderBasicCharSeeker<>(
                                    charSeeker,
                                    extractors,
                                    mark,
                                    decoder,
                                    columnDelimiter
                            )
                    )
            );

            Iterator<Long> operation2StartTimes =
                    gf.incrementing(workloadStartTimeAsMilli + readOperation2InterleaveAsMilli,
                            readOperation2InterleaveAsMilli);

            readOperation2Stream = gf.assignStartTimes(
                    operation2StartTimes,
                    operation2StreamWithoutTimes
            );

            readOperationFileReaders.add(charSeeker);
        }

        Iterator<Operation<?>> readOperation3Stream;
        {
            CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder =
                    new Query3EventStreamReader.Query3Decoder();
            Extractors extractors = new Extractors(arrayDelimiter, tupleDelimiter);
            CharSeeker charSeeker;
            try {
                charSeeker = new BufferedCharSeeker(
                        Readables.wrap(
                                new InputStreamReader(new FileInputStream(readOperation3File), Charsets.UTF_8)
                        ),
                        bufferSize
                );
            } catch (FileNotFoundException e) {
                throw new WorkloadException(
                        format("Unable to open parameters file: %s", readOperation3File.getAbsolutePath()),
                        e);
            }
            Mark mark = new Mark();
            // skip headers
            try {
                charSeeker.seek(mark, new int[]{columnDelimiter});
                charSeeker.seek(mark, new int[]{columnDelimiter});
                charSeeker.seek(mark, new int[]{columnDelimiter});
                charSeeker.seek(mark, new int[]{columnDelimiter});
                charSeeker.seek(mark, new int[]{columnDelimiter});
            } catch (IOException e) {
                throw new WorkloadException(format("Unable to advance parameters file beyond headers: %s",
                        readOperation3File.getAbsolutePath()), e);
            }

            Iterator<Operation<?>> operation3StreamWithoutTimes = new Query3EventStreamReader(
                    gf.repeating(
                            new CsvEventStreamReaderBasicCharSeeker<>(
                                    charSeeker,
                                    extractors,
                                    mark,
                                    decoder,
                                    columnDelimiter
                            )
                    )
            );

            Iterator<Long> operation3StartTimes =
                    gf.incrementing(workloadStartTimeAsMilli + readOperation3InterleaveAsMilli,
                            readOperation3InterleaveAsMilli);

            readOperation3Stream = gf.assignStartTimes(
                    operation3StartTimes,
                    operation3StreamWithoutTimes
            );

            readOperationFileReaders.add(charSeeker);
        }

        Iterator<Operation<?>> readOperation4Stream;
        {
            CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder =
                    new Query4EventStreamReader.Query4Decoder();
            Extractors extractors = new Extractors(arrayDelimiter, tupleDelimiter);
            CharSeeker charSeeker;
            try {
                charSeeker = new BufferedCharSeeker(
                        Readables.wrap(
                                new InputStreamReader(new FileInputStream(readOperation4File), Charsets.UTF_8)
                        ),
                        bufferSize
                );
            } catch (FileNotFoundException e) {
                throw new WorkloadException(
                        format("Unable to open parameters file: %s", readOperation4File.getAbsolutePath()),
                        e);
            }
            Mark mark = new Mark();
            // skip headers
            try {
                charSeeker.seek(mark, new int[]{columnDelimiter});
                charSeeker.seek(mark, new int[]{columnDelimiter});
                charSeeker.seek(mark, new int[]{columnDelimiter});
            } catch (IOException e) {
                throw new WorkloadException(format("Unable to advance parameters file beyond headers: %s",
                        readOperation4File.getAbsolutePath()), e);
            }

            Iterator<Operation<?>> operation4StreamWithoutTimes = new Query4EventStreamReader(
                    gf.repeating(
                            new CsvEventStreamReaderBasicCharSeeker<>(
                                    charSeeker,
                                    extractors,
                                    mark,
                                    decoder,
                                    columnDelimiter
                            )
                    )
            );

            Iterator<Long> operation4StartTimes =
                    gf.incrementing(workloadStartTimeAsMilli + readOperation4InterleaveAsMilli,
                            readOperation4InterleaveAsMilli);

            readOperation4Stream = gf.assignStartTimes(
                    operation4StartTimes,
                    operation4StreamWithoutTimes
            );

            readOperationFileReaders.add(charSeeker);
        }

        Iterator<Operation<?>> readOperation5Stream;
        {
            CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder =
                    new Query5EventStreamReader.Query5Decoder();
            Extractors extractors = new Extractors(arrayDelimiter, tupleDelimiter);
            CharSeeker charSeeker;
            try {
                charSeeker = new BufferedCharSeeker(
                        Readables.wrap(
                                new InputStreamReader(new FileInputStream(readOperation5File), Charsets.UTF_8)
                        ),
                        bufferSize
                );
            } catch (FileNotFoundException e) {
                throw new WorkloadException(
                        format("Unable to open parameters file: %s", readOperation5File.getAbsolutePath()),
                        e);
            }
            Mark mark = new Mark();
            // skip headers
            try {
                charSeeker.seek(mark, new int[]{columnDelimiter});
                charSeeker.seek(mark, new int[]{columnDelimiter});
            } catch (IOException e) {
                throw new WorkloadException(format("Unable to advance parameters file beyond headers: %s",
                        readOperation5File.getAbsolutePath()), e);
            }

            Iterator<Operation<?>> operation5StreamWithoutTimes = new Query5EventStreamReader(
                    gf.repeating(
                            new CsvEventStreamReaderBasicCharSeeker<>(
                                    charSeeker,
                                    extractors,
                                    mark,
                                    decoder,
                                    columnDelimiter
                            )
                    )
            );

            Iterator<Long> operation5StartTimes =
                    gf.incrementing(workloadStartTimeAsMilli + readOperation5InterleaveAsMilli,
                            readOperation5InterleaveAsMilli);

            readOperation5Stream = gf.assignStartTimes(
                    operation5StartTimes,
                    operation5StreamWithoutTimes
            );

            readOperationFileReaders.add(charSeeker);
        }

        Iterator<Operation<?>> readOperation6Stream;
        {
            CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder =
                    new Query6EventStreamReader.Query6Decoder();
            Extractors extractors = new Extractors(arrayDelimiter, tupleDelimiter);
            CharSeeker charSeeker;
            try {
                charSeeker = new BufferedCharSeeker(
                        Readables.wrap(
                                new InputStreamReader(new FileInputStream(readOperation6File), Charsets.UTF_8)
                        ),
                        bufferSize
                );
            } catch (FileNotFoundException e) {
                throw new WorkloadException(
                        format("Unable to open parameters file: %s", readOperation6File.getAbsolutePath()),
                        e);
            }
            Mark mark = new Mark();
            // skip headers
            try {
                charSeeker.seek(mark, new int[]{columnDelimiter});
                charSeeker.seek(mark, new int[]{columnDelimiter});
            } catch (IOException e) {
                throw new WorkloadException(format("Unable to advance parameters file beyond headers: %s",
                        readOperation6File.getAbsolutePath()), e);
            }

            Iterator<Operation<?>> operation6StreamWithoutTimes = new Query6EventStreamReader(
                    gf.repeating(
                            new CsvEventStreamReaderBasicCharSeeker<>(
                                    charSeeker,
                                    extractors,
                                    mark,
                                    decoder,
                                    columnDelimiter
                            )
                    )
            );

            Iterator<Long> operation6StartTimes =
                    gf.incrementing(workloadStartTimeAsMilli + readOperation6InterleaveAsMilli,
                            readOperation6InterleaveAsMilli);

            readOperation6Stream = gf.assignStartTimes(
                    operation6StartTimes,
                    operation6StreamWithoutTimes
            );

            readOperationFileReaders.add(charSeeker);
        }

        Iterator<Operation<?>> readOperation7Stream;
        {
            CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder =
                    new Query7EventStreamReader.Query7Decoder();
            Extractors extractors = new Extractors(arrayDelimiter, tupleDelimiter);
            CharSeeker charSeeker;
            try {
                charSeeker = new BufferedCharSeeker(
                        Readables.wrap(
                                new InputStreamReader(new FileInputStream(readOperation7File), Charsets.UTF_8)
                        ),
                        bufferSize
                );
            } catch (FileNotFoundException e) {
                throw new WorkloadException(
                        format("Unable to open parameters file: %s", readOperation7File.getAbsolutePath()),
                        e);
            }
            Mark mark = new Mark();
            // skip headers
            try {
                charSeeker.seek(mark, new int[]{columnDelimiter});
            } catch (IOException e) {
                throw new WorkloadException(format("Unable to advance parameters file beyond headers: %s",
                        readOperation7File.getAbsolutePath()), e);
            }

            Iterator<Operation<?>> operation7StreamWithoutTimes = new Query7EventStreamReader(
                    gf.repeating(
                            new CsvEventStreamReaderBasicCharSeeker<>(
                                    charSeeker,
                                    extractors,
                                    mark,
                                    decoder,
                                    columnDelimiter
                            )
                    )
            );

            Iterator<Long> operation7StartTimes =
                    gf.incrementing(workloadStartTimeAsMilli + readOperation7InterleaveAsMilli,
                            readOperation7InterleaveAsMilli);

            readOperation7Stream = gf.assignStartTimes(
                    operation7StartTimes,
                    operation7StreamWithoutTimes
            );

            readOperationFileReaders.add(charSeeker);
        }

        Iterator<Operation<?>> readOperation8Stream;
        {
            CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder =
                    new Query8EventStreamReader.Query8Decoder();
            Extractors extractors = new Extractors(arrayDelimiter, tupleDelimiter);
            CharSeeker charSeeker;
            try {
                charSeeker = new BufferedCharSeeker(
                        Readables.wrap(
                                new InputStreamReader(new FileInputStream(readOperation8File), Charsets.UTF_8)
                        ),
                        bufferSize
                );
            } catch (FileNotFoundException e) {
                throw new WorkloadException(
                        format("Unable to open parameters file: %s", readOperation8File.getAbsolutePath()),
                        e);
            }
            Mark mark = new Mark();
            // skip headers
            try {
                charSeeker.seek(mark, new int[]{columnDelimiter});
            } catch (IOException e) {
                throw new WorkloadException(format("Unable to advance parameters file beyond headers: %s",
                        readOperation8File.getAbsolutePath()), e);
            }

            Iterator<Operation<?>> operation8StreamWithoutTimes = new Query8EventStreamReader(
                    gf.repeating(
                            new CsvEventStreamReaderBasicCharSeeker<>(
                                    charSeeker,
                                    extractors,
                                    mark,
                                    decoder,
                                    columnDelimiter
                            )
                    )
            );

            Iterator<Long> operation8StartTimes =
                    gf.incrementing(workloadStartTimeAsMilli + readOperation8InterleaveAsMilli,
                            readOperation8InterleaveAsMilli);

            readOperation8Stream = gf.assignStartTimes(
                    operation8StartTimes,
                    operation8StreamWithoutTimes
            );

            readOperationFileReaders.add(charSeeker);
        }

        Iterator<Operation<?>> readOperation9Stream;
        {
            CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder =
                    new Query9EventStreamReader.Query9Decoder();
            Extractors extractors = new Extractors(arrayDelimiter, tupleDelimiter);
            CharSeeker charSeeker;
            try {
                charSeeker = new BufferedCharSeeker(
                        Readables.wrap(
                                new InputStreamReader(new FileInputStream(readOperation9File), Charsets.UTF_8)
                        ),
                        bufferSize
                );
            } catch (FileNotFoundException e) {
                throw new WorkloadException(
                        format("Unable to open parameters file: %s", readOperation9File.getAbsolutePath()),
                        e);
            }
            Mark mark = new Mark();
            // skip headers
            try {
                charSeeker.seek(mark, new int[]{columnDelimiter});
                charSeeker.seek(mark, new int[]{columnDelimiter});
            } catch (IOException e) {
                throw new WorkloadException(format("Unable to advance parameters file beyond headers: %s",
                        readOperation9File.getAbsolutePath()), e);
            }

            Iterator<Operation<?>> operation9StreamWithoutTimes = new Query9EventStreamReader(
                    gf.repeating(
                            new CsvEventStreamReaderBasicCharSeeker<>(
                                    charSeeker,
                                    extractors,
                                    mark,
                                    decoder,
                                    columnDelimiter
                            )
                    )
            );

            Iterator<Long> operation9StartTimes =
                    gf.incrementing(workloadStartTimeAsMilli + readOperation9InterleaveAsMilli,
                            readOperation9InterleaveAsMilli);

            readOperation9Stream = gf.assignStartTimes(
                    operation9StartTimes,
                    operation9StreamWithoutTimes
            );

            readOperationFileReaders.add(charSeeker);
        }

        Iterator<Operation<?>> readOperation10Stream;
        {
            CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder =
                    new Query10EventStreamReader.Query10Decoder();
            Extractors extractors = new Extractors(arrayDelimiter, tupleDelimiter);
            CharSeeker charSeeker;
            try {
                charSeeker = new BufferedCharSeeker(
                        Readables.wrap(
                                new InputStreamReader(new FileInputStream(readOperation10File), Charsets.UTF_8)
                        ),
                        bufferSize
                );
            } catch (FileNotFoundException e) {
                throw new WorkloadException(
                        format("Unable to open parameters file: %s", readOperation10File.getAbsolutePath()),
                        e);
            }
            Mark mark = new Mark();
            // skip headers
            try {
                charSeeker.seek(mark, new int[]{columnDelimiter});
                charSeeker.seek(mark, new int[]{columnDelimiter});
            } catch (IOException e) {
                throw new WorkloadException(format("Unable to advance parameters file beyond headers: %s",
                        readOperation10File.getAbsolutePath()), e);
            }

            Iterator<Operation<?>> operation10StreamWithoutTimes = new Query10EventStreamReader(
                    gf.repeating(
                            new CsvEventStreamReaderBasicCharSeeker<>(
                                    charSeeker,
                                    extractors,
                                    mark,
                                    decoder,
                                    columnDelimiter
                            )
                    )
            );

            Iterator<Long> operation10StartTimes =
                    gf.incrementing(workloadStartTimeAsMilli + readOperation10InterleaveAsMilli,
                            readOperation10InterleaveAsMilli);

            readOperation10Stream = gf.assignStartTimes(
                    operation10StartTimes,
                    operation10StreamWithoutTimes
            );

            readOperationFileReaders.add(charSeeker);
        }

        Iterator<Operation<?>> readOperation11Stream;
        {
            CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder =
                    new Query11EventStreamReader.Query11Decoder();
            Extractors extractors = new Extractors(arrayDelimiter, tupleDelimiter);
            CharSeeker charSeeker;
            try {
                charSeeker = new BufferedCharSeeker(
                        Readables.wrap(
                                new InputStreamReader(new FileInputStream(readOperation11File), Charsets.UTF_8)
                        ),
                        bufferSize
                );
            } catch (FileNotFoundException e) {
                throw new WorkloadException(
                        format("Unable to open parameters file: %s", readOperation11File.getAbsolutePath()),
                        e);
            }
            Mark mark = new Mark();
            // skip headers
            try {
                charSeeker.seek(mark, new int[]{columnDelimiter});
                charSeeker.seek(mark, new int[]{columnDelimiter});
                charSeeker.seek(mark, new int[]{columnDelimiter});
            } catch (IOException e) {
                throw new WorkloadException(format("Unable to advance parameters file beyond headers: %s",
                        readOperation11File.getAbsolutePath()), e);
            }

            Iterator<Operation<?>> operation11StreamWithoutTimes = new Query11EventStreamReader(
                    gf.repeating(
                            new CsvEventStreamReaderBasicCharSeeker<>(
                                    charSeeker,
                                    extractors,
                                    mark,
                                    decoder,
                                    columnDelimiter
                            )
                    )
            );

            Iterator<Long> operation11StartTimes =
                    gf.incrementing(workloadStartTimeAsMilli + readOperation11InterleaveAsMilli,
                            readOperation11InterleaveAsMilli);

            readOperation11Stream = gf.assignStartTimes(
                    operation11StartTimes,
                    operation11StreamWithoutTimes
            );

            readOperationFileReaders.add(charSeeker);
        }

        Iterator<Operation<?>> readOperation12Stream;
        {
            CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder =
                    new Query12EventStreamReader.Query12Decoder();
            Extractors extractors = new Extractors(arrayDelimiter, tupleDelimiter);
            CharSeeker charSeeker;
            try {
                charSeeker = new BufferedCharSeeker(
                        Readables.wrap(
                                new InputStreamReader(new FileInputStream(readOperation12File), Charsets.UTF_8)
                        ),
                        bufferSize
                );
            } catch (FileNotFoundException e) {
                throw new WorkloadException(
                        format("Unable to open parameters file: %s", readOperation12File.getAbsolutePath()),
                        e);
            }
            Mark mark = new Mark();
            // skip headers
            try {
                charSeeker.seek(mark, new int[]{columnDelimiter});
                charSeeker.seek(mark, new int[]{columnDelimiter});
            } catch (IOException e) {
                throw new WorkloadException(format("Unable to advance parameters file beyond headers: %s",
                        readOperation12File.getAbsolutePath()), e);
            }

            Iterator<Operation<?>> operation12StreamWithoutTimes = new Query12EventStreamReader(
                    gf.repeating(
                            new CsvEventStreamReaderBasicCharSeeker<>(
                                    charSeeker,
                                    extractors,
                                    mark,
                                    decoder,
                                    columnDelimiter
                            )
                    )
            );

            Iterator<Long> operation12StartTimes =
                    gf.incrementing(workloadStartTimeAsMilli + readOperation12InterleaveAsMilli,
                            readOperation12InterleaveAsMilli);

            readOperation12Stream = gf.assignStartTimes(
                    operation12StartTimes,
                    operation12StreamWithoutTimes
            );

            readOperationFileReaders.add(charSeeker);
        }

        Iterator<Operation<?>> readOperation13Stream;
        {
            CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder =
                    new Query13EventStreamReader.Query13Decoder();
            Extractors extractors = new Extractors(arrayDelimiter, tupleDelimiter);
            CharSeeker charSeeker;
            try {
                charSeeker = new BufferedCharSeeker(
                        Readables.wrap(
                                new InputStreamReader(new FileInputStream(readOperation13File), Charsets.UTF_8)
                        ),
                        bufferSize
                );
            } catch (FileNotFoundException e) {
                throw new WorkloadException(
                        format("Unable to open parameters file: %s", readOperation13File.getAbsolutePath()),
                        e);
            }
            Mark mark = new Mark();
            // skip headers
            try {
                charSeeker.seek(mark, new int[]{columnDelimiter});
                charSeeker.seek(mark, new int[]{columnDelimiter});
            } catch (IOException e) {
                throw new WorkloadException(format("Unable to advance parameters file beyond headers: %s",
                        readOperation13File.getAbsolutePath()), e);
            }

            Iterator<Operation<?>> operation13StreamWithoutTimes = new Query13EventStreamReader(
                    gf.repeating(
                            new CsvEventStreamReaderBasicCharSeeker<>(
                                    charSeeker,
                                    extractors,
                                    mark,
                                    decoder,
                                    columnDelimiter
                            )
                    )
            );

            Iterator<Long> operation13StartTimes =
                    gf.incrementing(workloadStartTimeAsMilli + readOperation13InterleaveAsMilli,
                            readOperation13InterleaveAsMilli);

            readOperation13Stream = gf.assignStartTimes(
                    operation13StartTimes,
                    operation13StreamWithoutTimes
            );

            readOperationFileReaders.add(charSeeker);
        }

        Iterator<Operation<?>> readOperation14Stream;
        {
            CsvEventStreamReaderBasicCharSeeker.EventDecoder<Object[]> decoder =
                    new Query14EventStreamReader.Query14Decoder();
            Extractors extractors = new Extractors(arrayDelimiter, tupleDelimiter);
            CharSeeker charSeeker;
            try {
                charSeeker = new BufferedCharSeeker(
                        Readables.wrap(
                                new InputStreamReader(new FileInputStream(readOperation14File), Charsets.UTF_8)
                        ),
                        bufferSize
                );
            } catch (FileNotFoundException e) {
                throw new WorkloadException(
                        format("Unable to open parameters file: %s", readOperation14File.getAbsolutePath()),
                        e);
            }
            Mark mark = new Mark();
            // skip headers
            try {
                charSeeker.seek(mark, new int[]{columnDelimiter});
                charSeeker.seek(mark, new int[]{columnDelimiter});
            } catch (IOException e) {
                throw new WorkloadException(format("Unable to advance parameters file beyond headers: %s",
                        readOperation14File.getAbsolutePath()), e);
            }

            Iterator<Operation<?>> operation14StreamWithoutTimes = new Query14EventStreamReader(
                    gf.repeating(
                            new CsvEventStreamReaderBasicCharSeeker<>(
                                    charSeeker,
                                    extractors,
                                    mark,
                                    decoder,
                                    columnDelimiter
                            )
                    )
            );

            Iterator<Long> operation14StartTimes =
                    gf.incrementing(workloadStartTimeAsMilli + readOperation14InterleaveAsMilli,
                            readOperation14InterleaveAsMilli);

            readOperation14Stream = gf.assignStartTimes(
                    operation14StartTimes,
                    operation14StreamWithoutTimes
            );

            readOperationFileReaders.add(charSeeker);
        }

        if (enabledLongReadOperationTypes.contains(LdbcQuery1.class)) {
            asynchronousNonDependencyStreamsList.add(readOperation1Stream);
        }
        if (enabledLongReadOperationTypes.contains(LdbcQuery2.class)) {
            asynchronousNonDependencyStreamsList.add(readOperation2Stream);
        }
        if (enabledLongReadOperationTypes.contains(LdbcQuery3.class)) {
            asynchronousNonDependencyStreamsList.add(readOperation3Stream);
        }
        if (enabledLongReadOperationTypes.contains(LdbcQuery4.class)) {
            asynchronousNonDependencyStreamsList.add(readOperation4Stream);
        }
        if (enabledLongReadOperationTypes.contains(LdbcQuery5.class)) {
            asynchronousNonDependencyStreamsList.add(readOperation5Stream);
        }
        if (enabledLongReadOperationTypes.contains(LdbcQuery6.class)) {
            asynchronousNonDependencyStreamsList.add(readOperation6Stream);
        }
        if (enabledLongReadOperationTypes.contains(LdbcQuery7.class)) {
            asynchronousNonDependencyStreamsList.add(readOperation7Stream);
        }
        if (enabledLongReadOperationTypes.contains(LdbcQuery8.class)) {
            asynchronousNonDependencyStreamsList.add(readOperation8Stream);
        }
        if (enabledLongReadOperationTypes.contains(LdbcQuery9.class)) {
            asynchronousNonDependencyStreamsList.add(readOperation9Stream);
        }
        if (enabledLongReadOperationTypes.contains(LdbcQuery10.class)) {
            asynchronousNonDependencyStreamsList.add(readOperation10Stream);
        }
        if (enabledLongReadOperationTypes.contains(LdbcQuery11.class)) {
            asynchronousNonDependencyStreamsList.add(readOperation11Stream);
        }
        if (enabledLongReadOperationTypes.contains(LdbcQuery12.class)) {
            asynchronousNonDependencyStreamsList.add(readOperation12Stream);
        }
        if (enabledLongReadOperationTypes.contains(LdbcQuery13.class)) {
            asynchronousNonDependencyStreamsList.add(readOperation13Stream);
        }
        if (enabledLongReadOperationTypes.contains(LdbcQuery14.class)) {
            asynchronousNonDependencyStreamsList.add(readOperation14Stream);
        }

        /*
         * Merge all dependency asynchronous operation streams, ordered by operation start times
         */
        Iterator<Operation<?>> asynchronousDependencyStreams = gf.mergeSortOperationsByTimeStamp(
                asynchronousDependencyStreamsList.toArray( new Iterator[asynchronousDependencyStreamsList.size()] )
        );
        /*
         * Merge all non dependency asynchronous operation streams, ordered by operation start times
         */
        Iterator<Operation<?>> asynchronousNonDependencyStreams = gf.mergeSortOperationsByTimeStamp(
                asynchronousNonDependencyStreamsList
                        .toArray( new Iterator[asynchronousNonDependencyStreamsList.size()] )
        );

        /* **************
         * **************
         * **************
         *  FINAL STREAMS
         * **************
         * **************
         * **************/

        ldbcSnbInteractiveWorkloadStreams.setAsynchronousStream(
                dependentAsynchronousOperationTypes,
                dependencyAsynchronousOperationTypes,
                asynchronousDependencyStreams,
                asynchronousNonDependencyStreams,
                null
        );

        return ldbcSnbInteractiveWorkloadStreams;
    }

    @Override
    public DbValidationParametersFilter dbValidationParametersFilter(Integer requiredValidationParameterCount) {
        final Set<Class<?>> multiResultOperations = Sets.newHashSet(
                LdbcShortQuery2PersonPosts.class,
                LdbcShortQuery3PersonFriends.class,
                LdbcShortQuery7MessageReplies.class,
                com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery1.class,
                com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery2.class,
//                LdbcQuery3.class,
                com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery4.class,
                com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery5.class,
                com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery6.class,
                com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery7.class,
                com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery8.class,
                com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery9.class,
                com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery10.class,
                com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery11.class,
                com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery12.class,
                com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery14.class
        );


        final Map<Class<?>, Long> remainingRequiredResultsPerLongReadType = new HashMap<>();
        long resultCountsAssignedForLongReadTypesSoFar = 0;
//        for (Class longReadOperationType : enabledLongReadOperationTypes) {
//            remainingRequiredResultsPerLongReadType.put(longReadOperationType, minimumResultCountPerOperationType);
//            resultCountsAssignedForLongReadTypesSoFar =
//                    resultCountsAssignedForLongReadTypesSoFar + minimumResultCountPerOperationType;
//        }

        return new LdbcSnbInteractiveDbValidationParametersFilter(
                multiResultOperations,
                0L,
                new HashMap<>(),
                remainingRequiredResultsPerLongReadType,
                new HashSet<>()
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
    public String serializeOperation(Operation<?> operation) throws SerializingMarshallingException {
        switch (operation.type()) {
            case LdbcQuery1.TYPE: {
                LdbcQuery1 ldbcQuery = (LdbcQuery1) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add(ldbcQuery.getClass().getName());
                operationAsList.add(ldbcQuery.personId());
                operationAsList.add(ldbcQuery.firstName());
                try {
                    return OBJECT_MAPPER.writeValueAsString(operationAsList);
                } catch (IOException e) {
                    throw new SerializingMarshallingException(
                            format("Error while trying to serialize result\n%s", operationAsList.toString()), e);
                }
            }
            case LdbcQuery2.TYPE: {
                LdbcQuery2 ldbcQuery = (LdbcQuery2) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add(ldbcQuery.getClass().getName());
                operationAsList.add(ldbcQuery.personId());
                operationAsList.add(ldbcQuery.maxDate());
                try {
                    return OBJECT_MAPPER.writeValueAsString(operationAsList);
                } catch (IOException e) {
                    throw new SerializingMarshallingException(
                            format("Error while trying to serialize result\n%s", operationAsList.toString()), e);
                }
            }
            case LdbcQuery3.TYPE: {
                LdbcQuery3 ldbcQuery = (LdbcQuery3) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add(ldbcQuery.getClass().getName());
                operationAsList.add(ldbcQuery.personId());
                operationAsList.add(ldbcQuery.countryXName());
                operationAsList.add(ldbcQuery.countryYName());
                operationAsList.add(ldbcQuery.startDate());
                operationAsList.add(ldbcQuery.durationDays());
                try {
                    return OBJECT_MAPPER.writeValueAsString(operationAsList);
                } catch (IOException e) {
                    throw new SerializingMarshallingException(
                            format("Error while trying to serialize result\n%s", operationAsList.toString()), e);
                }
            }
            case LdbcQuery4.TYPE: {
                LdbcQuery4 ldbcQuery = (LdbcQuery4) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add(ldbcQuery.getClass().getName());
                operationAsList.add(ldbcQuery.personId());
                operationAsList.add(ldbcQuery.startDate());
                operationAsList.add(ldbcQuery.durationDays());
                try {
                    return OBJECT_MAPPER.writeValueAsString(operationAsList);
                } catch (IOException e) {
                    throw new SerializingMarshallingException(
                            format("Error while trying to serialize result\n%s", operationAsList.toString()), e);
                }
            }
            case LdbcQuery5.TYPE: {
                LdbcQuery5 ldbcQuery = (LdbcQuery5) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add(ldbcQuery.getClass().getName());
                operationAsList.add(ldbcQuery.personId());
                operationAsList.add(ldbcQuery.minDate());
                try {
                    return OBJECT_MAPPER.writeValueAsString(operationAsList);
                } catch (IOException e) {
                    throw new SerializingMarshallingException(
                            format("Error while trying to serialize result\n%s", operationAsList.toString()), e);
                }
            }
            case LdbcQuery6.TYPE: {
                LdbcQuery6 ldbcQuery = (LdbcQuery6) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add(ldbcQuery.getClass().getName());
                operationAsList.add(ldbcQuery.personId());
                operationAsList.add(ldbcQuery.tagName());
                try {
                    return OBJECT_MAPPER.writeValueAsString(operationAsList);
                } catch (IOException e) {
                    throw new SerializingMarshallingException(
                            format("Error while trying to serialize result\n%s", operationAsList.toString()), e);
                }
            }
            case LdbcQuery7.TYPE: {
                LdbcQuery7 ldbcQuery = (LdbcQuery7) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add(ldbcQuery.getClass().getName());
                operationAsList.add(ldbcQuery.personId());
                try {
                    return OBJECT_MAPPER.writeValueAsString(operationAsList);
                } catch (IOException e) {
                    throw new SerializingMarshallingException(
                            format("Error while trying to serialize result\n%s", operationAsList.toString()), e);
                }
            }
            case LdbcQuery8.TYPE: {
                LdbcQuery8 ldbcQuery = (LdbcQuery8) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add(ldbcQuery.getClass().getName());
                operationAsList.add(ldbcQuery.personId());
                try {
                    return OBJECT_MAPPER.writeValueAsString(operationAsList);
                } catch (IOException e) {
                    throw new SerializingMarshallingException(
                            format("Error while trying to serialize result\n%s", operationAsList.toString()), e);
                }
            }
            case LdbcQuery9.TYPE: {
                LdbcQuery9 ldbcQuery = (LdbcQuery9) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add(ldbcQuery.getClass().getName());
                operationAsList.add(ldbcQuery.personId());
                operationAsList.add(ldbcQuery.maxDate());
                try {
                    return OBJECT_MAPPER.writeValueAsString(operationAsList);
                } catch (IOException e) {
                    throw new SerializingMarshallingException(
                            format("Error while trying to serialize result\n%s", operationAsList.toString()), e);
                }
            }
            case LdbcQuery10.TYPE: {
                LdbcQuery10 ldbcQuery = (LdbcQuery10) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add(ldbcQuery.getClass().getName());
                operationAsList.add(ldbcQuery.personId());
                operationAsList.add(ldbcQuery.month());
                try {
                    return OBJECT_MAPPER.writeValueAsString(operationAsList);
                } catch (IOException e) {
                    throw new SerializingMarshallingException(
                            format("Error while trying to serialize result\n%s", operationAsList.toString()), e);
                }
            }
            case LdbcQuery11.TYPE: {
                LdbcQuery11 ldbcQuery = (LdbcQuery11) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add(ldbcQuery.getClass().getName());
                operationAsList.add(ldbcQuery.personId());
                operationAsList.add(ldbcQuery.countryName());
                operationAsList.add(ldbcQuery.workFromYear());
                try {
                    return OBJECT_MAPPER.writeValueAsString(operationAsList);
                } catch (IOException e) {
                    throw new SerializingMarshallingException(
                            format("Error while trying to serialize result\n%s", operationAsList.toString()), e);
                }
            }
            case LdbcQuery12.TYPE: {
                LdbcQuery12 ldbcQuery = (LdbcQuery12) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add(ldbcQuery.getClass().getName());
                operationAsList.add(ldbcQuery.personId());
                operationAsList.add(ldbcQuery.tagClassName());
                try {
                    return OBJECT_MAPPER.writeValueAsString(operationAsList);
                } catch (IOException e) {
                    throw new SerializingMarshallingException(
                            format("Error while trying to serialize result\n%s", operationAsList.toString()), e);
                }
            }
            case LdbcQuery13.TYPE: {
                LdbcQuery13 ldbcQuery = (LdbcQuery13) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add(ldbcQuery.getClass().getName());
                operationAsList.add(ldbcQuery.person1Id());
                operationAsList.add(ldbcQuery.person2Id());
                try {
                    return OBJECT_MAPPER.writeValueAsString(operationAsList);
                } catch (IOException e) {
                    throw new SerializingMarshallingException(
                            format("Error while trying to serialize result\n%s", operationAsList.toString()), e);
                }
            }
            case LdbcQuery14.TYPE: {
                LdbcQuery14 ldbcQuery = (LdbcQuery14) operation;
                List<Object> operationAsList = new ArrayList<>();
                operationAsList.add(ldbcQuery.getClass().getName());
                operationAsList.add(ldbcQuery.person1Id());
                operationAsList.add(ldbcQuery.person2Id());
                try {
                    return OBJECT_MAPPER.writeValueAsString(operationAsList);
                } catch (IOException e) {
                    throw new SerializingMarshallingException(
                            format("Error while trying to serialize result\n%s", operationAsList.toString()), e);
                }
            }

            default: {
                throw new SerializingMarshallingException(
                        format(
                                "Workload does not know how to serialize operation\nWorkload: %s\nOperation Type: " +
                                        "%s\nOperation: %s",
                                getClass().getName(),
                                operation.getClass().getName(),
                                operation));
            }
        }
    }

    @Override
    public Operation<?> marshalOperation( String serializedOperation ) throws SerializingMarshallingException
    {
        List<Object> operationAsList;
        try {
            operationAsList = OBJECT_MAPPER.readValue(serializedOperation, TYPE_REFERENCE);
        } catch (IOException e) {
            throw new SerializingMarshallingException(
                    format("Error while parsing serialized results\n%s", serializedOperation), e);
        }

        String operationTypeName = (String) operationAsList.get(0);
        if (operationTypeName.equals(LdbcQuery1.class.getName())) {
            long personId = ((Number) operationAsList.get(1)).longValue();
            String firstName = (String) operationAsList.get(2);
            return new LdbcQuery1(personId, firstName);
        }

        if (operationTypeName.equals(LdbcQuery2.class.getName())) {
            long personId = ((Number) operationAsList.get(1)).longValue();
            Date maxDate = new Date( ((Number) operationAsList.get( 2 )).longValue() );
            return new LdbcQuery2(personId, maxDate);
        }

        if (operationTypeName.equals(LdbcQuery3.class.getName())) {
            long personId = ((Number) operationAsList.get(1)).longValue();
            String countryXName = (String) operationAsList.get(2);
            String countryYName = (String) operationAsList.get(3);
            Date startDate = new Date( ((Number) operationAsList.get( 4 )).longValue() );
            int durationDays = ((Number) operationAsList.get(5)).intValue();
            return new LdbcQuery3(personId, countryXName, countryYName, startDate, durationDays);
        }

        if (operationTypeName.equals(LdbcQuery4.class.getName())) {
            long personId = ((Number) operationAsList.get(1)).longValue();
            Date startDate = new Date( ((Number) operationAsList.get( 2 )).longValue() );
            int durationDays = ((Number) operationAsList.get(3)).intValue();
            return new LdbcQuery4(personId, startDate, durationDays);
        }

        if (operationTypeName.equals(LdbcQuery5.class.getName())) {
            long personId = ((Number) operationAsList.get(1)).longValue();
            Date minDate = new Date( ((Number) operationAsList.get( 2 )).longValue() );
            return new LdbcQuery5(personId, minDate);
        }

        if (operationTypeName.equals(LdbcQuery6.class.getName())) {
            long personId = ((Number) operationAsList.get(1)).longValue();
            String tagName = (String) operationAsList.get(2);
            return new LdbcQuery6(personId, tagName);
        }

        if (operationTypeName.equals(LdbcQuery7.class.getName())) {
            long personId = ((Number) operationAsList.get(1)).longValue();
            return new LdbcQuery7(personId);
        }

        if (operationTypeName.equals(LdbcQuery8.class.getName())) {
            long personId = ((Number) operationAsList.get(1)).longValue();
            return new LdbcQuery8(personId);
        }

        if (operationTypeName.equals(LdbcQuery9.class.getName())) {
            long personId = ((Number) operationAsList.get(1)).longValue();
            Date maxDate = new Date( ((Number) operationAsList.get( 2 )).longValue() );
            return new LdbcQuery9(personId, maxDate);
        }

        if (operationTypeName.equals(LdbcQuery10.class.getName())) {
            long personId = ((Number) operationAsList.get(1)).longValue();
            int month = ((Number) operationAsList.get(2)).intValue();
            return new LdbcQuery10(personId, month);
        }

        if (operationTypeName.equals(LdbcQuery11.class.getName())) {
            long personId = ((Number) operationAsList.get(1)).longValue();
            String countryName = (String) operationAsList.get(2);
            int workFromYear = ((Number) operationAsList.get(3)).intValue();
            return new LdbcQuery11(personId, countryName, workFromYear);
        }

        if (operationTypeName.equals(LdbcQuery12.class.getName())) {
            long personId = ((Number) operationAsList.get(1)).longValue();
            String tagClassName = (String) operationAsList.get(2);
            return new LdbcQuery12(personId, tagClassName);
        }

        if (operationTypeName.equals(LdbcQuery13.class.getName())) {
            long person1Id = ((Number) operationAsList.get(1)).longValue();
            long person2Id = ((Number) operationAsList.get(2)).longValue();
            return new LdbcQuery13(person1Id, person2Id);
        }

        if (operationTypeName.equals(LdbcQuery14.class.getName())) {
            long person1Id = ((Number) operationAsList.get(1)).longValue();
            long person2Id = ((Number) operationAsList.get(2)).longValue();
            return new com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery14(person1Id, person2Id);
        }

        throw new SerializingMarshallingException(
                format(
                        "Workload does not know how to marshal operation\nWorkload: %s\nAssumed Operation Type: " +
                        "%s\nSerialized Operation: %s",
                        getClass().getName(),
                        operationTypeName,
                        serializedOperation ) );
    }

    private static final Equator<LdbcQuery14Result> LDBC_QUERY_14_RESULT_EQUATOR = new Equator<LdbcQuery14Result>() {
        @Override
        public boolean equate(LdbcQuery14Result result1, LdbcQuery14Result result2) {
            return result1.equals(result2);
        }

        @Override
        public int hash(LdbcQuery14Result result) {
            return 1;
        }
    };

    @Override
    public boolean resultsEqual(Operation operation, Object result1, Object result2) throws WorkloadException {
        if (null == result1 || null == result2) {
            return false;
        } else if (operation.type() == LdbcQuery14.TYPE) {
            // TODO can this logic not be moved to LdbcQuery14Result class and performed in equals() method?
            /*
            Group results by weight, because results with same weight can come in any order
                Convert
                   [(weight, [ids...]), ...]
                To
                   Map<weight, [(weight, [ids...])]>
             */
            List<LdbcQuery14Result> typedResults1 = (List<LdbcQuery14Result>) result1;
            Map<Double, List<LdbcQuery14Result>> results1ByWeight = new HashMap<>();
            for (LdbcQuery14Result typedResult : typedResults1) {
                List<LdbcQuery14Result> resultByWeight = results1ByWeight.get(typedResult.pathWeight());
                if (null == resultByWeight) {
                    resultByWeight = new ArrayList<>();
                }
                resultByWeight.add(typedResult);
                results1ByWeight.put(typedResult.pathWeight(), resultByWeight);
            }

            List<LdbcQuery14Result> typedResults2 = (List<LdbcQuery14Result>) result2;
            Map<Double, List<LdbcQuery14Result>> results2ByWeight = new HashMap<>();
            for (LdbcQuery14Result typedResult : typedResults2) {
                List<LdbcQuery14Result> resultByWeight = results2ByWeight.get(typedResult.pathWeight());
                if (null == resultByWeight) {
                    resultByWeight = new ArrayList<>();
                }
                resultByWeight.add(typedResult);
                results2ByWeight.put(typedResult.pathWeight(), resultByWeight);
            }

            /*
            Perform equality check
                - compare set of keys
                - convert list of lists to set of lists & compare contains all for set of lists for each key
             */
            // compare set of keys
            if (!results1ByWeight.keySet().equals(results2ByWeight.keySet())) {
                return false;
            }
            // convert list of lists to set of lists & compare contains all for set of lists for each key
            for (Double weight : results1ByWeight.keySet()) {
                if (results1ByWeight.get(weight).size() != results2ByWeight.get(weight).size()) {
                    return false;
                }

                if (!CollectionUtils
                        .isEqualCollection(results1ByWeight.get(weight), results2ByWeight.get(weight),
                                LDBC_QUERY_14_RESULT_EQUATOR)) {
                    return false;
                }
            }

            return true;
        } else {
            return result1.equals(result2);
        }
    }
}
