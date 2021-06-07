package com.ldbc.driver;

import com.google.common.collect.Lists;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.control.DriverConfiguration;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.util.ClassLoaderHelper;
import com.ldbc.driver.util.ClassLoadingException;
import com.ldbc.driver.validation.ResultsLogValidationTolerances;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkloadConfiguration;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

public abstract class Workload implements Closeable {
    public static final long DEFAULT_MAXIMUM_EXPECTED_INTERLEAVE_AS_MILLI = TimeUnit.HOURS.toMillis(1);
    protected List<Closeable> forumUpdateOperationsFileReaders = new ArrayList<>();
    protected List<File> forumUpdateOperationFiles = new ArrayList<>();
    protected List<Closeable> personUpdateOperationsFileReaders = new ArrayList<>();
    protected List<File> personUpdateOperationFiles = new ArrayList<>();

    protected List<Closeable> readOperationFileReaders = new ArrayList<>();
    protected File readOperation1File;
    protected File readOperation2File;
    protected File readOperation3File;
    protected File readOperation4File;
    protected File readOperation5File;
    protected File readOperation6File;
    protected File readOperation7File;
    protected File readOperation8File;
    protected File readOperation9File;
    protected File readOperation10File;
    protected File readOperation11File;
    protected File readOperation12File;
    protected File readOperation13File;
    protected File readOperation14File;

    protected long readOperation1InterleaveAsMilli;
    protected long readOperation2InterleaveAsMilli;
    protected long readOperation3InterleaveAsMilli;
    protected long readOperation4InterleaveAsMilli;
    protected long readOperation5InterleaveAsMilli;
    protected long readOperation6InterleaveAsMilli;
    protected long readOperation7InterleaveAsMilli;
    protected long readOperation8InterleaveAsMilli;
    protected long readOperation9InterleaveAsMilli;
    protected long readOperation10InterleaveAsMilli;
    protected long readOperation11InterleaveAsMilli;
    protected long readOperation12InterleaveAsMilli;
    protected long readOperation13InterleaveAsMilli;
    protected long readOperation14InterleaveAsMilli;

    protected long updateInterleaveAsMilli;
    protected double compressionRatio;
    protected double shortReadDissipationFactor;

    protected Set<Class<?>> enabledLongReadOperationTypes;
    protected Set<Class<?>> enabledShortReadOperationTypes;
    protected Set<Class<?>> enabledWriteOperationTypes;
    protected LdbcSnbInteractiveWorkloadConfiguration.UpdateStreamParser parser;

    private boolean isInitialized = false;
    private boolean isClosed = false;

    public abstract Map<Integer, Class<? extends Operation<?>>> operationTypeToClassMapping();

    public ResultsLogValidationTolerances resultsLogValidationTolerances(
            DriverConfiguration configuration,
            boolean warmup
    ) {
        long excessiveDelayThresholdAsMilli = TimeUnit.SECONDS.toMillis(1);
        double toleratedExcessiveDelayCountPercentage = 0.01;
        long toleratedExcessiveDelayCount =
                (warmup) ? Math.round(configuration.warmupCount() * toleratedExcessiveDelayCountPercentage)
                        : Math.round(configuration.operationCount() * toleratedExcessiveDelayCountPercentage);
        // TODO this should really be percentages instead of absolute numbers
        Map<String, Long> toleratedExcessiveDelayCountPerType = new HashMap<>();
        for (Class<?> operationType : operationTypeToClassMapping().values()) {
            toleratedExcessiveDelayCountPerType.put(operationType.getSimpleName(), 10l);
        }
        return new ResultsLogValidationTolerances(
                excessiveDelayThresholdAsMilli,
                toleratedExcessiveDelayCount,
                toleratedExcessiveDelayCountPerType
        );
    }

    /**
     * Called once to initialize state for workload
     */
    public final void init(DriverConfiguration params) throws WorkloadException {
        if (isInitialized) {
            throw new WorkloadException("Workload may be initialized only once");
        }
        isInitialized = true;
        onInit(params.asMap());
    }

    public void onInit(Map<String, String> params) throws WorkloadException {
        if (params.containsKey(LdbcSnbInteractiveWorkloadConfiguration.UPDATES_DIRECTORY)) {
            String updatesDirectoryPath =
                    params.get(LdbcSnbInteractiveWorkloadConfiguration.UPDATES_DIRECTORY).trim();
            File updatesDirectory = new File(updatesDirectoryPath);
            if (!updatesDirectory.exists()) {
                throw new WorkloadException(format("Updates directory does not exist\nDirectory: %s",
                        updatesDirectory.getAbsolutePath()));
            }
            if (!updatesDirectory.isDirectory()) {
                throw new WorkloadException(format("Updates directory is not a directory\nDirectory: %s",
                        updatesDirectory.getAbsolutePath()));
            }
            forumUpdateOperationFiles = LdbcSnbInteractiveWorkloadConfiguration
                    .forumUpdateFilesInDirectory(updatesDirectory);
            personUpdateOperationFiles =
                    LdbcSnbInteractiveWorkloadConfiguration.personUpdateFilesInDirectory(updatesDirectory);
        } else {
            forumUpdateOperationFiles = new ArrayList<>();
            personUpdateOperationFiles = new ArrayList<>();
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
        for (String longReadOperationEnableKey : LdbcSnbInteractiveWorkloadConfiguration
                .LONG_READ_OPERATION_ENABLE_KEYS) {
            String longReadOperationEnabledString = params.get(longReadOperationEnableKey).trim();
            boolean longReadOperationEnabled = Boolean.parseBoolean(longReadOperationEnabledString);
            String longReadOperationClassName =
                    LdbcSnbInteractiveWorkloadConfiguration.LDBC_INTERACTIVE_PACKAGE_PREFIX +
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

        enabledShortReadOperationTypes = new HashSet<>();
        for (String shortReadOperationEnableKey : LdbcSnbInteractiveWorkloadConfiguration
                .SHORT_READ_OPERATION_ENABLE_KEYS) {
            String shortReadOperationEnabledString = params.get(shortReadOperationEnableKey).trim();
            boolean shortReadOperationEnabled = Boolean.parseBoolean(shortReadOperationEnabledString);
            String shortReadOperationClassName =
                    LdbcSnbInteractiveWorkloadConfiguration.LDBC_INTERACTIVE_PACKAGE_PREFIX +
                            LdbcSnbInteractiveWorkloadConfiguration.removePrefix(
                                    LdbcSnbInteractiveWorkloadConfiguration.removeSuffix(
                                            shortReadOperationEnableKey,
                                            LdbcSnbInteractiveWorkloadConfiguration.ENABLE_SUFFIX
                                    ),
                                    LdbcSnbInteractiveWorkloadConfiguration
                                            .LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX
                            );
            try {
                Class<?> shortReadOperationClass = ClassLoaderHelper.loadClass(shortReadOperationClassName);
                if (shortReadOperationEnabled) {
                    enabledShortReadOperationTypes.add(shortReadOperationClass);
                }
            } catch (ClassLoadingException e) {
                throw new WorkloadException(
                        format(
                                "Unable to load operation class for parameter: %s\nGuessed incorrect class name: %s",
                                shortReadOperationEnableKey, shortReadOperationClassName),
                        e
                );
            }
        }
        if (!enabledShortReadOperationTypes.isEmpty()) {
            if (!params.containsKey(LdbcSnbInteractiveWorkloadConfiguration.SHORT_READ_DISSIPATION)) {
                throw new WorkloadException(format("Configuration parameter missing: %s",
                        LdbcSnbInteractiveWorkloadConfiguration.SHORT_READ_DISSIPATION));
            }
            shortReadDissipationFactor = Double.parseDouble(
                    params.get(LdbcSnbInteractiveWorkloadConfiguration.SHORT_READ_DISSIPATION).trim()
            );
            if (shortReadDissipationFactor < 0 || shortReadDissipationFactor > 1) {
                throw new WorkloadException(
                        format("Configuration parameter %s should be in interval [1.0,0.0] but is: %s",
                                shortReadDissipationFactor));
            }
        }

        enabledWriteOperationTypes = new HashSet<>();
        for (String writeOperationEnableKey : LdbcSnbInteractiveWorkloadConfiguration.WRITE_OPERATION_ENABLE_KEYS) {
            String writeOperationEnabledString = params.get(writeOperationEnableKey).trim();
            boolean writeOperationEnabled = Boolean.parseBoolean(writeOperationEnabledString);
            String writeOperationClassName = LdbcSnbInteractiveWorkloadConfiguration.LDBC_INTERACTIVE_PACKAGE_PREFIX +
                    LdbcSnbInteractiveWorkloadConfiguration.removePrefix(
                            LdbcSnbInteractiveWorkloadConfiguration.removeSuffix(
                                    writeOperationEnableKey,
                                    LdbcSnbInteractiveWorkloadConfiguration.ENABLE_SUFFIX
                            ),
                            LdbcSnbInteractiveWorkloadConfiguration
                                    .LDBC_SNB_INTERACTIVE_PARAM_NAME_PREFIX
                    );
            try {
                Class<?> writeOperationClass = ClassLoaderHelper.loadClass(writeOperationClassName);
                if (writeOperationEnabled) {
                    enabledWriteOperationTypes.add(writeOperationClass);
                }
            } catch (ClassLoadingException e) {
                throw new WorkloadException(
                        format(
                                "Unable to load operation class for parameter: %s\nGuessed incorrect class name: %s",
                                writeOperationEnableKey, writeOperationClassName),
                        e
                );
            }
        }

        List<String> frequencyKeys =
                Lists.newArrayList(LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_FREQUENCY_KEYS);
        Set<String> missingFrequencyKeys = LdbcSnbInteractiveWorkloadConfiguration
                .missingParameters(params, frequencyKeys);
        if (enabledWriteOperationTypes.isEmpty() &&
                !params.containsKey(LdbcSnbInteractiveWorkloadConfiguration.UPDATE_INTERLEAVE)) {
            // if UPDATE_INTERLEAVE is missing and writes are disabled set it to DEFAULT
            params.put(
                    LdbcSnbInteractiveWorkloadConfiguration.UPDATE_INTERLEAVE,
                    LdbcSnbInteractiveWorkloadConfiguration.DEFAULT_UPDATE_INTERLEAVE
            );
        }
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
                    params.get(LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_1_INTERLEAVE_KEY).trim());
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


    public final void close() throws IOException {
        if (isClosed) {
            throw new IOException("Workload may be cleaned up only once");
        }
        isClosed = true;
        onClose();
    }

    protected void onClose() throws IOException {
        for (Closeable forumUpdateOperationsFileReader : forumUpdateOperationsFileReaders) {
            forumUpdateOperationsFileReader.close();
        }

        for (Closeable personUpdateOperationsFileReader : personUpdateOperationsFileReaders) {
            personUpdateOperationsFileReader.close();
        }

        for (Closeable readOperationFileReader : readOperationFileReaders) {
            readOperationFileReader.close();
        }
    }

    public final WorkloadStreams streams(GeneratorFactory gf, boolean hasDbConnected) throws WorkloadException {
        if (!isInitialized) {
            throw new WorkloadException("Workload has not been initialized");
        }
        return getStreams(gf, hasDbConnected);
    }

    protected abstract WorkloadStreams getStreams(GeneratorFactory generators, boolean hasDbConnected)
            throws WorkloadException;

    public DbValidationParametersFilter dbValidationParametersFilter(final Integer requiredValidationParameterCount) {
        return new DbValidationParametersFilter() {
            private final List<Operation<?>> injectedOperations = new ArrayList<>();
            int validationParameterCount = 0;

            @Override
            public boolean useOperation(Operation<?> operation) {
                return true;
            }

            @Override
            public DbValidationParametersFilterResult useOperationAndResultForValidation(
                    Operation<?> operation,
                    Object operationResult) {
                if (validationParameterCount < requiredValidationParameterCount) {
                    validationParameterCount++;
                    return new DbValidationParametersFilterResult(
                            DbValidationParametersFilterAcceptance.ACCEPT_AND_CONTINUE,
                            injectedOperations
                    );
                } else {
                    return new DbValidationParametersFilterResult(
                            DbValidationParametersFilterAcceptance.REJECT_AND_FINISH,
                            injectedOperations
                    );
                }
            }
        };
    }

    public long maxExpectedInterleaveAsMilli() {
        return DEFAULT_MAXIMUM_EXPECTED_INTERLEAVE_AS_MILLI;
    }

    public abstract String serializeOperation(Operation<?> operation) throws SerializingMarshallingException;

    public abstract Operation<?> marshalOperation(String serializedOperation) throws SerializingMarshallingException;

    public abstract boolean resultsEqual(Operation<?> operation, Object result1, Object result2)
            throws WorkloadException;

    public interface DbValidationParametersFilter {
        boolean useOperation(Operation<?> operation);

        DbValidationParametersFilterResult useOperationAndResultForValidation(
                Operation<?> operation,
                Object operationResult);
    }

    public enum DbValidationParametersFilterAcceptance {
        ACCEPT_AND_CONTINUE,
        ACCEPT_AND_FINISH,
        REJECT_AND_CONTINUE,
        REJECT_AND_FINISH;
    }

    public static class DbValidationParametersFilterResult {
        private final DbValidationParametersFilterAcceptance acceptance;
        private final List<Operation<?>> injectedOperations;

        public DbValidationParametersFilterResult(
                DbValidationParametersFilterAcceptance acceptance,
                List<Operation<?>> injectedOperations) {
            this.acceptance = acceptance;
            this.injectedOperations = injectedOperations;
        }

        public DbValidationParametersFilterAcceptance acceptance() {
            return acceptance;
        }

        public List<Operation<?>> injectedOperations() {
            return injectedOperations;
        }
    }

}