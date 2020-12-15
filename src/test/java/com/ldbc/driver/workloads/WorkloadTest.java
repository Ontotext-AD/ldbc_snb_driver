package com.ldbc.driver.workloads;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.ldbc.driver.Operation;
import com.ldbc.driver.Workload;
import com.ldbc.driver.WorkloadStreams;
import com.ldbc.driver.modes.*;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.control.ControlService;
import com.ldbc.driver.control.DriverConfiguration;
import com.ldbc.driver.control.DriverConfigurationException;
import com.ldbc.driver.control.LocalControlService;
import com.ldbc.driver.control.Log4jLoggingServiceFactory;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.generator.RandomDataGeneratorFactory;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.testutils.TestUtils;
import com.ldbc.driver.util.Bucket;
import com.ldbc.driver.util.Histogram;
import com.ldbc.driver.util.Tuple2;
import com.ldbc.driver.validation.DbValidationResult;
import com.ldbc.driver.validation.WorkloadValidationResult;
import com.ldbc.driver.validation.WorkloadValidator;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public abstract class WorkloadTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    TimeSource timeSource = new SystemTimeSource();

    public abstract Workload workload() throws Exception;

    public abstract List<Tuple2<Operation, Object>> operationsAndResults() throws Exception;

    public abstract List<DriverConfiguration> configurations() throws Exception;

    private List<DriverConfiguration> withTempResultDirs(List<DriverConfiguration> configurations)
            throws IOException, DriverConfigurationException {
        List<DriverConfiguration> configurationsWithTempResultDirs = new ArrayList<>();
        for (DriverConfiguration configuration : configurations) {
            configurationsWithTempResultDirs.add(
                    configuration.applyArg(
                            ConsoleAndFileDriverConfiguration.RESULT_DIR_PATH_ARG,
                            temporaryFolder.newFolder().getAbsolutePath()
                    )
            );
        }
        return configurationsWithTempResultDirs;
    }

    private List<DriverConfiguration> withWarmup(List<DriverConfiguration> configurations)
            throws DriverConfigurationException {
        List<DriverConfiguration> configurationsWithSkip = new ArrayList<>();
        for (DriverConfiguration configuration : configurations) {
            configurationsWithSkip.add(
                    (0 == configuration.warmupCount())
                            ? configuration.applyArg(ConsoleAndFileDriverConfiguration.WARMUP_COUNT_ARG, Long.toString(10))
                            : configuration
            );
        }
        return configurationsWithSkip;
    }

    private List<DriverConfiguration> withSkip(List<DriverConfiguration> configurations)
            throws DriverConfigurationException {
        List<DriverConfiguration> configurationsWithSkip = new ArrayList<>();
        for (DriverConfiguration configuration : configurations) {
            configurationsWithSkip.add(
                    (0 == configuration.skipCount())
                            ? configuration.applyArg(ConsoleAndFileDriverConfiguration.SKIP_COUNT_ARG, Long.toString(10))
                            : configuration
            );
        }
        return configurationsWithSkip;
    }

    public abstract List<Tuple2<DriverConfiguration, Histogram<Class, Double>>> configurationsWithExpectedQueryMix()
            throws Exception;

    @Test
    public void shouldHaveOneToOneMappingBetweenOperationClassesAndOperationTypes() throws Exception {
        try (Workload workload = workload()) {
            Map<Integer, Class<? extends Operation>> typeToClassMapping = workload.operationTypeToClassMapping();
            assertThat(
                    typeToClassMapping.keySet().size(),
                    equalTo(Sets.newHashSet(typeToClassMapping.values()).size())
            );
        }
    }

    @Test
    public void shouldHaveNonNegativeTypesForAllOperations() throws Exception {
        try (Workload workload = workload()) {
            for (Map.Entry<Integer, Class<? extends Operation>> entry :
                    workload.operationTypeToClassMapping().entrySet()) {
                assertTrue(
                        format("%s has negative type: %s", entry.getValue().getSimpleName(), entry.getKey()),
                        entry.getKey() >= 0
                );
            }
        }
        for (Tuple2<Operation, Object> operation : operationsAndResults()) {
            assertTrue(
                    format("%s has negative type: %s", operation.getClass().getSimpleName(), operation._1().type()),
                    operation._1().type() >= 0
            );
        }
    }

    @Test
    public void shouldBeAbleToSerializeAndMarshalAllOperations() throws Exception {
        // Given
        try (Workload workload = workload()) {
            List<Tuple2<Operation, Object>> operationsAndResults = operationsAndResults();

            // When

            // Then
            for (Tuple2<Operation, Object> operationsAndResult : operationsAndResults) {
                assertThat(
                        format("original != marshal(serialize(original))\n" +
                                        "Original: %s\n" +
                                        "Serialized: %s\n" +
                                        "Marshaled: %s",
                                operationsAndResult._1(),
                                workload.serializeOperation(
                                        operationsAndResult._1()
                                ),
                                workload.marshalOperation(
                                        workload.serializeOperation(
                                                operationsAndResult._1()
                                        )
                                )
                        ),
                        workload.marshalOperation(
                                workload.serializeOperation(
                                        operationsAndResult._1()
                                )
                        ),
                        equalTo(operationsAndResult._1()));
            }
        }
    }

    @Test
    public void shouldBeAbleToSerializeAndMarshalAllOperationResults() throws Exception {
        // Given
        List<Tuple2<Operation, Object>> operationsAndResults = operationsAndResults();

        // When

        // Then
        for (Tuple2<Operation, Object> operationsAndResult : operationsAndResults) {
            assertThat(
                    format("original != marshal(serialize(original))\n" +
                                    "Original: %s\n" +
                                    "Serialized: %s\n" +
                                    "Marshaled: %s",
                            operationsAndResult._2(),
                            operationsAndResult._1().serializeResult(
                                    operationsAndResult._2()
                            ),
                            operationsAndResult._1().marshalResult(
                                    operationsAndResult._1().serializeResult(
                                            operationsAndResult._2()
                                    )
                            )
                    ),
                    operationsAndResult._1().marshalResult(
                            operationsAndResult._1().serializeResult(
                                    operationsAndResult._2()
                            )
                    ),
                    equalTo(operationsAndResult._2()));
        }
    }

    @Test
    public void shouldGenerateManyOperationsInReasonableTimeForLongReadOnly() throws Exception {
        for (DriverConfiguration configuration : withTempResultDirs(configurations())) {
            long operationCount = 1_000_000;
            long timeoutAsMilli = TimeUnit.SECONDS.toMillis(5);

            try (Workload workload = new ClassNameWorkloadFactory(configuration.getWorkloadClassName())
                    .createWorkload()) {
                workload.init(configuration);
                GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42L));
                Iterator<Operation> operations = gf.limit(
                        WorkloadStreams.mergeSortedByStartTimeExcludingChildOperationGenerators(
                                gf,
                                workload.streams(gf, true)
                        ),
                        operationCount
                );
                long timeout = timeSource.nowAsMilli() + timeoutAsMilli;
                boolean workloadGeneratedOperationsBeforeTimeout =
                        TestUtils.generateBeforeTimeout(operations, timeout, timeSource, operationCount);
                assertTrue(workloadGeneratedOperationsBeforeTimeout);
            }
        }
    }

    @Test
    public void shouldBeRepeatableWhenTwoIdenticalWorkloadsAreUsedWithIdenticalGeneratorFactories() throws Exception {
        for (DriverConfiguration configuration : withSkip(withWarmup(withTempResultDirs(configurations())))) {
            WorkloadFactory workloadFactory = new ClassNameWorkloadFactory(configuration.getWorkloadClassName());
            GeneratorFactory gf1 = new GeneratorFactory(new RandomDataGeneratorFactory(42L));
            GeneratorFactory gf2 = new GeneratorFactory(new RandomDataGeneratorFactory(42L));
            try (Workload workloadA = workloadFactory.createWorkload();
                 Workload workloadB = workloadFactory.createWorkload()) {
                workloadA.init(configuration);
                workloadB.init(configuration);

                List<Class> operationsA = ImmutableList.copyOf(
                        Iterators.transform(
                                gf1.limit(
                                        WorkloadStreams.mergeSortedByStartTimeExcludingChildOperationGenerators(
                                                gf1,
                                                workloadA.streams(gf1, true)
                                        ),
                                        configuration.getOperationCount()
                                ),
                                new Function<Operation, Class>() {
                                    @Override
                                    public Class apply(Operation operation) {
                                        return operation.getClass();
                                    }
                                }
                        )
                );

                List<Class> operationsB = ImmutableList.copyOf(
                        Iterators.transform(
                                gf1.limit(
                                        WorkloadStreams.mergeSortedByStartTimeExcludingChildOperationGenerators(
                                                gf2,
                                                workloadB.streams(gf2, true)
                                        ),
                                        configuration.getOperationCount()
                                ),
                                new Function<Operation, Class>() {
                                    @Override
                                    public Class apply(Operation operation) {
                                        return operation.getClass();
                                    }
                                }
                        )
                );

                assertThat(operationsA.size(), is(operationsB.size()));

                Iterator<Class> operationsAIt = operationsA.iterator();
                Iterator<Class> operationsBIt = operationsB.iterator();

                while (operationsAIt.hasNext()) {
                    Class a = operationsAIt.next();
                    Class b = operationsBIt.next();
                    assertThat(a, equalTo(b));
                }
            }
        }
    }

    @Test
    public void shouldGenerateConfiguredQueryMix()
            throws Exception {
        for (Tuple2<DriverConfiguration, Histogram<Class, Double>> configurationWithExpectedQueryMix :
                configurationsWithExpectedQueryMix()) {
            DriverConfiguration configuration = configurationWithExpectedQueryMix._1();
            Histogram<Class, Double> expectedQueryMix = configurationWithExpectedQueryMix._2();
            Histogram<Class, Long> actualQueryMix = new Histogram<>(0L);
            for (Map.Entry<Bucket<Class>, Double> bucketEntry : expectedQueryMix.getAllBuckets()) {
                actualQueryMix.addBucket(bucketEntry.getKey(), 0L);
            }
            WorkloadFactory workloadFactory = new ClassNameWorkloadFactory(configuration.getWorkloadClassName());
            try (Workload workload = workloadFactory.createWorkload()) {
                workload.init(configuration);

                // When

                GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42L));
                Iterator<Class> operationTypes = Iterators.transform(
                        gf.limit(
                                WorkloadStreams.mergeSortedByStartTimeExcludingChildOperationGenerators(
                                        gf,
                                        workload.streams(gf, true)
                                ),
                                configuration.getOperationCount()
                        ),
                        new Function<Operation, Class>() {
                            @Override
                            public Class apply(Operation operation) {
                                return operation.getClass();
                            }
                        }
                );

                // Then

                actualQueryMix.importValueSequence(operationTypes);

                double tolerance = 0.01d;

                assertTrue(
                        format("Distributions should be within tolerance: %s\n%s\n%s",
                                tolerance,
                                actualQueryMix.toPercentageValues().toPrettyString(),
                                expectedQueryMix.toPercentageValues().toPrettyString()
                        ),
                        Histogram.equalsWithinTolerance(
                                actualQueryMix.toPercentageValues(),
                                expectedQueryMix.toPercentageValues(),
                                tolerance
                        )
                );
            }
        }
    }

    @Test
    public void shouldLoadFromConfigFile() throws Exception {
        for (DriverConfiguration configuration : withSkip(withWarmup(withTempResultDirs(configurations())))) {
            File configurationFile = temporaryFolder.newFile();
            Files.write(configurationFile.toPath(), configuration.toPropertiesString().getBytes());
            assertTrue(configurationFile.exists());

            configuration = ConsoleAndFileDriverConfiguration.fromArgs(new String[]{
                    "-P", configurationFile.getAbsolutePath(),
                    "-dm", "EXECUTE_WORKLOAD"
            });

            ResultsDirectory resultsDirectory = new ResultsDirectory(configuration);

            for (File file : resultsDirectory.expectedFiles()) {
                assertFalse(format("Did not expect file to exist %s", file.getAbsolutePath()), file.exists());
            }

            // When
            ControlService controlService = new LocalControlService(
                    timeSource.nowAsMilli(),
                    configuration,
                    new Log4jLoggingServiceFactory(false),
                    timeSource
            );

            DriverMode driverMode = DriverModeFactory.buildDriverMode(DriverModeType.EXECUTE_WORKLOAD, controlService);
            driverMode.init();
            driverMode.startExecutionAndAwaitCompletion();

            // Then
            for (File file : resultsDirectory.expectedFiles()) {
                assertTrue(file.exists());
            }
            assertThat(resultsDirectory.expectedFiles(), equalTo(resultsDirectory.files()));

            if (configuration.warmupCount() > 0) {
                long resultsLogSize = resultsDirectory.getResultsLogFileLength(true);
                assertThat(
                        format("Expected %s <= entries in results log <= %s\nFound %s\nResults Log: %s",
                                operationCountLower(configuration.warmupCount()),
                                operationCountUpper(configuration.warmupCount()),
                                resultsLogSize,
                                resultsDirectory.getResultsLogFile(true).getAbsolutePath()
                        ),
                        resultsLogSize,
                        allOf(
                                greaterThanOrEqualTo(operationCountLower(configuration.warmupCount())),
                                lessThanOrEqualTo(operationCountUpper(configuration.warmupCount()))
                        )
                );
            }
            long resultsLogSize = resultsDirectory.getResultsLogFileLength(false);
            assertThat(
                    format("Expected %s <= entries in results log <= %s\nFound %s\nResults Log: %s",
                            operationCountLower(configuration.getOperationCount()),
                            operationCountUpper(configuration.getOperationCount()),
                            resultsLogSize,
                            resultsDirectory.getResultsLogFile(false).getAbsolutePath()
                    ),
                    resultsLogSize,
                    allOf(
                            greaterThanOrEqualTo(operationCountLower(configuration.getOperationCount())),
                            lessThanOrEqualTo(operationCountUpper(configuration.getOperationCount()))
                    )
            );
        }
    }

    @Test
    public void shouldAssignMonotonicallyIncreasingScheduledStartTimesToOperations() throws Exception {
        GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42L));

        for (DriverConfiguration configuration : withSkip(withWarmup(withTempResultDirs(configurations())))) {
            try (Workload workload =
                         new ClassNameWorkloadFactory(configuration.getWorkloadClassName()).createWorkload()) {
                workload.init(configuration);

                List<Operation> operations = Lists.newArrayList(
                        gf.limit(
                                WorkloadStreams.mergeSortedByStartTimeExcludingChildOperationGenerators(
                                        gf,
                                        workload.streams(gf, true)
                                ),
                                configuration.getOperationCount()
                        )
                );

                long prevOperationScheduledStartTime = operations.get(0).scheduledStartTimeAsMilli() - 1;
                for (Operation operation : operations) {
                    assertTrue(operation.scheduledStartTimeAsMilli() >= prevOperationScheduledStartTime);
                    prevOperationScheduledStartTime = operation.scheduledStartTimeAsMilli();
                }
            }
        }
    }

    @Test
    public void shouldRunWorkload() throws Exception {
        for (DriverConfiguration configuration : withSkip(withWarmup(withTempResultDirs(configurations())))) {
            ResultsDirectory resultsDirectory = new ResultsDirectory(configuration);

            for (File file : resultsDirectory.expectedFiles()) {
                assertFalse(format("Did not expect file to exist %s", file.getAbsolutePath()), file.exists());
            }

            ControlService controlService = new LocalControlService(
                    timeSource.nowAsMilli(),
                    configuration,
                    new Log4jLoggingServiceFactory(false),
                    timeSource
            );
//            ClientModeType driverMode = controlService.configuration().getDriverMode();
            DriverModeType driverMode = DriverModeType.EXECUTE_WORKLOAD;

            DriverMode clientMode = DriverModeFactory.buildDriverMode(driverMode, controlService);
            clientMode.init();
            clientMode.startExecutionAndAwaitCompletion();

            for (File file : resultsDirectory.expectedFiles()) {
                assertTrue(file.exists());
            }
            assertThat(resultsDirectory.expectedFiles(), equalTo(resultsDirectory.files()));

            if (configuration.warmupCount() > 0) {
                long resultsLogSize = resultsDirectory.getResultsLogFileLength(true);
                assertThat(
                        format("Expected %s <= entries in results log <= %s\nFound %s\nResults Log: %s",
                                operationCountLower(configuration.warmupCount()),
                                operationCountUpper(configuration.warmupCount()),
                                resultsLogSize,
                                resultsDirectory.getResultsLogFile(true).getAbsolutePath()
                        ),
                        resultsLogSize,
                        allOf(
                                greaterThanOrEqualTo(operationCountLower(configuration.warmupCount())),
                                lessThanOrEqualTo(operationCountUpper(configuration.warmupCount()))
                        )
                );
            }
            long resultsLogSize = resultsDirectory.getResultsLogFileLength(false);
            assertThat(
                    format("Expected %s <= entries in results log <= %s\nFound %s\nResults Log: %s",
                            operationCountLower(configuration.getOperationCount()),
                            operationCountUpper(configuration.getOperationCount()),
                            resultsLogSize,
                            resultsDirectory.getResultsLogFile(false).getAbsolutePath()
                    ),
                    resultsLogSize,
                    allOf(
                            greaterThanOrEqualTo(operationCountLower(configuration.getOperationCount())),
                            lessThanOrEqualTo(operationCountUpper(configuration.getOperationCount()))
                    )
            );
        }
    }

    @Test
    public void shouldCreateValidationParametersThenUseThemToPerformDatabaseValidationThenPass() throws Exception {
        for (DriverConfiguration configuration : withSkip(withWarmup(withTempResultDirs(configurations())))) {
            // **************************************************
            // where validation parameters should be written (ensure file does not yet exist)
            // **************************************************
            File validationParamsFile = temporaryFolder.newFile();
            assertThat(validationParamsFile.length(), is(0L));

            ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions validationParams =
                    new ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions(
                            validationParamsFile.getAbsolutePath(),
                            500
                    );

            configuration = configuration.applyArg(
                    ConsoleAndFileDriverConfiguration.CREATE_VALIDATION_PARAMS_ARG,
                    validationParams.toCommandlineString()
            ).applyArg(
                    ConsoleAndFileDriverConfiguration.DRIVER_MODE_ARG,
                    DriverModeType.CREATE_VALIDATION_PARAMS.toString()
            );

            ResultsDirectory resultsDirectory = new ResultsDirectory(configuration);

            for (File file : resultsDirectory.expectedFiles()) {
                assertFalse(format("Did not expect file to exist %s", file.getAbsolutePath()), file.exists());
            }

            // **************************************************
            // create validation parameters file
            // **************************************************
            ControlService controlService = new LocalControlService(
                    timeSource.nowAsMilli(),
                    configuration,
                    new Log4jLoggingServiceFactory(false),
                    timeSource
            );

            DriverModeType driverMode = controlService.getConfiguration().getDriverMode();
            DriverMode clientForValidationFileCreation = DriverModeFactory.buildDriverMode(driverMode, controlService);
            clientForValidationFileCreation.init();
            clientForValidationFileCreation.startExecutionAndAwaitCompletion();

            // **************************************************
            // check that validation file creation worked
            // **************************************************
            assertTrue(validationParamsFile.length() > 0);

            // **************************************************
            // configuration for using validation parameters file to validate the database
            // **************************************************
            configuration = configuration
                    .applyArg(ConsoleAndFileDriverConfiguration.CREATE_VALIDATION_PARAMS_ARG, null)
                    .applyArg(
                            ConsoleAndFileDriverConfiguration.DB_VALIDATION_FILE_PATH_ARG,
                            validationParamsFile.getAbsolutePath()
                    ).applyArg(
                            ConsoleAndFileDriverConfiguration.DRIVER_MODE_ARG,
                            DriverModeType.VALIDATE_DATABASE.toString()
                    );

            // **************************************************
            // validate the database
            // **************************************************
            controlService = new LocalControlService(
                    timeSource.nowAsMilli(),
                    configuration,
                    new Log4jLoggingServiceFactory(false),
                    timeSource
            );

            DriverModeType driverModeForDatabaseValidation = controlService.getConfiguration().getDriverMode();
            DriverMode clientModeForDatabaseValidation = DriverModeFactory.buildDriverMode(driverModeForDatabaseValidation, controlService);
            clientModeForDatabaseValidation.init();
            DbValidationResult dbValidationResult = (DbValidationResult) clientModeForDatabaseValidation.startExecutionAndAwaitCompletion();

            // **************************************************
            // check that validation was successful
            // **************************************************
            assertTrue(validationParamsFile.length() > 0);
            assertThat(dbValidationResult, is(notNullValue()));
            assertTrue(format("Validation with following error\n%s", dbValidationResult.resultMessage()),
                    dbValidationResult.isSuccessful());
        }
    }

    @Test
    public void shouldPassWorkloadValidation() throws Exception {
        for (DriverConfiguration configuration : withSkip(withWarmup(withTempResultDirs(configurations())))) {
            WorkloadValidator workloadValidator = new WorkloadValidator();
            WorkloadValidationResult workloadValidationResult = workloadValidator.validate(
                    new ClassNameWorkloadFactory(configuration.getWorkloadClassName()),
                    configuration,
                    new Log4jLoggingServiceFactory(true)
            );
            assertTrue(workloadValidationResult.errorMessage(), workloadValidationResult.isSuccessful());
        }
    }

    // TODO add tests related to the results log tolerances that are provided by the workload

    public static final double LOWER_PERCENT = 0.9;
    public static final double UPPER_PERCENT = 1.1;
    public static final long DIFFERENCE_ABSOLUTE = 50;

    public static long operationCountLower(long operationCount) {
        return Math.min(
                percent(operationCount, LOWER_PERCENT),
                operationCount - DIFFERENCE_ABSOLUTE
        );
    }

    public static long operationCountUpper(long operationCount) {
        return Math.max(
                percent(operationCount, UPPER_PERCENT),
                operationCount + DIFFERENCE_ABSOLUTE
        );
    }

    public static long percent(long value, double percent) {
        return Math.round(value * percent);
    }
}
