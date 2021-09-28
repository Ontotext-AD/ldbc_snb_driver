package com.ldbc.driver;

import com.google.common.base.Charsets;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Sets;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.control.DriverConfiguration;
import com.ldbc.driver.csv.charseeker.*;
import com.ldbc.driver.csv.simple.SimpleCsvFileReader;
import com.ldbc.driver.generator.CsvEventStreamReaderBasicCharSeeker;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.generator.RandomDataGeneratorFactory;
import com.ldbc.driver.util.ClassLoaderHelper;
import com.ldbc.driver.util.ClassLoadingException;
import com.ldbc.driver.util.Tuple;
import com.ldbc.driver.util.Tuple2;
import com.ldbc.driver.validation.ResultsLogValidationTolerances;
import com.ldbc.driver.workloads.common.Query10EventStreamReader;
import com.ldbc.driver.workloads.common.Query11EventStreamReader;
import com.ldbc.driver.workloads.common.Query12EventStreamReader;
import com.ldbc.driver.workloads.common.Query13EventStreamReader;
import com.ldbc.driver.workloads.common.Query14EventStreamReader;
import com.ldbc.driver.workloads.common.Query1EventStreamReader;
import com.ldbc.driver.workloads.common.Query2EventStreamReader;
import com.ldbc.driver.workloads.common.Query3EventStreamReader;
import com.ldbc.driver.workloads.common.Query4EventStreamReader;
import com.ldbc.driver.workloads.common.Query5EventStreamReader;
import com.ldbc.driver.workloads.common.Query6EventStreamReader;
import com.ldbc.driver.workloads.common.Query7EventStreamReader;
import com.ldbc.driver.workloads.common.Query8EventStreamReader;
import com.ldbc.driver.workloads.common.Query9EventStreamReader;
import com.ldbc.driver.workloads.ldbc.snb.interactive.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

public abstract class Workload implements Closeable {

    public enum BENCHMARK_MODE {
        DEFAULT_BENCHMARK_MODE, GRAPHDB_BENCHMARK_MODE
    }

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

	public abstract Map<Integer, Class<? extends Operation>> operationTypeToClassMapping();

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
				throw new WorkloadException(format("Updates directory does not exist%nDirectory: %s",
						updatesDirectory.getAbsolutePath()));
			}
			if (!updatesDirectory.isDirectory()) {
				throw new WorkloadException(format("Updates directory is not a directory%nDirectory: %s",
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
			String packagePrefix = getBenchmarkMode() == BENCHMARK_MODE.GRAPHDB_BENCHMARK_MODE ?
					LdbcSnbInteractiveWorkloadConfiguration.LDBC_GRAPHDB_INTERACTIVE_PACKAGE_PREFIX :
					LdbcSnbInteractiveWorkloadConfiguration.LDBC_INTERACTIVE_PACKAGE_PREFIX;
			String longReadOperationClassName =
					packagePrefix + LdbcSnbInteractiveWorkloadConfiguration.removePrefix(
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
								"Unable to load operation class for parameter: %s%nGuessed incorrect class name: %s",
								longReadOperationEnableKey, longReadOperationClassName),
						e
				);
			}
		}

		enabledShortReadOperationTypes = new HashSet<>();
		if (getBenchmarkMode() != BENCHMARK_MODE.GRAPHDB_BENCHMARK_MODE) {
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
									"Unable to load operation class for parameter: %s%nGuessed incorrect class name: %s",
									shortReadOperationEnableKey, shortReadOperationClassName),
							e
					);
				}
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
								LdbcSnbInteractiveWorkloadConfiguration.SHORT_READ_DISSIPATION, shortReadDissipationFactor));
			}
		}

		enabledWriteOperationTypes = new HashSet<>();
		if (getBenchmarkMode() != BENCHMARK_MODE.GRAPHDB_BENCHMARK_MODE) {
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
									"Unable to load operation class for parameter: %s%nGuessed incorrect class name: %s",
									writeOperationEnableKey, writeOperationClassName),
							e
					);
				}
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

	protected WorkloadStreams getStreams(GeneratorFactory gf, boolean hasDbConnected)
			throws WorkloadException {
		long workloadStartTimeAsMilli = Long.MAX_VALUE;
		WorkloadStreams ldbcSnbInteractiveWorkloadStreams = new WorkloadStreams();
		List<Iterator<?>> asynchronousDependencyStreamsList = new ArrayList<>();
		List<Iterator<?>> asynchronousNonDependencyStreamsList = new ArrayList<>();
		Set<Class<? extends Operation>> dependentAsynchronousOperationTypes = Sets.newHashSet();
		Set<Class<? extends Operation>> dependencyAsynchronousOperationTypes = Sets.newHashSet();

		/* *******
		 * *******
		 * *******
		 *  WRITES
		 * *******
		 * *******
		 * *******/

		// TODO put person/forum update stream pairs into same streams, to half required thread count
		/*
		 * Create person write operation streams
		 */
		if (enabledWriteOperationTypes.contains(LdbcUpdate1AddPerson.class)) {
			for (File personUpdateOperationFile : personUpdateOperationFiles) {
				Iterator<Operation> personUpdateOperationsParser;
				try {
					Tuple2<Iterator<Operation>, Closeable> parserAndCloseable =
							fileToWriteStreamParser(personUpdateOperationFile, parser);
					personUpdateOperationsParser = parserAndCloseable._1();
					personUpdateOperationsFileReaders.add(parserAndCloseable._2());
				} catch (IOException e) {
					throw new WorkloadException(
							"Unable to open person update stream: " + personUpdateOperationFile.getAbsolutePath(), e);
				}
				if (!personUpdateOperationsParser.hasNext()) {
					// Update stream is empty
					System.out.println(
							format(""
											+ "***********************************************\n"
											+ "  !! WARMING !!\n"
											+ "  Update stream is empty: %s\n"
											+ "  Check that data generation process completed successfully\n"
											+ "***********************************************",
									personUpdateOperationFile.getAbsolutePath()
							)
					);
					continue;
				}
				PeekingIterator<Operation> unfilteredPersonUpdateOperations =
						Iterators.peekingIterator(personUpdateOperationsParser);

				try {
					if (unfilteredPersonUpdateOperations.peek().scheduledStartTimeAsMilli() <
							workloadStartTimeAsMilli) {
						workloadStartTimeAsMilli = unfilteredPersonUpdateOperations.peek().scheduledStartTimeAsMilli();
					}
				} catch (NoSuchElementException e) {
					// do nothing, exception just means that stream was empty
				}

				// Filter Write Operations
				Predicate<Operation> enabledWriteOperationsFilter = operation -> enabledWriteOperationTypes.contains(operation.getClass());
				Iterator<Operation> filteredPersonUpdateOperations =
						Iterators.filter(unfilteredPersonUpdateOperations, enabledWriteOperationsFilter);

				Set<Class<? extends Operation>> dependentPersonUpdateOperationTypes = Sets.newHashSet();
				Set<Class<? extends Operation>> dependencyPersonUpdateOperationTypes =
						Sets.newHashSet(
								LdbcUpdate1AddPerson.class
						);

				ChildOperationGenerator personUpdateChildOperationGenerator = null;

				ldbcSnbInteractiveWorkloadStreams.addBlockingStream(
						dependentPersonUpdateOperationTypes,
						dependencyPersonUpdateOperationTypes,
						filteredPersonUpdateOperations,
						Collections.emptyIterator(),
						null
				);
			}
		}

		/*
		 * Create forum write operation streams
		 */
		if (enabledWriteOperationTypes.contains(LdbcUpdate2AddPostLike.class) ||
				enabledWriteOperationTypes.contains(LdbcUpdate3AddCommentLike.class) ||
				enabledWriteOperationTypes.contains(LdbcUpdate4AddForum.class) ||
				enabledWriteOperationTypes.contains(LdbcUpdate5AddForumMembership.class) ||
				enabledWriteOperationTypes.contains(LdbcUpdate6AddPost.class) ||
				enabledWriteOperationTypes.contains(LdbcUpdate7AddComment.class) ||
				enabledWriteOperationTypes.contains(LdbcUpdate8AddFriendship.class)
		) {
			for (File forumUpdateOperationFile : forumUpdateOperationFiles) {
				Iterator<Operation> forumUpdateOperationsParser;
				try {
					Tuple2<Iterator<Operation>, Closeable> parserAndCloseable =
							fileToWriteStreamParser(forumUpdateOperationFile, parser);
					forumUpdateOperationsParser = parserAndCloseable._1();
					forumUpdateOperationsFileReaders.add(parserAndCloseable._2());
				} catch (IOException e) {
					throw new WorkloadException(
							"Unable to open forum update stream: " + forumUpdateOperationFile.getAbsolutePath(), e);
				}
				if (!forumUpdateOperationsParser.hasNext()) {
					// Update stream is empty
					System.out.println(
							format(""
											+ "***********************************************\n"
											+ "  !! WARMING !!\n"
											+ "  Update stream is empty: %s\n"
											+ "  Check that data generation process completed successfully\n"
											+ "***********************************************",
									forumUpdateOperationFile.getAbsolutePath()
							)
					);
					continue;
				}
				PeekingIterator<Operation> unfilteredForumUpdateOperations =
						Iterators.peekingIterator(forumUpdateOperationsParser);

				try {
					if (unfilteredForumUpdateOperations.peek().scheduledStartTimeAsMilli() < workloadStartTimeAsMilli) {
						workloadStartTimeAsMilli = unfilteredForumUpdateOperations.peek().scheduledStartTimeAsMilli();
					}
				} catch (NoSuchElementException e) {
					// do nothing, exception just means that stream was empty
				}

				// Filter Write Operations
				Predicate<Operation> enabledWriteOperationsFilter = operation -> enabledWriteOperationTypes.contains(operation.getClass());
				Iterator<Operation> filteredForumUpdateOperations =
						Iterators.filter(unfilteredForumUpdateOperations, enabledWriteOperationsFilter);

				Set<Class<? extends Operation>> dependentForumUpdateOperationTypes =
						Sets.newHashSet(
								LdbcUpdate2AddPostLike.class,
								LdbcUpdate3AddCommentLike.class,
								LdbcUpdate4AddForum.class,
								LdbcUpdate5AddForumMembership.class,
								LdbcUpdate6AddPost.class,
								LdbcUpdate7AddComment.class,
								LdbcUpdate8AddFriendship.class
						);
				Set<Class<? extends Operation>> dependencyForumUpdateOperationTypes = Sets.newHashSet();

				ChildOperationGenerator forumUpdateChildOperationGenerator = null;

				ldbcSnbInteractiveWorkloadStreams.addBlockingStream(
						dependentForumUpdateOperationTypes,
						dependencyForumUpdateOperationTypes,
						Collections.emptyIterator(),
						filteredForumUpdateOperations,
						forumUpdateChildOperationGenerator
				);
			}
		}

		if (Long.MAX_VALUE == workloadStartTimeAsMilli) {
			workloadStartTimeAsMilli = 0;
		}

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

		Iterator<Operation> readOperation1Stream;
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

			Iterator<Operation> operation1StreamWithoutTimes = new Query1EventStreamReader(
					gf.repeating(
							new CsvEventStreamReaderBasicCharSeeker<>(
									charSeeker,
									extractors,
									mark,
									decoder,
									columnDelimiter
							)
					), getBenchmarkMode()
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

		Iterator<Operation> readOperation2Stream;
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

			Iterator<Operation> operation2StreamWithoutTimes = new Query2EventStreamReader(
					gf.repeating(
							new CsvEventStreamReaderBasicCharSeeker<>(
									charSeeker,
									extractors,
									mark,
									decoder,
									columnDelimiter
							)
					), getBenchmarkMode()
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

		Iterator<Operation> readOperation3Stream;
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

			Iterator<Operation> operation3StreamWithoutTimes = new Query3EventStreamReader(
					gf.repeating(
							new CsvEventStreamReaderBasicCharSeeker<>(
									charSeeker,
									extractors,
									mark,
									decoder,
									columnDelimiter
							)
					), getBenchmarkMode()
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

		Iterator<Operation> readOperation4Stream;
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

			Iterator<Operation> operation4StreamWithoutTimes = new Query4EventStreamReader(
					gf.repeating(
							new CsvEventStreamReaderBasicCharSeeker<>(
									charSeeker,
									extractors,
									mark,
									decoder,
									columnDelimiter
							)
					), getBenchmarkMode()
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

		Iterator<Operation> readOperation5Stream;
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

			Iterator<Operation> operation5StreamWithoutTimes = new Query5EventStreamReader(
					gf.repeating(
							new CsvEventStreamReaderBasicCharSeeker<>(
									charSeeker,
									extractors,
									mark,
									decoder,
									columnDelimiter
							)
					), getBenchmarkMode()
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

		Iterator<Operation> readOperation6Stream;
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

			Iterator<Operation> operation6StreamWithoutTimes = new Query6EventStreamReader(
					gf.repeating(
							new CsvEventStreamReaderBasicCharSeeker<>(
									charSeeker,
									extractors,
									mark,
									decoder,
									columnDelimiter
							)
					), getBenchmarkMode()
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

		Iterator<Operation> readOperation7Stream;
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

			Iterator<Operation> operation7StreamWithoutTimes = new Query7EventStreamReader(
					gf.repeating(
							new CsvEventStreamReaderBasicCharSeeker<>(
									charSeeker,
									extractors,
									mark,
									decoder,
									columnDelimiter
							)
					), getBenchmarkMode()
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

		Iterator<Operation> readOperation8Stream;
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

			Iterator<Operation> operation8StreamWithoutTimes = new Query8EventStreamReader(
					gf.repeating(
							new CsvEventStreamReaderBasicCharSeeker<>(
									charSeeker,
									extractors,
									mark,
									decoder,
									columnDelimiter
							)
					), getBenchmarkMode()
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

		Iterator<Operation> readOperation9Stream;
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

			Iterator<Operation> operation9StreamWithoutTimes = new Query9EventStreamReader(
					gf.repeating(
							new CsvEventStreamReaderBasicCharSeeker<>(
									charSeeker,
									extractors,
									mark,
									decoder,
									columnDelimiter
							)
					), getBenchmarkMode()
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

		Iterator<Operation> readOperation10Stream;
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

			Iterator<Operation> operation10StreamWithoutTimes = new Query10EventStreamReader(
					gf.repeating(
							new CsvEventStreamReaderBasicCharSeeker<>(
									charSeeker,
									extractors,
									mark,
									decoder,
									columnDelimiter
							)
					), getBenchmarkMode()
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

		Iterator<Operation> readOperation11Stream;
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

			Iterator<Operation> operation11StreamWithoutTimes = new Query11EventStreamReader(
					gf.repeating(
							new CsvEventStreamReaderBasicCharSeeker<>(
									charSeeker,
									extractors,
									mark,
									decoder,
									columnDelimiter
							)
					), getBenchmarkMode()
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

		Iterator<Operation> readOperation12Stream;
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

			Iterator<Operation> operation12StreamWithoutTimes = new Query12EventStreamReader(
					gf.repeating(
							new CsvEventStreamReaderBasicCharSeeker<>(
									charSeeker,
									extractors,
									mark,
									decoder,
									columnDelimiter
							)
					), getBenchmarkMode()
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

		Iterator<Operation> readOperation13Stream;
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

			Iterator<Operation> operation13StreamWithoutTimes = new Query13EventStreamReader(
					gf.repeating(
							new CsvEventStreamReaderBasicCharSeeker<>(
									charSeeker,
									extractors,
									mark,
									decoder,
									columnDelimiter
							)
					), getBenchmarkMode()
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

		Iterator<Operation> readOperation14Stream;
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

			Iterator<Operation> operation14StreamWithoutTimes = new Query14EventStreamReader(
					gf.repeating(
							new CsvEventStreamReaderBasicCharSeeker<>(
									charSeeker,
									extractors,
									mark,
									decoder,
									columnDelimiter
							)
					), getBenchmarkMode()
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

		if (enabledLongReadOperationTypes.contains(getClassNameByMode(LdbcQuery1.class.getSimpleName()))) {
			asynchronousNonDependencyStreamsList.add(readOperation1Stream);
		}
		if (enabledLongReadOperationTypes.contains(getClassNameByMode(LdbcQuery2.class.getSimpleName()))) {
			asynchronousNonDependencyStreamsList.add(readOperation2Stream);
		}
		if (enabledLongReadOperationTypes.contains(getClassNameByMode(LdbcQuery3.class.getSimpleName()))) {
			asynchronousNonDependencyStreamsList.add(readOperation3Stream);
		}
		if (enabledLongReadOperationTypes.contains(getClassNameByMode(LdbcQuery4.class.getSimpleName()))) {
			asynchronousNonDependencyStreamsList.add(readOperation4Stream);
		}
		if (enabledLongReadOperationTypes.contains(getClassNameByMode(LdbcQuery5.class.getSimpleName()))) {
			asynchronousNonDependencyStreamsList.add(readOperation5Stream);
		}
		if (enabledLongReadOperationTypes.contains(getClassNameByMode(LdbcQuery6.class.getSimpleName()))) {
			asynchronousNonDependencyStreamsList.add(readOperation6Stream);
		}
		if (enabledLongReadOperationTypes.contains(getClassNameByMode(LdbcQuery7.class.getSimpleName()))) {
			asynchronousNonDependencyStreamsList.add(readOperation7Stream);
		}
		if (enabledLongReadOperationTypes.contains(getClassNameByMode(LdbcQuery8.class.getSimpleName()))) {
			asynchronousNonDependencyStreamsList.add(readOperation8Stream);
		}
		if (enabledLongReadOperationTypes.contains(getClassNameByMode(LdbcQuery9.class.getSimpleName()))) {
			asynchronousNonDependencyStreamsList.add(readOperation9Stream);
		}
		if (enabledLongReadOperationTypes.contains(getClassNameByMode(LdbcQuery10.class.getSimpleName()))) {
			asynchronousNonDependencyStreamsList.add(readOperation10Stream);
		}
		if (enabledLongReadOperationTypes.contains(getClassNameByMode(LdbcQuery11.class.getSimpleName()))) {
			asynchronousNonDependencyStreamsList.add(readOperation11Stream);
		}
		if (enabledLongReadOperationTypes.contains(getClassNameByMode(LdbcQuery12.class.getSimpleName()))) {
			asynchronousNonDependencyStreamsList.add(readOperation12Stream);
		}
		if (enabledLongReadOperationTypes.contains(getClassNameByMode(LdbcQuery13.class.getSimpleName()))) {
			asynchronousNonDependencyStreamsList.add(readOperation13Stream);
		}
		if (enabledLongReadOperationTypes.contains(getClassNameByMode(LdbcQuery14.class.getSimpleName()))) {
			asynchronousNonDependencyStreamsList.add(readOperation14Stream);
		}

		/*
		 * Merge all dependency asynchronous operation streams, ordered by operation start times
		 */
		Iterator<Operation> asynchronousDependencyStreams = gf.mergeSortOperationsByTimeStamp(
				asynchronousDependencyStreamsList.toArray(new Iterator[asynchronousDependencyStreamsList.size()])
		);
		/*
		 * Merge all non dependency asynchronous operation streams, ordered by operation start times
		 */
		Iterator<Operation> asynchronousNonDependencyStreams = gf.mergeSortOperationsByTimeStamp(
				asynchronousNonDependencyStreamsList
						.toArray(new Iterator[asynchronousNonDependencyStreamsList.size()])
		);

		/* *******
		 * *******
		 * *******
		 *  SHORT READS
		 * *******
		 * *******
		 * *******/

		ChildOperationGenerator shortReadsChildGenerator = null;
		if (!enabledShortReadOperationTypes.isEmpty()) {
			Map<Integer, Long> longReadInterleavesAsMilli = new HashMap<>();
			longReadInterleavesAsMilli.put(LdbcQuery1.TYPE, readOperation1InterleaveAsMilli);
			longReadInterleavesAsMilli.put(LdbcQuery2.TYPE, readOperation2InterleaveAsMilli);
			longReadInterleavesAsMilli.put(LdbcQuery3.TYPE, readOperation3InterleaveAsMilli);
			longReadInterleavesAsMilli.put(LdbcQuery4.TYPE, readOperation4InterleaveAsMilli);
			longReadInterleavesAsMilli.put(LdbcQuery5.TYPE, readOperation5InterleaveAsMilli);
			longReadInterleavesAsMilli.put(LdbcQuery6.TYPE, readOperation6InterleaveAsMilli);
			longReadInterleavesAsMilli.put(LdbcQuery7.TYPE, readOperation7InterleaveAsMilli);
			longReadInterleavesAsMilli.put(LdbcQuery8.TYPE, readOperation8InterleaveAsMilli);
			longReadInterleavesAsMilli.put(LdbcQuery9.TYPE, readOperation9InterleaveAsMilli);
			longReadInterleavesAsMilli.put(LdbcQuery10.TYPE, readOperation10InterleaveAsMilli);
			longReadInterleavesAsMilli.put(LdbcQuery11.TYPE, readOperation11InterleaveAsMilli);
			longReadInterleavesAsMilli.put(LdbcQuery12.TYPE, readOperation12InterleaveAsMilli);
			longReadInterleavesAsMilli.put(LdbcQuery13.TYPE, readOperation13InterleaveAsMilli);
			longReadInterleavesAsMilli.put(LdbcQuery14.TYPE, readOperation14InterleaveAsMilli);

			RandomDataGeneratorFactory randomFactory = new RandomDataGeneratorFactory(42l);
			double initialProbability = 1.0;
			Queue<Long> personIdBuffer = (hasDbConnected)
					? LdbcSnbShortReadGenerator.synchronizedCircularQueueBuffer(1024)
					: LdbcSnbShortReadGenerator.constantBuffer(1);
			Queue<Long> messageIdBuffer = (hasDbConnected)
					? LdbcSnbShortReadGenerator.synchronizedCircularQueueBuffer(1024)
					: LdbcSnbShortReadGenerator.constantBuffer(1);
			LdbcSnbShortReadGenerator.SCHEDULED_START_TIME_POLICY scheduledStartTimePolicy = (hasDbConnected)
					?
					LdbcSnbShortReadGenerator.SCHEDULED_START_TIME_POLICY.PREVIOUS_OPERATION_ACTUAL_FINISH_TIME
					:
					LdbcSnbShortReadGenerator.SCHEDULED_START_TIME_POLICY.PREVIOUS_OPERATION_SCHEDULED_START_TIME;
			LdbcSnbShortReadGenerator.BufferReplenishFun bufferReplenishFun = (hasDbConnected)
					? new LdbcSnbShortReadGenerator
					.ResultBufferReplenishFun(
					personIdBuffer, messageIdBuffer)
					: new LdbcSnbShortReadGenerator
					.NoOpBufferReplenishFun();
			shortReadsChildGenerator = new LdbcSnbShortReadGenerator(
					initialProbability,
					shortReadDissipationFactor,
					updateInterleaveAsMilli,
					enabledShortReadOperationTypes,
					compressionRatio,
					personIdBuffer,
					messageIdBuffer,
					randomFactory,
					longReadInterleavesAsMilli,
					scheduledStartTimePolicy,
					bufferReplenishFun
			);
		}

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
				shortReadsChildGenerator
		);

		return ldbcSnbInteractiveWorkloadStreams;
	}

	private Tuple2<Iterator<Operation>, Closeable> fileToWriteStreamParser(File updateOperationsFile,
																			  LdbcSnbInteractiveWorkloadConfiguration.UpdateStreamParser parser) throws IOException, WorkloadException {
		switch (parser) {
			case REGEX: {
				SimpleCsvFileReader csvFileReader = new SimpleCsvFileReader(updateOperationsFile,
						SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_REGEX_STRING);
				return Tuple.tuple2(WriteEventStreamReaderRegex.create(csvFileReader),
						csvFileReader);
			}
			case CHAR_SEEKER: {
				int bufferSize = 1024 * 1024;
//                BufferedCharSeeker charSeeker = new BufferedCharSeeker(Readables.wrap(new FileReader
// (updateOperationsFile)), bufferSize);
				BufferedCharSeeker charSeeker = new BufferedCharSeeker(
						Readables.wrap(
								new InputStreamReader(new FileInputStream(updateOperationsFile), Charsets.UTF_8)
						),
						bufferSize
				);
				Extractors extractors = new Extractors(';', ',');
				return Tuple.tuple2(
						WriteEventStreamReaderCharSeeker.create(charSeeker, extractors, '|'), charSeeker);
			}
			case CHAR_SEEKER_THREAD: {
				int bufferSize = 1024 * 1024;
				BufferedCharSeeker charSeeker = new BufferedCharSeeker(
						ThreadAheadReadable.threadAhead(
								Readables.wrap(
										new InputStreamReader(new FileInputStream(updateOperationsFile), Charsets.UTF_8)
								),
								bufferSize
						),
						bufferSize
				);
				Extractors extractors = new Extractors(';', ',');
				return Tuple.tuple2(
						WriteEventStreamReaderCharSeeker.create(charSeeker, extractors, '|'), charSeeker);
			}
		}
		SimpleCsvFileReader csvFileReader = new SimpleCsvFileReader(updateOperationsFile,
				SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_REGEX_STRING);
		return Tuple.tuple2(WriteEventStreamReaderRegex.create(csvFileReader),
				csvFileReader);
	}

	public DbValidationParametersFilter dbValidationParametersFilter(final Integer requiredValidationParameterCount) {
		return new DbValidationParametersFilter() {
			private final List<Operation> injectedOperations = new ArrayList<>();
			int validationParameterCount = 0;

			@Override
			public boolean useOperation(Operation operation) {
				return true;
			}

			@Override
			public DbValidationParametersFilterResult useOperationAndResultForValidation(
					Operation operation,
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

	public abstract String serializeOperation(Operation operation) throws SerializingMarshallingException;

	public abstract Operation marshalOperation(String serializedOperation) throws SerializingMarshallingException;

	public abstract boolean resultsEqual(Operation operation, Object result1, Object result2)
			throws WorkloadException;

	protected abstract BENCHMARK_MODE getBenchmarkMode();

	public interface DbValidationParametersFilter {
		boolean useOperation(Operation operation);

		DbValidationParametersFilterResult useOperationAndResultForValidation(
				Operation operation,
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
		private final List<Operation> injectedOperations;

		public DbValidationParametersFilterResult(
				DbValidationParametersFilterAcceptance acceptance,
				List<Operation> injectedOperations) {
			this.acceptance = acceptance;
			this.injectedOperations = injectedOperations;
		}

		public DbValidationParametersFilterAcceptance acceptance() {
			return acceptance;
		}

		public List<Operation> injectedOperations() {
			return injectedOperations;
		}
	}

	private Class<?> getClassNameByMode(String className) {
		try {
			return ClassLoaderHelper.loadClass(((getBenchmarkMode() == BENCHMARK_MODE.DEFAULT_BENCHMARK_MODE ?
					LdbcSnbInteractiveWorkloadConfiguration.LDBC_INTERACTIVE_PACKAGE_PREFIX :
					LdbcSnbInteractiveWorkloadConfiguration.LDBC_GRAPHDB_INTERACTIVE_PACKAGE_PREFIX) + className));
		} catch (ClassLoadingException ex) {
			throw new RuntimeException("Could not load class", ex);
		}
	}
}