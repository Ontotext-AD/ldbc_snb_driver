package com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive;

import com.ldbc.driver.Db;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.Operation;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.control.LoggingService;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkloadConfiguration;
import com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive.readers.LdbcUtils;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.http.HTTPRepository;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static java.lang.String.format;

public class LdbcSnbInteractiveGraphDb extends Db {

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
		void sleep(Operation<?> operation, long sleepNs);
	}

	private static SleepFun sleepFun;
	private GraphDbConnectionState connectionState = null;
	private static final Map<Integer, String> BENCHMARK_QUERIES = new HashMap<>();

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

		LdbcUtils.loadQueriesFromDirectory(BENCHMARK_QUERIES, params.get(LdbcSnbInteractiveGraphDBWorkloadConfiguration.LDBC_SNB_QUERY_DIR));

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

	private static void sleep(Operation<?> operation, long sleepNs) {
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
			List<BindingSet> results = dbConnectionState.getGraphDbClient().execute(BENCHMARK_QUERIES.get(LdbcQuery1.TYPE), operation.parameterMap());

			resultReporter.report(200, GraphDBLdbcSnbInteractiveOperationResultSets.read1Results(results), operation);
		}
	}

	public static class LdbcQuery2Handler implements OperationHandler<LdbcQuery2, GraphDbConnectionState> {
		@Override
		public void executeOperation(LdbcQuery2 operation, GraphDbConnectionState dbConnectionState,
									 ResultReporter resultReporter) throws DbException {
			sleep(operation, sleepDurationAsNano);
			List<BindingSet> results = dbConnectionState.getGraphDbClient().execute(BENCHMARK_QUERIES.get(LdbcQuery2.TYPE), operation.parameterMap());
			resultReporter.report(200, GraphDBLdbcSnbInteractiveOperationResultSets.read2Results(results), operation);
		}
	}

	public static class LdbcQuery3Handler implements OperationHandler<LdbcQuery3, GraphDbConnectionState> {
		@Override
		public void executeOperation(LdbcQuery3 operation, GraphDbConnectionState dbConnectionState,
									 ResultReporter resultReporter) throws DbException {
			sleep(operation, sleepDurationAsNano);
			List<BindingSet> results = dbConnectionState.getGraphDbClient().execute(BENCHMARK_QUERIES.get(LdbcQuery3.TYPE), operation.parameterMap());
			resultReporter.report(200, GraphDBLdbcSnbInteractiveOperationResultSets.read3Results(results), operation);
		}
	}

	public static class LdbcQuery4Handler implements OperationHandler<LdbcQuery4, GraphDbConnectionState> {
		@Override
		public void executeOperation(LdbcQuery4 operation, GraphDbConnectionState dbConnectionState,
									 ResultReporter resultReporter) throws DbException {
			sleep(operation, sleepDurationAsNano);
			List<BindingSet> results = dbConnectionState.getGraphDbClient().execute(BENCHMARK_QUERIES.get(LdbcQuery4.TYPE), operation.parameterMap());

			resultReporter.report(200, GraphDBLdbcSnbInteractiveOperationResultSets.read4Results(results), operation);
		}
	}

	public static class LdbcQuery5Handler implements OperationHandler<LdbcQuery5, GraphDbConnectionState> {
		@Override
		public void executeOperation(LdbcQuery5 operation, GraphDbConnectionState dbConnectionState,
									 ResultReporter resultReporter) throws DbException {
			sleep(operation, sleepDurationAsNano);
			List<BindingSet> results = dbConnectionState.getGraphDbClient().execute(BENCHMARK_QUERIES.get(LdbcQuery5.TYPE), operation.parameterMap());

			resultReporter.report(200, GraphDBLdbcSnbInteractiveOperationResultSets.read5Results(results), operation);
		}
	}

	public static class LdbcQuery6Handler implements OperationHandler<LdbcQuery6, GraphDbConnectionState> {
		@Override
		public void executeOperation(LdbcQuery6 operation, GraphDbConnectionState dbConnectionState,
									 ResultReporter resultReporter) throws DbException {
			sleep(operation, sleepDurationAsNano);
			List<BindingSet> results = dbConnectionState.getGraphDbClient().execute(BENCHMARK_QUERIES.get(LdbcQuery6.TYPE), operation.parameterMap());

			resultReporter.report(200, GraphDBLdbcSnbInteractiveOperationResultSets.read6Results(results), operation);
		}
	}

	public static class LdbcQuery7Handler implements OperationHandler<LdbcQuery7, GraphDbConnectionState> {
		@Override
		public void executeOperation(LdbcQuery7 operation, GraphDbConnectionState dbConnectionState,
									 ResultReporter resultReporter) throws DbException {
			sleep(operation, sleepDurationAsNano);
			List<BindingSet> results = dbConnectionState.getGraphDbClient().execute(BENCHMARK_QUERIES.get(LdbcQuery7.TYPE), operation.parameterMap());

			resultReporter.report(200, GraphDBLdbcSnbInteractiveOperationResultSets.read7Results(results), operation);
		}
	}

	public static class LdbcQuery8Handler implements OperationHandler<LdbcQuery8, GraphDbConnectionState> {
		@Override
		public void executeOperation(LdbcQuery8 operation, GraphDbConnectionState dbConnectionState,
									 ResultReporter resultReporter) throws DbException {
			sleep(operation, sleepDurationAsNano);
			List<BindingSet> results = dbConnectionState.getGraphDbClient().execute(BENCHMARK_QUERIES.get(LdbcQuery8.TYPE), operation.parameterMap());

			resultReporter.report(200, GraphDBLdbcSnbInteractiveOperationResultSets.read8Results(results), operation);
		}
	}

	public static class LdbcQuery9Handler implements OperationHandler<LdbcQuery9, GraphDbConnectionState> {
		@Override
		public void executeOperation(LdbcQuery9 operation, GraphDbConnectionState dbConnectionState,
									 ResultReporter resultReporter) throws DbException {
			sleep(operation, sleepDurationAsNano);
			List<BindingSet> results = dbConnectionState.getGraphDbClient().execute(BENCHMARK_QUERIES.get(LdbcQuery9.TYPE), operation.parameterMap());

			resultReporter.report(200, GraphDBLdbcSnbInteractiveOperationResultSets.read9Results(results), operation);
		}
	}

	public static class LdbcQuery10Handler implements OperationHandler<LdbcQuery10, GraphDbConnectionState> {
		@Override
		public void executeOperation(LdbcQuery10 operation, GraphDbConnectionState dbConnectionState,
									 ResultReporter resultReporter) throws DbException {
			sleep(operation, sleepDurationAsNano);
			List<BindingSet> results = dbConnectionState.getGraphDbClient().execute(BENCHMARK_QUERIES.get(LdbcQuery10.TYPE), operation.parameterMap());

			resultReporter.report(200, GraphDBLdbcSnbInteractiveOperationResultSets.read10Results(results), operation);
		}
	}

	public static class LdbcQuery11Handler implements OperationHandler<LdbcQuery11, GraphDbConnectionState> {
		@Override
		public void executeOperation(LdbcQuery11 operation, GraphDbConnectionState dbConnectionState,
									 ResultReporter resultReporter) throws DbException {
			sleep(operation, sleepDurationAsNano);
			List<BindingSet> results = dbConnectionState.getGraphDbClient().execute(BENCHMARK_QUERIES.get(LdbcQuery11.TYPE), operation.parameterMap());

			resultReporter.report(200, GraphDBLdbcSnbInteractiveOperationResultSets.read11Results(results), operation);
		}
	}

	public static class LdbcQuery12Handler implements OperationHandler<LdbcQuery12, GraphDbConnectionState> {
		@Override
		public void executeOperation(LdbcQuery12 operation, GraphDbConnectionState dbConnectionState,
									 ResultReporter resultReporter) throws DbException {
			sleep(operation, sleepDurationAsNano);
			List<BindingSet> results = dbConnectionState.getGraphDbClient().execute(BENCHMARK_QUERIES.get(LdbcQuery12.TYPE), operation.parameterMap());

			resultReporter.report(200, GraphDBLdbcSnbInteractiveOperationResultSets.read12Results(results), operation);
		}
	}

	public static class LdbcQuery13Handler implements OperationHandler<LdbcQuery13, GraphDbConnectionState> {
		@Override
		public void executeOperation(LdbcQuery13 operation, GraphDbConnectionState dbConnectionState,
									 ResultReporter resultReporter) throws DbException {
			sleep(operation, sleepDurationAsNano);
			List<BindingSet> results = dbConnectionState.getGraphDbClient().execute(BENCHMARK_QUERIES.get(LdbcQuery13.TYPE), operation.parameterMap());

			resultReporter.report(200, GraphDBLdbcSnbInteractiveOperationResultSets.read13Results(results), operation);
		}
	}

	public static class LdbcQuery14Handler implements OperationHandler<LdbcQuery14, GraphDbConnectionState> {
		@Override
		public void executeOperation(LdbcQuery14 operation, GraphDbConnectionState dbConnectionState,
									 ResultReporter resultReporter) throws DbException {
			sleep(operation, sleepDurationAsNano);
			List<BindingSet> results = dbConnectionState.getGraphDbClient().execute(BENCHMARK_QUERIES.get(LdbcQuery14.TYPE), operation.parameterMap());

			resultReporter.report(200, GraphDBLdbcSnbInteractiveOperationResultSets.read14Results(results), operation);
		}
	}
}
