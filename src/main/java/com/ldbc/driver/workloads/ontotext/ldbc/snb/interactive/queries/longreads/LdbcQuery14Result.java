package com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive.queries.longreads;

import com.google.common.collect.Lists;
import com.ldbc.driver.validation.ValidationEquality;
import org.eclipse.rdf4j.model.IRI;

import java.util.Iterator;
import java.util.Objects;

public class LdbcQuery14Result {
	private final Iterable<? extends IRI> personIdsInPath;
	private final double pathWeight;

	public LdbcQuery14Result(Iterable<? extends IRI> personIdsInPath, double pathWeight) {
		this.personIdsInPath = personIdsInPath;
		this.pathWeight = pathWeight;
	}

	public Iterable<? extends IRI> personsIdsInPath() {
		// force to List, as Guava/Jackson magic changes it to a strange collection that breaks equality somewhere
		// not performance sensitive code path, only used for validation & serialization - not during runs
		return Lists.newArrayList(personIdsInPath);
	}

	public double pathWeight() {
		return pathWeight;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		LdbcQuery14Result that = (LdbcQuery14Result) o;
		if (!ValidationEquality.doubleEquals(that.pathWeight, pathWeight)) {
			return false;
		}
		if (null == personIdsInPath || null == that.personIdsInPath) {
			return false;
		}
		return personIdPathsEqual(personIdsInPath, that.personIdsInPath);
	}

	private boolean personIdPathsEqual(Iterable<? extends IRI> path1, Iterable<? extends IRI> path2) {
		Iterator<? extends IRI> path1Iterator = path1.iterator();
		Iterator<? extends IRI> path2Iterator = path2.iterator();
		while (path1Iterator.hasNext()) {
			if (!path2Iterator.hasNext()) {
				return false;
			}
			IRI path1IdIRI = path1Iterator.next();
			IRI path2IdIRI = path2Iterator.next();
			if (null == path1IdIRI || null == path2IdIRI) {
				return false;
			}
			return Objects.equals(path1IdIRI, path2IdIRI);
		}
		return !path2Iterator.hasNext();
	}

	@Override
	public String toString() {
		return "LdbcQuery14Result{" +
				"personIdsInPath=" + personIdsInPath +
				", pathWeight=" + pathWeight +
				'}';
	}
}