package com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive;

import com.google.common.collect.Lists;
import com.ldbc.driver.util.ListUtils;
import org.eclipse.rdf4j.model.IRI;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class LdbcQuery12Result {
	private final IRI personId;
	private final String personFirstName;
	private final String personLastName;
	private final String tagNames;
	private final int replyCount;

	public LdbcQuery12Result(IRI personId, String personFirstName, String personLastName, String tagNames, int replyCount) {
		this.personId = personId;
		this.personFirstName = personFirstName;
		this.personLastName = personLastName;
		this.tagNames = tagNames;
		this.replyCount = replyCount;
	}

	public IRI personId() {
		return personId;
	}

	public String personFirstName() {
		return personFirstName;
	}

	public String personLastName() {
		return personLastName;
	}

	public String tagNames() {
		return tagNames;
	}

	public int replyCount() {
		return replyCount;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		LdbcQuery12Result that = (LdbcQuery12Result) o;

		if (!Objects.equals(personId, that.personId)) return false;
		if (replyCount != that.replyCount) return false;
		if (!Objects.equals(personFirstName, that.personFirstName))
			return false;
		if (!Objects.equals(personLastName, that.personLastName))
			return false;
		return Objects.equals(tagNames, that.tagNames);
	}

	@Override
	public String toString() {
		return "LdbcQuery12Result{" +
				"personId=" + personId +
				", personFirstName='" + personFirstName + '\'' +
				", personLastName='" + personLastName + '\'' +
				", tagNames=" + tagNames +
				", replyCount=" + replyCount +
				'}';
	}
}
