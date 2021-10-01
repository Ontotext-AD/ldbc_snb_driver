package com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive.queries.longreads;

public class LdbcQuery13Result {
    private final int shortestPathLength;

    public LdbcQuery13Result(int shortestPathLength) {
        this.shortestPathLength = shortestPathLength;
    }

    public int shortestPathLength() {
        return shortestPathLength;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcQuery13Result that = (LdbcQuery13Result) o;

        return shortestPathLength == that.shortestPathLength;
    }

    @Override
    public int hashCode() {
        return shortestPathLength ^ (shortestPathLength >>> 31);
    }

    @Override
    public String toString() {
        return "LdbcQuery13Result{" +
                "shortestPathLength=" + shortestPathLength +
                '}';
    }
}
