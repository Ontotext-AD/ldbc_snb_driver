package com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive;

import java.util.Objects;

public class LdbcQuery4Result {
    private final String tagName;
    private final int postCount;

    public LdbcQuery4Result(String tagName, int postCount) {
        this.tagName = tagName;
        this.postCount = postCount;
    }

    public String tagName() {
        return tagName;
    }

    public int postCount() {
        return postCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcQuery4Result that = (LdbcQuery4Result) o;

        if (postCount != that.postCount) return false;
        return Objects.equals(tagName, that.tagName);
    }

    @Override
    public String toString() {
        return "LdbcQuery4Result{" +
                "tagName='" + tagName + '\'' +
                ", postCount=" + postCount +
                '}';
    }
}