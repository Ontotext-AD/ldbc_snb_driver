package com.ldbc.driver.workloads.ontotext.ldbc.snb.interactive.queries.longreads;

import java.util.Objects;

public class LdbcQuery5Result {
    private final String forumTitle;
    private final int postCount;

    public LdbcQuery5Result(String forumTitle, int postCount) {
        this.forumTitle = forumTitle;
        this.postCount = postCount;
    }

    public String forumTitle() {
        return forumTitle;
    }

    public int postCount() {
        return postCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcQuery5Result result = (LdbcQuery5Result) o;

        if (postCount != result.postCount) return false;
        return Objects.equals(forumTitle, result.forumTitle);
    }

    @Override
    public String toString() {
        return "LdbcQuery5Result{" +
                "forumTitle='" + forumTitle + '\'' +
                ", postCount=" + postCount +
                '}';
    }
}
