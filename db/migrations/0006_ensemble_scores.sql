-- db/migrations/0006_ensemble_scores.sql
CREATE TABLE IF NOT EXISTS derived.ensemble_member_scores (
    as_of       date    NOT NULL,
    member_key  text    NOT NULL,
    sharpe_raw  double precision NOT NULL,   -- annualized, no penalty
    sharpe      double precision NOT NULL,   -- deflated, clipped ≥0
    n_obs       integer NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (as_of, member_key)
);
CREATE INDEX IF NOT EXISTS idx_ensemble_scores_as_of
    ON derived.ensemble_member_scores (as_of DESC);