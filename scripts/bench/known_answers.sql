-- Compare the engagement models with the known answers of a learner-journey dataset.
--
-- Expects the dataset's expected_engagement and expected_video_seconds files loaded into
-- bench.expected_engagement and bench.expected_video_seconds (see README.md), the models in
-- reporting and event_sink, and writes its working tables to bench.cmp_*.
-- Run with: clickhouse-client --multiquery < scripts/bench/known_answers.sql
--
-- Rows are matched on (metric, course_key, actor_id, content_level, section/subsection display
-- name). The name is what the charts show, and fact_pageview_engagement has no block_id.

create or replace table bench.block_names engine = Memory as
select location, display_name_with_location as name
from event_sink.dim_most_recent_course_blocks;

create or replace table bench.cmp_expected engine = MergeTree order by tuple() as
select e.metric, e.course_key, e.actor_id, e.content_level, n.name,
       e.status, e.status_truth, e.done, e.total
from bench.expected_engagement e
left join bench.block_names n on n.location = e.block_id;

create or replace table bench.cmp_actual engine = MergeTree order by tuple() as
select metric, course_key, actor_id, content_level, name,
       groupUniqArray(status) as statuses, count() as n_rows,
       countIf(username = '') as n_no_username
from (
    select 'pages' as metric, course_key, actor_id, content_level,
           section_subsection_name as name, section_subsection_page_engagement as status, username
    from reporting.fact_pageview_engagement
    union all
    select 'problems', course_key, actor_id, content_level, section_subsection_name,
           section_subsection_problem_engagement, username
    from reporting.fact_problem_engagement
    union all
    select 'videos', course_key, actor_id, content_level, section_subsection_name,
           section_subsection_video_engagement, username
    from reporting.fact_video_engagement
)
group by metric, course_key, actor_id, content_level, name
settings max_bytes_before_external_group_by = 4000000000;

create or replace table bench.cmp_joined engine = MergeTree order by tuple() as
select
    coalesce(e.metric, a.metric) as metric,
    coalesce(e.content_level, a.content_level) as content_level,
    coalesce(e.course_key, a.course_key) as course_key,
    coalesce(e.actor_id, a.actor_id) as actor_id,
    coalesce(e.name, a.name) as name,
    e.status as expected, e.status_truth as expected_truth, e.done as done, e.total as total,
    a.statuses as actual, a.n_rows as n_rows, a.n_no_username as n_no_username,
    multiIf(
        e.metric is null, if(a.actor_id = '', 'extra: blank actor', 'extra'),
        a.metric is null, 'missing',
        a.n_rows > 1, 'several rows',
        a.statuses[1] = e.status, 'match',
        'wrong status'
    ) as outcome
from bench.cmp_expected e
full outer join bench.cmp_actual a
    on e.metric = a.metric and e.course_key = a.course_key and e.actor_id = a.actor_id
    and e.content_level = a.content_level and e.name = a.name
settings join_use_nulls = 1, join_algorithm = 'grace_hash', grace_hash_join_initial_buckets = 8;

-- 1. Outcome per metric and level
select metric, content_level, outcome, count() as rows,
       round(100 * rows / sum(rows) over (partition by metric, content_level), 1) as pct
from bench.cmp_joined
group by metric, content_level, outcome
order by metric, content_level, rows desc
format PrettyCompactMonoBlock;

-- 2. Expected vs actual status where both exist
select metric, content_level, expected, arrayStringConcat(actual, ' | ') as actual, count() as rows
from bench.cmp_joined
where outcome in ('wrong status', 'several rows')
group by all
order by metric, content_level, rows desc
limit 40
format PrettyCompactMonoBlock;

-- 3. Missing rows by expected status
select metric, content_level, expected, count() as rows
from bench.cmp_joined where outcome = 'missing'
group by all order by metric, content_level, rows desc
format PrettyCompactMonoBlock;

-- 4. Videos: does the model follow observable or true playback?
select content_level,
       countIf(actual[1] = expected) as matches_observable,
       countIf(actual[1] = expected_truth) as matches_truth,
       countIf(expected != expected_truth) as observable_differs_from_truth
from bench.cmp_joined
where metric = 'videos' and outcome in ('match', 'wrong status')
group by content_level
format PrettyCompactMonoBlock;

-- 5. PII: rows for a real learner without a username, by actor id kind
select metric, startsWith(actor_id, 'mailto:') as mbox_actor,
       sum(n_rows) as rows, sum(n_no_username) as no_username
from bench.cmp_joined
where actual is not null and actor_id != ''
group by all order by metric, mbox_actor
format PrettyCompactMonoBlock;

-- 6. Watched video seconds per learner and video vs expected
create or replace table bench.cmp_video_seconds engine = MergeTree order by tuple() as
select
    coalesce(e.course_key, a.course_key) as course_key,
    coalesce(e.actor_id, a.actor_id) as actor_id,
    coalesce(e.video_block_id, a.video) as video,
    ifNull(e.observable_seconds, 0) as exp_seconds,
    ifNull(e.observable_distinct_seconds, 0) as exp_distinct,
    ifNull(e.truth_seconds, 0) as truth_seconds,
    ifNull(a.seconds, 0) as act_seconds,
    ifNull(a.distinct_seconds, 0) as act_distinct
from bench.expected_video_seconds e
full outer join (
    select course_key, actor_id, splitByString('/xblock/', object_id)[-1] as video,
           sum(watch_count) as seconds, uniqExact(watched_segment) as distinct_seconds
    from reporting.fact_video_segments
    group by all
) a on e.course_key = a.course_key and e.actor_id = a.actor_id and e.video_block_id = a.video
settings join_use_nulls = 1;

select
    count() as learner_videos,
    countIf(act_seconds = exp_seconds) as seconds_exact,
    countIf(act_distinct = exp_distinct) as distinct_exact,
    sum(exp_seconds) as expected_seconds,
    sum(act_seconds) as actual_seconds,
    round(100 * (actual_seconds - expected_seconds) / expected_seconds, 1) as pct_diff,
    sum(truth_seconds) as truth_seconds_total
from bench.cmp_video_seconds
format PrettyCompactMonoBlock;

-- 7. Chart numbers per section / subsection bar, using the Superset dataset metrics:
--    pages and problems count rows (countIf), videos count distinct learners.
create or replace table bench.cmp_chart_act engine = MergeTree order by tuple() as
select metric, content_level, course_key, name,
       if(metric = 'videos', uniqExactIf(actor_id, status like 'At least%' or status like 'All%'),
          countIf(status like 'At least%' or status like 'All%')) as act_at_least,
       if(metric = 'videos', uniqExactIf(actor_id, status like 'All%'),
          countIf(status like 'All%')) as act_all
from (
    select 'pages' as metric, content_level, course_key, section_subsection_name as name,
           actor_id, section_subsection_page_engagement as status
    from reporting.fact_pageview_engagement
    union all
    select 'problems', content_level, course_key, section_subsection_name, actor_id,
           section_subsection_problem_engagement from reporting.fact_problem_engagement
    union all
    select 'videos', content_level, course_key, section_subsection_name, actor_id,
           section_subsection_video_engagement from reporting.fact_video_engagement
)
group by all;

create or replace table bench.cmp_chart engine = MergeTree order by tuple() as
with exp as (
    select metric, content_level, course_key, name,
           countIf(done > 0) as exp_at_least, countIf(done = total) as exp_all
    from bench.cmp_expected group by all
)
select coalesce(exp.metric, act.metric) as metric,
       coalesce(exp.content_level, act.content_level) as content_level,
       coalesce(exp.course_key, act.course_key) as course_key, coalesce(exp.name, act.name) as name,
       ifNull(exp_at_least, 0) as exp_at_least, ifNull(act_at_least, 0) as act_at_least,
       ifNull(exp_all, 0) as exp_all, ifNull(act_all, 0) as act_all
from exp full outer join bench.cmp_chart_act act
    on exp.metric = act.metric and exp.content_level = act.content_level
    and exp.course_key = act.course_key and exp.name = act.name
settings join_use_nulls = 1;

select metric, content_level, count() as bars,
       countIf(act_at_least != exp_at_least) as bars_wrong_at_least,
       countIf(act_all != exp_all) as bars_wrong_all,
       sum(exp_at_least) as exp_at_least_total, sum(act_at_least) as act_at_least_total,
       sum(exp_all) as exp_all_total, sum(act_all) as act_all_total
from bench.cmp_chart group by all order by all
format PrettyCompactMonoBlock;
