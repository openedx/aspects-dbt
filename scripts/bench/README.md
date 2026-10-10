# Engagement benchmark and known-answer check

Tools for checking the engagement models (pages, problems, videos) for correctness against known
answers, and for measuring the queries the Superset dashboards run. Neither runs in CI: they need a
dataset in the millions of events to mean anything. CI covers the same ground with unit tests,
`scripts/ci/check_course_pruning.py` and the incremental checks in `.github/workflows/coverage.yml`.

## 1. Generate a dataset with known answers

[xapi-db-load](https://github.com/openedx/xapi-db-load)'s `journeys` command simulates learners
working through properly nested courses and writes the expected engagement results with the data:

```
xapi-db-load journeys --config_file example_configs/journeys_oracle.yaml --output_dir oracle   # ~9K events, every edge case
xapi-db-load journeys --config_file example_configs/journeys_m.yaml --output_dir m             # ~15M events, 25k learners
```

## 2. Load it

Use a ClickHouse you can wipe, at the version and size you care about (we used 25.8 limited to
4 CPU / 16 GB), with the Aspects migrations and `dbt run` applied to empty source tables. Copy the
dataset directory under the server's `user_files` path, then (database names as in a default
install; Aspects' xAPI database may be named differently):

```sql
insert into event_sink.course_overviews select * from file('m/courses.csv.gz', 'CSV',
    'org String, course_key String, display_name String, course_start String, course_end String,
     enrollment_start String, enrollment_end String, self_paced Bool, course_data_json String,
     created String, modified String, dump_id UUID, time_last_dumped String');
insert into event_sink.course_blocks select * from file('m/blocks.csv.gz', 'CSV',
    'org String, course_key String, location String, display_name String, xblock_data_json String,
     order Int32, edited_on String, dump_id UUID, time_last_dumped String');
insert into event_sink.external_id select * from file('m/external_ids.csv.gz', 'CSV',
    'external_user_id UUID, external_id_type String, username String, user_id Int32, dump_id UUID,
     time_last_dumped String');
insert into event_sink.user_profile select * from file('m/user_profiles.csv.gz', 'CSV');
insert into xapi.xapi_events_all select * from file('m/xapi.csv.gz', 'CSV',
    'event_id UUID, emission_time DateTime64(6), event String');

create database if not exists bench;
create table bench.expected_engagement engine = MergeTree order by tuple() as
    select * from file('m/expected_engagement.csv.gz', 'CSVWithNames');
create table bench.expected_video_seconds engine = MergeTree order by tuple() as
    select * from file('m/expected_video_seconds.csv.gz', 'CSVWithNames');
system reload dictionaries;
```

To imitate live traffic, where a learner's events arrive in many small inserts, add
`settings max_block_size = 20000, min_insert_block_size_rows = 20000,
min_insert_block_size_bytes = 0, max_insert_threads = 1, max_threads = 1` to the xAPI insert.

Then bring the refreshable models up to date: `dbt run -s fact_video_watch_intervals+` (its
post_hook back-fills history) and `system refresh view reporting.dim_course_blocks_mv`.

## 3. Check the numbers

```
clickhouse-client --multiquery < scripts/bench/known_answers.sql
```

The last table it prints is what the charts show: per metric and level, the number of bars whose
"at least one" or "all" count differs from the known answer. All zeros is a pass. The other tables
explain differences row by row. Rows the models don't produce for "No ... yet" show up as
"missing", which is expected: the charts never show that status.

## 4. Benchmark the dashboard queries

```
uv run --with jinja2 --with pyyaml scripts/bench/bench.py --aspects ../tutor-contrib-aspects \
    --course-key <course> --learner <username> --label after --runs 3 --rls staff --burst
python scripts/bench/compare_runs.py results/before.json results/after.json
```

`manifest.json` in the dataset lists a busy learner for every course. `--rls admin` leaves off the
course filter Superset adds for course staff.
