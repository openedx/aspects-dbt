format:
	sqlfmt models macros

coverage:
	dbt-coverage compute doc --cov-fail-under 1.0
