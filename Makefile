format:
	sqlfmt models macros

coverage:
	dbt-coverage compute doc --cov-fail-under 0.9
