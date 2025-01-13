Aspects dbt
===========

Purpose
*******

This repository hosts the dbt project for Open edX `Aspects`_. It contains sql files
and configuration that control the `ClickHouse`_ reporting schema, data
transformations, tests, and documentation that drive Aspects reports. This project
is still in its infancy and will be growing with documentation and examples as we
approach the Aspects v1 launch.

A dbt docs site with the details of this project, built off the main branch is located here: https://openedx.github.io/aspects-dbt/

.. _ClickHouse: https://clickhouse.com
.. _Aspects: https://docs.openedx.org/projects/openedx-aspects/en/latest/index.html


Getting Help
************

Documentation
=============
Model naming pattern:

* ``dim_`` - Table/view/dict of attributes pertaining to a noun (actor, course, etc)
* ``fact_`` - Table/view/dict of events or qualitative data

Installation
************

1. ``pip install -r requirements.txt`` will install dbt and other required packages.
2. ``dbt deps`` will install the dbt packages defined in `packages.yml <packages.yml>`_ (this step needs to be run when initializing your project or when changing the package list).

Running dbt
***********

``dbt run`` will compile and create the models defined in the "aspects" dbt project. By default, dbt will look in the ``xapi`` schema to find source tables. The ``XAPI_SCHEMA`` environment variable can be used to specify a different schema.

Testing
*******
As of `dbt v1.8 <https://docs.getdbt.com/reference/resource-properties/unit-tests>`, models can now be tested with UNIT tests in addition to the existing DATA tests. Unit tests validate the SQL model logic by building the models using a (known to be good) dataset and comparing the results to a provided 'expected' dataset. This is especially beneficial when updating a model to ensure the output has not changed.

The ``unit_tests.yaml`` file in each model directory contains any tests for the models in that directory.
The ``unit-test-seeds`` directory contains all seed data csv files. There is one file for each base table (event_sink & xapi) and each 'expected' dataset.

``dbt test`` will only run data & generic tests (NOT unit tests). This is the default mode.

``dbt test --selector unit_tests`` will run all unit tests.
These require tables to be seeded first. To do this, add 'unit-test-seeds' to ``seed-paths:`` in ``dbt_project.yml`` and run ``dbt seed --full-refresh && dbt run --full-refresh``.

``dbt test --selector all_tests`` will run all data/generic/unit tests.


Removing old models
*******************

dbt does not automatically remove models that have been deleted from this project. As we remove models we will add them to a macro that can be manually run to clean up things which are no longer needed. This can be important to prevent stale materialized views from breaking when schemas change, and to prevent unnecessary inserts writes to tables that aren't used.

If you need a model that has been removed due to custom reporting you should either move that model to the system you use to manage your custom schema (such as your own dbt package) instead of letting the old version remain. This will let you explicitly upgrade it as necessary.

``dbt run-operation remove_deprecated_models`` will drop the relations and ``dbt -d run-operation remove_deprecated_models`` will drop with debug information showing the commands that are run.

More Help
=========

If you're having trouble, we have discussion forums at
https://discuss.openedx.org where you can connect with others in the
community.

Our real-time conversations are on Slack. You can request a `Slack
invitation`_, then join our `community Slack workspace`_.

For anything non-trivial, the best path is to open an issue in this
repository with as many details about the issue you are facing as you
can provide.

https://github.com/openedx/aspects-dbt/issues

For more information about these options, see the `Getting Help`_ page.

.. _Slack invitation: https://openedx.org/slack
.. _community Slack workspace: https://openedx.slack.com/
.. _Getting Help: https://openedx.org/getting-help

License
*******

Please see `LICENSE <LICENSE>`_ for details.

Contributing
************

Contributions are very welcome.
Please read `How To Contribute <https://openedx.org/r/how-to-contribute>`_ for details.

This project is currently accepting all types of contributions, bug fixes,
security fixes, maintenance work, or new features.  However, please make sure
to have a discussion about your new feature idea with the maintainers prior to
beginning development to maximize the chances of your change being accepted.
You can start a conversation by creating a new issue on this repo summarizing
your idea.

Make sure to format the SQL models via `make format` before submitting a pull request.

The Open edX Code of Conduct
****************************

All community members are expected to follow the `Open edX Code of Conduct`_.

.. _Open edX Code of Conduct: https://openedx.org/code-of-conduct/

People
******

The assigned maintainers for this component and other project details may be
found in `Backstage`_. Backstage pulls this data from the ``catalog-info.yaml``
file in this repo.

.. _Backstage: https://open-edx-backstage.herokuapp.com/catalog/default/component/openedx-event-sink-clickhouse

Reporting Security Issues
*************************

Please do not report security issues in public. Please email security@openedx.org.
