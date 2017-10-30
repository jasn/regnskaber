Regnskaber
==========

This is a Python3 module for downloading financial statements from the Danish
Business Authority (Erhvervsstyrelsen).  There are two primary features:
``fetch`` and ``transform``.

The ``fetch`` command is used for getting the raw data from the Danish
Business authority.  The way this data is organized is not very useful for
'learning', to make it better there is a ``transform`` command that can be run
after fetching all the data, that creates tables where each row corresponds to
one financial statement.

Setup and installation
=======================

Dependencies
------------
The following libraries need to be already installed on your system:
``libxml2-dev``, ``libxmlsec1-dev``, ``libmysqlclient-dev``.
You also need to have either a postgres database or a mysql database installed on your system.

Setup
-----
The module can be installed using pip after installing the dependencies mentioned earlier.
Run the following command:

``pip install --process-dependency-links git+https://github.com/jasn/regnskaber``

This should install regnskaber as a module in your current python environment.


Fetch
=====

To fetch the raw data from Danish Business Authority run

``python -m regnskaber fetch -p {number of processes}``

It might be beneficial for you to use more than one process for getting the
data since this is a very lengthy process.  Even with several cores it can take a
couple of days.

If you have not configured the database information yet, you will be asked for your credentials.

I recommend you redirct stderr to a file, so that you can later see if some financial statements are missing.

It is relatively straight forward to incorporate the fetch command into your own project.
The database needs to be configured first however, by e.g. running ``python -m regnskaber reconfigure``.
Make sure to call ``setup_database_connection()`` before running ``fetch.fetch_to_db(procceses)``.

``sample.py``
```python3
from contextlib import redirect_stderr
from regnskaber import setup_database_connection, fetch

processes = 16
setup_database_connection()
with StringIO() error_log:
    with redirect_stderr(error_log):
        fetch.fetch_to_db(processes)
        # now wait a couple of days.
```
Transform
=========

``python -m regnskaber transform {table definition file}``
There are two pre-made table definition files shipped with the project (see examples further down).


Table Definitions file explained
---------------------------------------

The table definitions file (TDF) is a [JSON](https://en.wikipedia.org/wiki/JSON) file.
The TDF is a list of dictionaries where each dictionary specifies a ``tablename`` (string) and ``columns``.
The ``columns`` entry is again a list of dictionaries, where each dictionary specifies a single column of the table.
An entry in the ``columns`` list (i.e. the dictionary) has the following keys: ``name``, ``sqltype``, ``regnskabs_fieldname``, ``dimensions``, ``method``.

* ``name`` is a string with the name of the column in the resulting table (remember your sql database may only support up to a certain length for column names, e.g. 64 characters).
* ``sqltype`` is the type of the column in the resulting table.
* ``regnskabs_fieldname`` is the name of the field in the financial statement this column is computed from.
* ``dimensions`` is either ``null`` or a list specifying which dimensions must be present on the ``regnskabs_fieldname``.
    e.g. we might be interested in ``fsa:ProfitLoss``, but only the ones that concern ``fsa:ResultDistributionDimension`` and ``fsa:ProposedDividendRecognisedInLiabilitiesMember``.
* ``method`` describes how to compute the resulting column from the specified input, and is again a dictionary.
  * ``name`` is the name of the function to call. These can be ``generic_number``, ``generic_text``, or ``generic_date``.
  * ``when_multiple``: what to do in case multiple entries in the financial statement match.
       For ``generic_number`` and ``generic_date`` this parameter is ignored.
       For ``generic_text`` it can be ``any``, ``none``, or ``concatenate``.
       In case multiple entries match and no strategy has been specified an error will be raised.
       Since ``generic_number`` and ``generic_date`` ignore ``when_multiple`` they implement simple heuristics.
       ``generic_number`` tries to find the 'most precise' and
       ``generic_date`` finds the first entry that will match a date format.

If the premade methods are not sufficient, there is a hook for adding more: ``register_method(name, func)`` in [make_feature_table.py](regnskaber/make_feature_table.py).
To extend the framework, just import regnskaber and make sure to call ``setup_database_connection()`` before calling ``main`` in [make_feature_table.py](regnskaber/make_feature_table.py).

Here is a small example

``run.py``:
```python
from regnskaber import (interactive_ensure_config_exists,
                        setup_database_connection,
                        make_feature_table as transform)

from random import random

def bar_method(regnskab_dict, fieldName, when_multiple=None, dimensions=None):
    return random()

interactive_ensure_config_exists()
setup_database_connection()
tdf_name = 'foo.json'
transform.register_method('bar', bar_method)
transform.main(tdf_name)
```
``foo.json``:
```json
[
  {
    "tablename": "test",
    "columns": [
      {
        "name": "baz",
        "sqltype": "Double",
        "regnskabs_fieldname": "fsa:ProfitLoss",
        "dimensions": null,
        "method": {
          "name": "bar"
        }
      }
    ]
  }
]
```

Now run
``python run.py``
Table definitions file examples
-------------------------------
See [regnskabstal table defintion](regnskaber/resources/feature_table_regnskabstal.json) and
[regnskabstekst table definition](regnskaber/resources/feature_table_regnskabstekst.json)
for examples of table definitions files.


Reconfigure
===========

If you want to change the database credentials or connection information you can run:

``python -m regnskaber reconfigure``

This will interactively ask for the needed information, and discard what was
previously there.  Note that you can interrupt this at any time before entering
the last detail, and nothing will have changed.

