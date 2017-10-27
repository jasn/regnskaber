Regnskaber
==========

This is a Python3 module for downloading financial statements from the Danish
Business Authority (Erhvervsstyrelsen).  There are two primary features:
``Fetch`` and ``Transform``.

The ``fetch`` command is used for getting the 'raw' data from the Danish
Business authority.  The way this data is organized is not very useful for
'learning', to make it better there is a ``transform`` command that can be run
after fetching all the data, that creates tables where each row corresponds to
one financial statement.

Fetch
=====

To fetch the raw data from Danish Business Authority run

    ``python -m regnskaber fetch -p {number of processes}``

It might be beneficial for you to use more than one process for getting the
data since this is a very lengthy process.  Even with several cores it can take a
couple of days.

If you have not configured the database information yet, you will be asked for your credentials.

I recommend you redirct stderr to a file, so that you can later see if some financial statements are missing.

Transform
=========

``python -m regnskaber transform {table definition file}``
There are two pre-made table definition files shipped with the project.


Table definition file explained
-------------------------------
See [regnskabstal table defintion](regnskaber/resources/feature_table_regnskabstal.json) and
[regnskabstekst table definition](regnskaber/resources/feature_table_regnskabstekst.json)
for examples of table definition files.


Reconfigure
===========

If you want to change the database credentials or connection information you can run:

``python -m regnskaber reconfigure``

This will interactively ask for the needed information, and discard what was
previously there.  Note that you can interrupt this at any time before entering
the last detail, and nothing will have changed.

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
