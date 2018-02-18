mongo-export-index-clean
========================

Remove records from a MongoDB bson export ( `www.mongodb.com`_ ) that violates index size restrictions.

A change in MongoDB version 2.6 means that older exports will fail to import properly because older releases simply
ignored records with fields that exceeded the index size restrictions.

This utility takes an export and removes the records that violates the size restriction (or duplicate unique index).


.. _www.mongodb.com: https://www.mongodb.com/

Getting started
---------------

.. code:: bash

    pip install mongo-export-index-clean

    cd testdata

    mongo-export-index-clean myfile.bson

The utility will remove records  a .bad

Source code and feedback
------------------------

Fully open sourced with `Apache License`_ on `github.com/sverrirab/mongo-export-index-clean`_ including issue tracking.

.. _Apache License: https://github.com/sverrirab/mongo-export-index-clean/blob/master/LICENSE.rst
.. _github.com/sverrirab/mongo-export-index-clean: https://github.com/sverrirab/mongo-export-index-clean
