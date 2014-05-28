#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2014 VoltDB Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

{
    "basic-compoundex": {"schema": "schema.py",
                         "ddl": "compoundex-DDL.sql",
                         "template": "compound.sql",
                         "normalizer": "normalizer.py"},
     "basic-joins": {"schema": "schema.py",
                     "ddl": "DDL.sql",
                      "template": "basic-joins.sql",
                      "normalizer": "normalizer.py"},
     "basic-index-joins": {"schema": "schema.py",
                           "ddl": "index-DDL.sql",
                           "template": "basic-joins.sql",
                           "normalizer": "normalizer.py"},
     "basic-compoundex-joins": {"schema": "schema.py",
                                "ddl": "compoundex-DDL.sql",
                                "template": "basic-joins.sql",
                                "normalizer": "normalizer.py"},
    "advanced-joins": {"schema": "schema.py",
                       "ddl": "DDL.sql",
                       "template": "advanced-joins.sql",
                       "normalizer": "normalizer.py"},
    "advanced-index-joins": {"schema": "schema.py",
                             "ddl": "index-DDL.sql",
                             "template": "advanced-joins.sql",
                             "normalizer": "normalizer.py"},
    "advanced-subq-joins": {"schema": "schema.py",
                            "ddl": "DDL.sql",
                            "template": "advanced-subq-joins.sql",
                            "normalizer": "normalizer.py"},
    "advanced-compoundex-joins": {"schema": "schema.py",
                                  "ddl": "compoundex-DDL.sql",
                                  "template": "advanced-joins.sql",
                                  "normalizer": "normalizer.py"},
}
