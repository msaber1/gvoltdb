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

# This file contains configs having different schema from those in regression-config.py

{
# THESE ALL SUCCEED, USE THE TEMPLATE INPUT
    "basic-unions": {"schema": "union-schema.py",
                     "ddl": "DDL.sql",
                     "template": "basic-unions.sql",
                     "normalizer": "normalizer.py"},
    "mixed-unions": {"schema": "union-schema.py",
                     "ddl": "DDL.sql",
                     "template": "mixed-unions.sql",
                     "normalizer": "normalizer.py"},
# THESE ALL SUCCEED, USE THE TEMPLATE INPUT
    "basic-strings": {"schema": "strings-schema.py",
                      "ddl": "strings-DDL.sql",
                      "template": "basic-strings.sql",
                      "normalizer": "normalizer.py"},
# THESE ALL SUCCEED, USE TEMPLATE INPUT
    "partial-covering": {"schema": "partial-covering-schema.py",
                         "ddl": "partial-covering-DDL.sql",
                         "template": "partial-covering.sql",
                         "normalizer": "normalizer.py"},
# BIGINT OVERFLOW CAUSES FAILURES IN THIS SUITE, USE REGRESSION INPUT
    "regression-basic-ints": {"schema": "int-schema.py",
                              "ddl": "int-DDL.sql",
                              "template": "regression-basic-ints.sql",
                              "normalizer": "normalizer.py"},
# HSQL HAS BAD DEFAULT PRECISION
# AND VoltDB gives VOLTDB ERROR: Type DECIMAL can't be cast as FLOAT
# AND HSQLDB backend gives the likes of:
#   VOLTDB ERROR: UNEXPECTED FAILURE: org.voltdb.ExpectedProcedureException:
#   HSQLDB Backend DML Error (Scale of 56.11063569750000000000000000000000 is 32 and the max is 12)
# USE REGRESSION INPUT
    "regression-basic-decimal": {"schema": "decimal-schema.py",
                                 "ddl": "DDL.sql",
                                 "template": "regression-basic-decimal.sql",
                                 "normalizer": "normalizer.py"},
# If the ONLY problem was that HSQL HAS BAD DEFAULT PRECISION, we could use the original template input
# and FUZZY MATCHING, instead.
# Enable this test to investigate the "DECIMAL can't be cast as FLOAT" and/or "Backend DML Error" issues
# without being thrown off by HSQL HAS BAD DEFAULT PRECISION issues.
#    "basic-decimal-fuzzy": {"schema": "decimal-schema.py",
#                            "ddl": "DDL.sql",
#                            "template": "basic-decimal.sql",
#                            "normalizer": "fuzzynormalizer.py"},
#
# FLOATING POINT ROUNDING ISSUES BETWEEN VOLT AND HSQL, USE FUZZY MATCHING
    "basic-timestamp": {"schema": "timestamp-schema.py",
                        "ddl": "DDL.sql",
                        "template": "basic-timestamp.sql",
                        "normalizer": "normalizer.py"},
# BIGINT OVERFLOW CAUSES FAILURES IN THIS SUITE, USE REGRESSION INPUT
    "regression-basic-matview": {"schema": "matview-basic-schema.py",
                                 "ddl": "matview-DDL.sql",
                                 "template": "regression-basic-matview.sql",
                                 "normalizer": "normalizer.py"},

# THESE ALL SUCCEED, USE TEMPLATE INPUT
    "advanced-strings": {"schema": "strings-schema.py",
                         "ddl": "strings-DDL.sql",
                         "template": "advanced-strings.sql",
                         "normalizer": "normalizer.py"},
    "advanced-ints": {"schema": "int-schema.py",
                      "ddl": "int-DDL.sql",
                      "template": "advanced-ints.sql",
                      "normalizer": "normalizer.py"},
# BIGINT OVERFLOW CAUSES FAILURES IN THIS SUITE, USE REGRESSION INPUT
    "regression-advanced-ints-cntonly": {"schema": "int-schema.py",
                                         "ddl": "int-DDL.sql",
                                         "template": "regression-advanced-ints-cntonly.sql",
                                         "normalizer": "not-a-normalizer.py"},

# ADVANCED MATERIALIZED VIEW TESTING, INCLUDING COMPLEX GROUP BY AND AGGREGATIONS.
    "advanced-matview-nonjoin": {"schema": "matview-advanced-nonjoin-schema.py",
                                 "ddl": "matview-DDL.sql",
                                 "template": "advanced-matview-nonjoin.sql",
                                 "normalizer": "normalizer.py"},

    "advanced-matview-join": {"schema": "matview-advanced-join-schema.py",
                              "ddl": "matview-DDL.sql",
                              "template": "advanced-matview-join.sql",
                              "normalizer": "normalizer.py"},

# To test index count
    "index-count1": {"schema": "index-count1-schema.py",
                     "ddl": "DDL.sql",
                     "template": "index-count1.sql",
                     "normalizer": "normalizer.py"},

# To test index scan: forward scan, reverse scan
    "index-scan": {"schema": "index-scan-schema.py",
        "ddl": "index-DDL.sql",
        "template": "index-scan.sql",
        "normalizer": "normalizer.py"},

# This suite written to test push-down of aggregates and limits in combination
# with indexes, projections and order-by.
    "pushdown": {"schema": "pushdown-schema.py",
                 "ddl": "DDL.sql",
                 "template": "pushdown.sql",
                 "normalizer": "normalizer.py"},
# HSQL SEEMS TO HAVE A BAD DEFAULT PRECISION
# AND VoltDB gives VOLTDB ERROR: Type DECIMAL can't be cast as FLOAT, so keep it disabled, for now.
# If the only problem were HSQL HAS BAD DEFAULT PRECISION, we could USE FUZZY MATCHING.
# Enable this test to investigate the "DECIMAL can't be cast as FLOAT" issue
#    "advanced-decimal-fuzzy": {"schema": "decimal-schema.py",
#                               "ddl": "DDL.sql",
#                               "template": "advanced-decimal.sql",
#                               "normalizer": "fuzzynormalizer.py"},
}
