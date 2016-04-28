/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */package db.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class MoveAggregatedDataToStream extends VoltProcedure {

    public final SQLStmt INSERT_SUMMARY_DATA_TO_STREAM = new SQLStmt(
        "INSERT INTO EVENTS_SUMMARY_TO_JDBC" +
            "(EVENT_INSTANCE_ID,TOATL_COUNT ,ACK_COUNT ,NACK_COUNT" +
            ",FAILED_COUNT ,DELIVERED_COUNT ,RESPONSE_COUNT" +
            ",CONVERSION_COUNT ,VALUE ,RESPONSE_COUNT_UNIQUE" +
            ",CONVERSION_COUNT_UNIQUE ,CNTL_GROUP_PART_COUNT" +
            ",CNTL_GROUP_CON_COUNT ,CNTL_GROUP_UNIQUE_CON_COUNT" +
            ",CNTL_GROUP_CON_VALUE ,RESPONSE_VALUE" +
            ",UNFORESEEN_RESPONSE_COUNT ,UNFORESEEN_CONVERSION_COUNT" +
            ",UNFORESEEN_RESPONSE_VALUE ,UNFORESEEN_CONVERSION_VALUE" +
            ",UNFORESEEN_RESP_CNT_UNIQUE ,UNFORESEEN_CONV_CNT_UNIQUE" +
            ",FULLFILLMENT_COUNT ,FULFILLMENT_FAIL_COUNT" +
            ",FULFILLMENT_FAIL_COST) " +
         "SELECT EVENT_INSTANCE_ID,TOATL_COUNT" +
            ",ACK_COUNT ,NACK_COUNT ,FAILED_COUNT ,DELIVERED_COUNT" +
            ",RESPONSE_COUNT ,CONVERSION_COUNT ,VALUE" +
            ",RESPONSE_COUNT_UNIQUE ,CONVERSION_COUNT_UNIQUE" +
            ",CNTL_GROUP_PART_COUNT ,CNTL_GROUP_CON_COUNT" +
            ",CNTL_GROUP_UNIQUE_CON_COUNT ,CNTL_GROUP_CON_VALUE" +
            ",RESPONSE_VALUE ,UNFORESEEN_RESPONSE_COUNT" +
            ",UNFORESEEN_CONVERSION_COUNT ,UNFORESEEN_RESPONSE_VALUE" +
            ",UNFORESEEN_CONVERSION_VALUE ,UNFORESEEN_RESP_CNT_UNIQUE" +
            ",UNFORESEEN_CONV_CNT_UNIQUE ,FULLFILLMENT_COUNT" +
            ",FULFILLMENT_FAIL_COUNT ,FULFILLMENT_FAIL_COST FROM " +
            "EVENTS_SUMMARY_VW;");

    public final SQLStmt TRUNCATE_VIEW = new SQLStmt("DELETE FROM EVENTS_SUMMARY_VW;");

    public VoltTable[] run() throws VoltAbortException {
        voltQueueSQL(INSERT_SUMMARY_DATA_TO_STREAM);
        voltQueueSQL(TRUNCATE_VIEW);
        return voltExecuteSQL(true);
    }
}
