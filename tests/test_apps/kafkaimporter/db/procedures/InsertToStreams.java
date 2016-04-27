package db.procedures;

import java.util.Date;
import java.util.Random;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

public class InsertToStreams extends VoltProcedure {

    public final SQLStmt INSERT_TO_HBASE_STREAM = new SQLStmt(
            "INSERT INTO EVENTS_TO_HBASE(EVENT_INSTANCE_ID,"
                    + "EVENT_TYPE_ID, EVENT_DATE, PARTNER_ID, CONSUMER_ID,"
                    + "ADDRESS, SUB_DOMAIN_1, SUB_DOMAIN_2, SUB_DOMAIN_3,"
                    + "EVENT_QUANTITY, EVENT_VALUE, TRANSACTION_CODE, IN_MESSAGE_ID,"
                    + "OUT_MESSAGE_ID, EVENT_TRACKING_ID, TRACKING_RULE_ID) "
                    + "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);");

    public final SQLStmt INSERT_TO_HDFS_STREAM = new SQLStmt(
            "INSERT INTO EVENTS_TO_HDFS(EVENT_INSTANCE_ID,"
                    + "EVENT_TYPE_ID, EVENT_DATE, PARTNER_ID, CONSUMER_ID,"
                    + "ADDRESS, SUB_DOMAIN_1, SUB_DOMAIN_2, SUB_DOMAIN_3,"
                    + "EVENT_QUANTITY, EVENT_VALUE, TRANSACTION_CODE, IN_MESSAGE_ID,"
                    + "OUT_MESSAGE_ID, EVENT_TRACKING_ID, TRACKING_RULE_ID) "
                    + "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);");

    public VoltTable[] run(final int eventInstanceId) throws VoltAbortException {
        
        Random rand = new Random();
        
        SampleRecord record = new SampleRecord(eventInstanceId, rand);

        voltQueueSQL(INSERT_TO_HBASE_STREAM, eventInstanceId, record.event_type_id,
                record.event_date, record.partner_id, record.consumer_id, record.address, record.sub_domain_1,
                record.sub_domain_2, record.sub_domain_3, record.event_quantity, record.event_value,
                record.transaction_code, record.in_message_id, record.out_message_id, record.event_tracking_id,
                record.tracking_rule_id);

        voltQueueSQL(INSERT_TO_HDFS_STREAM, eventInstanceId, record.event_type_id,
                record.event_date, record.partner_id, record.consumer_id, record.address, record.sub_domain_1,
                record.sub_domain_2, record.sub_domain_3, record.event_quantity, record.event_value,
                record.transaction_code, record.in_message_id, record.out_message_id, record.event_tracking_id,
                record.tracking_rule_id);
        return voltExecuteSQL(true);
    }
}
