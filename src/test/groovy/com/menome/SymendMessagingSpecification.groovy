package com.menome

import com.menome.util.messageBuilder.Connection
import com.menome.util.messageBuilder.MessageBuilder

import java.time.Instant

class SymendMessagingSpecification extends MessagingSpecification {
    List<Connection> accounts
    List<Connection> activities
    List<Connection> dialers
    int ACCOUNT_COUNT = 5000
    int ACTIVITY_COUNT = 10
    int DIALER_COUNT = 5000


    def setup() {
        accounts = buildAccountList(ACCOUNT_COUNT)
        activities = buildActivityList(ACTIVITY_COUNT)
        dialers = buildDialerList(DIALER_COUNT)

    }

    List<String> buildSymendMessages(int count) {
        buildSymendMessages(count, Integer.MAX_VALUE, Integer.MAX_VALUE)
    }

    List<String> buildSymendMessages(int primaryNodeCount, int connectionCount, int primaryNodePropertyCount) {
        List<String> messages = []
        Random random = new Random()
        (1..primaryNodeCount).each {
            def message = MessageBuilder.builder()
                    .Name("Collection:+${UUID.randomUUID()}")
                    .NodeType("CollectionEvent")
                    .Priority(1)
                    .SourceSystem("menome_test_framework")
                    .ConformedDimensions("ACCTNUM": random.nextInt(5000), "ENT_SEQ_NO": random.nextInt(5000))
                    .Properties(deriveProperties(primaryNodePropertyCount))
                    .Connections(deriveConnections(connectionCount))
                    .build()
                    .toJSON()

            messages.add(message)
        }
        messages
    }

    List<String> buildSymendAccounts(int count) {
        List<String> accounts = []
        (1..count).each { int accountId ->
            String message = MessageBuilder.builder()
                    .Name("Account ${accountId}")
                    .NodeType("Account")
                    .Priority(1)
                    .SourceSystem("menome_test_framework")
                    .ConformedDimensions(["ACCTNUM": accountId])
                    .build()
                    .toJSON()
            accounts.add(message)
        }
        accounts
    }


    List<String> buildSymendActivities(count) {
        List<String> activities = []
        (1..count).each { int activityCodeId ->
            String activity = MessageBuilder.builder()
                    .Name("Activity ${activityCodeId}")
                    .NodeType("CollectionActivity")
                    .Priority(1)
                    .SourceSystem("menome_test_framework")
                    .ConformedDimensions(["COL_ACTIVITY_CODE": activityCodeId])
                    .build()
                    .toJSON()
            activities.add(activity)
        }
        activities
    }

    List<String> buildSymendDialerEntries(count) {
        List<String> dialerEntries = []
        (1..count).each { int accountNum ->
            String activity = MessageBuilder.builder()
                    .Name("Dialer Entry ${accountNum}")
                    .NodeType("DialerEntry")
                    .Priority(1)
                    .SourceSystem("menome_test_framework")
                    .ConformedDimensions(["SNAPSHOT_SDT": accountNum, "ACCTNUM": accountNum])
                    .build()
                    .toJSON()
            dialerEntries.add(activity)
        }
        dialerEntries
    }


    Map deriveProperties(int countOfPropertiesToTake) {
        Random random = new Random()
        def today = Instant.now().toString()
        Map<String, Object> properties = [
                "VENDOR_SDC"              : UUID.randomUUID(),
                "APPLICATION_ID"          : UUID.randomUUID(),
                "DL_SERVICE_CODE"         : UUID.randomUUID(),
                "ASGN_COLLECTOR"          : UUID.randomUUID(),
                "ASGN_AGENCY"             : UUID.randomUUID(),
                "APPROVAL_COLLECTOR"      : UUID.randomUUID(),
                "COL_PATH_CODE"           : UUID.randomUUID(),
                "COL_ACTV_CODE"           : UUID.randomUUID(),
                "COL_ACTV_TYPE_IND"       : UUID.randomUUID(),
                "COL_WAIVER_IND"          : UUID.randomUUID(),
                "COL_COLLECTOR_ID"        : UUID.randomUUID(),
                "COL_NEXT_STP_APR_COD"    : UUID.randomUUID(),
                "COL_ACTV_RSN_CODE"       : UUID.randomUUID(),
                "FRAUD_TREATMENT_IND"     : UUID.randomUUID(),
                "COL_ASSIGNED_GRP"        : UUID.randomUUID(),
                "COL_TYPE"                : UUID.randomUUID(),
                "PA_CATEGORY"             : UUID.randomUUID(),
                "OUTSRC_AGENCY"           : UUID.randomUUID(),
                "COL_WO_REASON"           : UUID.randomUUID(),
                "COL_SMS_LTR_CODE"        : UUID.randomUUID(),
                "COL_WAIVER_RSN_CODE"     : UUID.randomUUID(),
                "COLLECTION_TIMED_ACTION1": UUID.randomUUID(),
                "COLLECTION_TIMED_ACTION2": UUID.randomUUID(),
                "COLLECTION_TIMED_ACTION3": UUID.randomUUID(),
                "COLLECTION_TIMED_ACTION4": UUID.randomUUID(),
                "COLLECTION_TIMED_ACTION5": UUID.randomUUID(),
                "BAN"                     : random.nextInt(100),
                "ENT_SEQ_NO"              : random.nextInt(100),
                "OPERATOR_ID"             : random.nextInt(100),
                "DL_UPDATE_STAMP"         : random.nextInt(100),
                "COL_STEP_NUM"            : random.nextInt(100),
                "SYS_CREATION_DATE"       : today,
                "SYS_UPDATE_DATE"         : today,
                "COL_ACTV_DATE"           : today,
                "ASGN_COLL_DATE"          : today,
                "ASGN_AGENCY_DATE"        : today,
                "COL_NEXT_STEP_DATE"      : today,
                "FRAUD_TREATMENT_DATE"    : today,
                "COL_WAIVER_EXP_DATE"     : today,
                "COL_ACTV_DATE_OLD"       : today,
                "SYS_DATE"                : today
        ] as Map<String, Object>

        properties.take(Integer.min(countOfPropertiesToTake, properties.size()))
    }

    List<Connection> deriveConnections(int connectionCount) {

        Random random = new Random()

        List<Connection> connections = [
                accounts[random.nextInt(ACCOUNT_COUNT)]
                , activities[random.nextInt(ACTIVITY_COUNT)]
                , dialers[random.nextInt(DIALER_COUNT)]]

        connections.take(Integer.min(connectionCount, connections.size()))
    }

    List<Connection> buildAccountList(int count) {
        Random random = new Random()
        List<Connection> connections = []
        (1..count).each {
            def accountId = random.nextInt(count)
            def account = Connection.builder().Name("Account ${accountId}").NodeType("Account").RelType("COLLECTION_ENTRY_FOR_ACCOUNT").ForewardRel(true).ConformedDimensions(["ACCTNUM": accountId]).Properties(["SourceSystem": "menome_test_framework"]).build()
            connections.add(account)
        }
        connections
    }

    List<Connection> buildActivityList(count) {
        Random random = new Random()
        List<Connection> connections = []
        (1..count).each {
            def activityCodeId = random.nextInt(count)
            def activity = Connection.builder().Name("Activity ${activityCodeId}").NodeType("CollectionActivity").RelType("ACTIVITY").ForewardRel(true).ConformedDimensions(["COL_ACTIVITY_CODE": activityCodeId]).Properties(["SourceSystem": "menome_test_framework"]).build()
            connections.add(activity)
        }
        connections
    }

    List<Connection> buildDialerList(count) {
        Random random = new Random()
        List<Connection> connections = []
        (1..count).each {
            def key = random.nextInt(count)
            def activity = Connection.builder().Name("Dialer Entry ${key}").NodeType("DialerEntry").RelType("COLLECTION").ForewardRel(true).ConformedDimensions(["SNAPSHOT_SDT": key, "ACCTNUM": key]).Properties(["SourceSystem": "menome_test_framework"]).build()
            connections.add(activity)
        }
        connections
    }

}
