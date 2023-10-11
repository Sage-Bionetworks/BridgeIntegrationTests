package org.sagebionetworks.bridge.sdk.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.sagebionetworks.bridge.rest.api.ForSuperadminsApi;
import org.sagebionetworks.bridge.rest.api.ForWorkersApi;
import org.sagebionetworks.bridge.rest.api.StudiesApi;
import org.sagebionetworks.bridge.rest.exceptions.EntityNotFoundException;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.rest.model.UploadTableRow;
import org.sagebionetworks.bridge.rest.model.UploadTableRowQuery;
import org.sagebionetworks.bridge.user.TestUser;
import org.sagebionetworks.bridge.user.TestUserHelper;
import org.sagebionetworks.bridge.util.IntegTestUtils;

public class UploadTableTest {
    private static final DateTime CREATED_ON_1 = DateTime.parse("2018-01-01T00:00:00Z");
    private static final DateTime CREATED_ON_2 = DateTime.parse("2018-01-02T00:00:00Z");
    private static final DateTime CREATED_ON_3 = DateTime.parse("2018-01-03T00:00:00Z");
    private static final DateTime CREATED_ON_4 = DateTime.parse("2018-01-04T00:00:00Z");
    private static final DateTime CREATED_ON_5 = DateTime.parse("2018-01-05T00:00:00Z");
    private static final String DUMMY_ASSESSMENT_ID_A = "dummy-assessment-A";
    private static final String DUMMY_ASSESSMENT_ID_B = "dummy-assessment-B";
    private static final String DUMMY_HEALTH_CODE = "dummy-health-code";
    private static final String DUMMY_RECORD_ID_1 = "dummy-record-1";
    private static final String DUMMY_RECORD_ID_2 = "dummy-record-2";
    private static final String DUMMY_RECORD_ID_3 = "dummy-record-3";
    private static final String DUMMY_RECORD_ID_4 = "dummy-record-4";
    private static final String DUMMY_RECORD_ID_5 = "dummy-record-5";

    private static TestUser admin;
    private static ForSuperadminsApi superadminApi;
    private static ForWorkersApi workersApi;

    private Study study;
    private String studyId;

    @BeforeClass
    public static void beforeClass() throws IOException {
        admin = TestUserHelper.getSignedInAdmin();
        superadminApi = admin.getClient(ForSuperadminsApi.class);
        workersApi = admin.getClient(ForWorkersApi.class);
    }

    @Before
    public void before() throws Exception {
        // Create study.
        StudiesApi studiesApi = admin.getClient(StudiesApi.class);
        studyId = Tests.randomIdentifier(HealthDataEx3Test.class);
        study = new Study().identifier(studyId).name(studyId);
        studiesApi.createStudy(study).execute();
        study = studiesApi.getStudy(studyId).execute().body();
    }

    @After
    public void after() throws Exception {
        if (study != null) {
            admin.getClient(StudiesApi.class).deleteStudy(study.getIdentifier(), true).execute();
        }
    }

    @Test
    public void test() throws IOException {
        // Create a row.
        UploadTableRow row = new UploadTableRow().recordId(DUMMY_RECORD_ID_1).assessmentId(DUMMY_ASSESSMENT_ID_A)
                .assessmentRevision(1).createdOn(CREATED_ON_1).testData(true).healthCode(DUMMY_HEALTH_CODE)
                .participantVersion(1).putMetadataItem("foo", "bar")
                .putDataItem("baz", "qux");
        workersApi.saveUploadTableRowForWorker(IntegTestUtils.TEST_APP_ID, studyId, row).execute();

        // Get the row back and verify.
        row = superadminApi.getUploadTableRowForSuperadmin(IntegTestUtils.TEST_APP_ID, studyId, DUMMY_RECORD_ID_1)
                .execute().body();
        assertEquals(IntegTestUtils.TEST_APP_ID, row.getAppId());
        assertEquals(studyId, row.getStudyId());
        assertEquals(DUMMY_RECORD_ID_1, row.getRecordId());
        assertEquals(DUMMY_ASSESSMENT_ID_A, row.getAssessmentId());
        assertEquals(1, row.getAssessmentRevision().intValue());
        assertEquals(CREATED_ON_1, row.getCreatedOn());
        assertTrue(row.isTestData());
        assertEquals(DUMMY_HEALTH_CODE, row.getHealthCode());
        assertEquals(1, row.getParticipantVersion().intValue());
        assertEquals("bar", row.getMetadata().get("foo"));
        assertEquals("qux", row.getData().get("baz"));

        // Update the row. Just add a data field.
        row.putDataItem("aaa", "bbb");
        workersApi.saveUploadTableRowForWorker(IntegTestUtils.TEST_APP_ID, studyId, row).execute();

        // Get the row back. Just verify we have the old and new data fields.
        row = superadminApi.getUploadTableRowForSuperadmin(IntegTestUtils.TEST_APP_ID, studyId, DUMMY_RECORD_ID_1)
                .execute().body();
        assertEquals("qux", row.getData().get("baz"));
        assertEquals("bbb", row.getData().get("aaa"));

        // Delete the row.
        superadminApi.deleteUploadTableRowForSuperadmin(IntegTestUtils.TEST_APP_ID, studyId, DUMMY_RECORD_ID_1)
                .execute();

        // Verify the row is gone.
        try {
            superadminApi.getUploadTableRowForSuperadmin(IntegTestUtils.TEST_APP_ID, studyId, DUMMY_RECORD_ID_1)
                    .execute();
            fail("expected exception");
        } catch (EntityNotFoundException ex) {
            // expected exception
        }
    }

    @Test
    public void query() throws IOException {
        // Make some table rows with the following fields:
        // 1. row with assessment A revision 1, createdOn 2018-01-01
        // 2. row with assessment A revision 2, createdOn 2018-01-02
        // 3. row with assessment B revision 1, createdOn 2018-01-03
        // 4. row with assessment B revision 2, createdOn 2018-01-04, testData=true
        // 5. row with assessment B revision 3, createdOn 2018-01-05, testData=true
        UploadTableRow row1 = new UploadTableRow().recordId(DUMMY_RECORD_ID_1).assessmentId(DUMMY_ASSESSMENT_ID_A)
                .assessmentRevision(1).createdOn(CREATED_ON_1).testData(false).healthCode(DUMMY_HEALTH_CODE);
        workersApi.saveUploadTableRowForWorker(IntegTestUtils.TEST_APP_ID, studyId, row1).execute();

        UploadTableRow row2 = new UploadTableRow().recordId(DUMMY_RECORD_ID_2).assessmentId(DUMMY_ASSESSMENT_ID_A)
                .assessmentRevision(2).createdOn(CREATED_ON_2).testData(false).healthCode(DUMMY_HEALTH_CODE);
        workersApi.saveUploadTableRowForWorker(IntegTestUtils.TEST_APP_ID, studyId, row2).execute();

        UploadTableRow row3 = new UploadTableRow().recordId(DUMMY_RECORD_ID_3).assessmentId(DUMMY_ASSESSMENT_ID_B)
                .assessmentRevision(1).createdOn(CREATED_ON_3).testData(false).healthCode(DUMMY_HEALTH_CODE);
        workersApi.saveUploadTableRowForWorker(IntegTestUtils.TEST_APP_ID, studyId, row3).execute();

        UploadTableRow row4 = new UploadTableRow().recordId(DUMMY_RECORD_ID_4).assessmentId(DUMMY_ASSESSMENT_ID_B)
                .assessmentRevision(2).createdOn(CREATED_ON_4).testData(true).healthCode(DUMMY_HEALTH_CODE);
        workersApi.saveUploadTableRowForWorker(IntegTestUtils.TEST_APP_ID, studyId, row4).execute();

        UploadTableRow row5 = new UploadTableRow().recordId(DUMMY_RECORD_ID_5).assessmentId(DUMMY_ASSESSMENT_ID_B)
                .assessmentRevision(3).createdOn(CREATED_ON_5).testData(true).healthCode(DUMMY_HEALTH_CODE);
        workersApi.saveUploadTableRowForWorker(IntegTestUtils.TEST_APP_ID, studyId, row5).execute();

        // Basic query with only app and study. Verify we get only 3 rows (no test data). Rows can be in any order, so
        // throw record IDs into a set.
        List<UploadTableRow> rowList = workersApi.queryUploadTableRowsForWorker(IntegTestUtils.TEST_APP_ID, studyId,
                new UploadTableRowQuery()).execute().body().getItems();
        assertEquals(3, rowList.size());
        Set<String> recordIdSet = rowList.stream().map(UploadTableRow::getRecordId).collect(Collectors.toSet());
        assertEquals(3, recordIdSet.size());
        assertTrue(recordIdSet.contains(DUMMY_RECORD_ID_1));
        assertTrue(recordIdSet.contains(DUMMY_RECORD_ID_2));
        assertTrue(recordIdSet.contains(DUMMY_RECORD_ID_3));

        // Query with assessment ID A and revision 1. Verify we get only record 1.
        UploadTableRowQuery query = new UploadTableRowQuery().assessmentId(DUMMY_ASSESSMENT_ID_A)
                .assessmentRevision(1);
        rowList = workersApi.queryUploadTableRowsForWorker(IntegTestUtils.TEST_APP_ID, studyId, query).execute().body()
                .getItems();
        assertEquals(1, rowList.size());
        assertEquals(DUMMY_RECORD_ID_1, rowList.get(0).getRecordId());

        // Query with start and end date. End date is exclusive, so if we set startDate=CREATED_ON_2 and
        // endDate=CREATED_ON_3, we'll only get record 2.
        query = new UploadTableRowQuery().startTime(CREATED_ON_2).endTime(CREATED_ON_3);
        rowList = workersApi.queryUploadTableRowsForWorker(IntegTestUtils.TEST_APP_ID, studyId, query).execute().body()
                .getItems();
        assertEquals(1, rowList.size());
        assertEquals(DUMMY_RECORD_ID_2, rowList.get(0).getRecordId());

        // Query with includeTestData=true. This will return all 5 rows.
        query = new UploadTableRowQuery().includeTestData(true);
        rowList = workersApi.queryUploadTableRowsForWorker(IntegTestUtils.TEST_APP_ID, studyId, query).execute().body()
                .getItems();
        assertEquals(5, rowList.size());
        recordIdSet = rowList.stream().map(UploadTableRow::getRecordId).collect(Collectors.toSet());
        assertEquals(5, recordIdSet.size());
        assertTrue(recordIdSet.contains(DUMMY_RECORD_ID_1));
        assertTrue(recordIdSet.contains(DUMMY_RECORD_ID_2));
        assertTrue(recordIdSet.contains(DUMMY_RECORD_ID_3));
        assertTrue(recordIdSet.contains(DUMMY_RECORD_ID_4));
        assertTrue(recordIdSet.contains(DUMMY_RECORD_ID_5));

        // Pagination test. Set page size to 2. We should get a page of 2, a second page of 2, and a page of 1.
        recordIdSet = new HashSet<>();
        query = new UploadTableRowQuery().includeTestData(true).start(0).pageSize(2);
        rowList = workersApi.queryUploadTableRowsForWorker(IntegTestUtils.TEST_APP_ID, studyId, query).execute().body()
                .getItems();
        assertEquals(2, rowList.size());
        recordIdSet.add(rowList.get(0).getRecordId());
        recordIdSet.add(rowList.get(1).getRecordId());

        query = new UploadTableRowQuery().includeTestData(true).start(2).pageSize(2);
        rowList = workersApi.queryUploadTableRowsForWorker(IntegTestUtils.TEST_APP_ID, studyId, query).execute().body()
                .getItems();
        assertEquals(2, rowList.size());
        recordIdSet.add(rowList.get(0).getRecordId());
        recordIdSet.add(rowList.get(1).getRecordId());

        query = new UploadTableRowQuery().includeTestData(true).start(4).pageSize(2);
        rowList = workersApi.queryUploadTableRowsForWorker(IntegTestUtils.TEST_APP_ID, studyId, query).execute().body()
                .getItems();
        assertEquals(1, rowList.size());
        recordIdSet.add(rowList.get(0).getRecordId());

        assertEquals(5, recordIdSet.size());
        assertTrue(recordIdSet.contains(DUMMY_RECORD_ID_1));
        assertTrue(recordIdSet.contains(DUMMY_RECORD_ID_2));
        assertTrue(recordIdSet.contains(DUMMY_RECORD_ID_3));
        assertTrue(recordIdSet.contains(DUMMY_RECORD_ID_4));
        assertTrue(recordIdSet.contains(DUMMY_RECORD_ID_5));

        // Get the next page just for good measure. It will be empty.
        query = new UploadTableRowQuery().includeTestData(true).start(6).pageSize(2);
        rowList = workersApi.queryUploadTableRowsForWorker(IntegTestUtils.TEST_APP_ID, studyId, query).execute().body()
                .getItems();
        assertEquals(0, rowList.size());
    }
}
