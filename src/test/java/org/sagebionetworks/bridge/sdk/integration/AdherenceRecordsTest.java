package org.sagebionetworks.bridge.sdk.integration;

import static java.util.stream.Collectors.toList;
import static org.joda.time.DateTimeZone.UTC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.sagebionetworks.bridge.rest.model.AdherenceRecordType.ASSESSMENT;
import static org.sagebionetworks.bridge.rest.model.AdherenceRecordType.SESSION;
import static org.sagebionetworks.bridge.rest.model.PerformanceOrder.SEQUENTIAL;
import static org.sagebionetworks.bridge.rest.model.Role.DEVELOPER;
import static org.sagebionetworks.bridge.rest.model.Role.RESEARCHER;
import static org.sagebionetworks.bridge.rest.model.SortOrder.DESC;
import static org.sagebionetworks.bridge.sdk.integration.InitListener.CLINIC_VISIT;
import static org.sagebionetworks.bridge.sdk.integration.InitListener.FAKE_ENROLLMENT;
import static org.sagebionetworks.bridge.sdk.integration.Tests.STUDY_ID_1;
import static org.sagebionetworks.bridge.util.IntegTestUtils.TEST_APP_ID;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.sagebionetworks.bridge.rest.RestUtils;
import org.sagebionetworks.bridge.rest.api.AssessmentsApi;
import org.sagebionetworks.bridge.rest.api.ForConsentedUsersApi;
import org.sagebionetworks.bridge.rest.api.ForDevelopersApi;
import org.sagebionetworks.bridge.rest.api.ForResearchersApi;
import org.sagebionetworks.bridge.rest.api.SchedulesV2Api;
import org.sagebionetworks.bridge.rest.model.AdherenceRecord;
import org.sagebionetworks.bridge.rest.model.AdherenceRecordList;
import org.sagebionetworks.bridge.rest.model.AdherenceRecordUpdates;
import org.sagebionetworks.bridge.rest.model.AdherenceRecordsSearch;
import org.sagebionetworks.bridge.rest.model.Assessment;
import org.sagebionetworks.bridge.rest.model.AssessmentInfo;
import org.sagebionetworks.bridge.rest.model.AssessmentReference2;
import org.sagebionetworks.bridge.rest.model.EventStream;
import org.sagebionetworks.bridge.rest.model.EventStreamAdherenceReport;
import org.sagebionetworks.bridge.rest.model.Schedule2;
import org.sagebionetworks.bridge.rest.model.ScheduledAssessment;
import org.sagebionetworks.bridge.rest.model.ScheduledSession;
import org.sagebionetworks.bridge.rest.model.Session;
import org.sagebionetworks.bridge.rest.model.SessionInfo;
import org.sagebionetworks.bridge.rest.model.SortOrder;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.rest.model.StudyActivityEvent;
import org.sagebionetworks.bridge.rest.model.StudyActivityEventList;
import org.sagebionetworks.bridge.rest.model.StudyActivityEventRequest;
import org.sagebionetworks.bridge.rest.model.TimeWindow;
import org.sagebionetworks.bridge.rest.model.Timeline;
import org.sagebionetworks.bridge.user.TestUser;
import org.sagebionetworks.bridge.user.TestUserHelper;

/**
 * This test is based on the example schedule and timeline that are provided in our 
 * developer documentation. That document provides a visual representation of which
 * records should be returned by these searches. Each record is created with a  tag 
 * that also provides a string description of the record, as follows:
 * 
 *  S[1-3]   - S1 (session 1), S2 (session 2), S3 (session3)
 *  D[01-21] - D07 = day 7, D21 = day 21, etc.
 *  W[1-2]   - W1 (time window 1), W2 (time window 2)
 *  A        - record for assessment A
 *  B        - record for assessment B
 *  
 *  If there's no A or B at the end of the tag, the record is for the session.
 *
 * @see https://developer.sagebridge.org/articles/v2/scheduling.html
 */
public class AdherenceRecordsTest {

    private static final DateTime ENROLLMENT = DateTime.parse("2020-05-10T00:00:00.000Z");
    private static final DateTime T1 = DateTime.parse("2020-05-18T00:00:00.000Z");
    private static final DateTime T2 = DateTime.parse("2020-09-03T00:00:00.000Z");
    private TestUser developer;
    private TestUser participant;
    private TestUser researcher;
    private Schedule2 schedule;
    private Assessment assessmentA;
    private Assessment assessmentB;
    private Session session1;
    private Session session2;
    private Session session3;
    private String asmtATag;
    private String asmtBTag;
    private Timeline timeline;
    private ForDevelopersApi developersApi;
    
    @Before
    public void before() throws Exception {
        developer = TestUserHelper.createAndSignInUser(AdherenceRecordsTest.class, false, DEVELOPER);
        developersApi = developer.getClient(ForDevelopersApi.class);
        AssessmentsApi asmtsApi = developer.getClient(AssessmentsApi.class);
        
        Study study = developersApi.getStudy(STUDY_ID_1).execute().body();
        
        // If there's a schedule associated to study 1, we need to delete it.
        TestUser admin = TestUserHelper.getSignedInAdmin();
        if (study.getScheduleGuid() != null) {
            admin.getClient(SchedulesV2Api.class).deleteSchedule(study.getScheduleGuid()).execute();
        }        
        
        asmtATag = Tests.randomIdentifier(getClass());
        asmtBTag = Tests.randomIdentifier(getClass());
        
        assessmentA = new Assessment()
                .identifier(asmtATag)
                .osName("Universal")
                .ownerId(developer.getSession().getOrgMembership())
                .title("Assessment A");
        assessmentA = asmtsApi.createAssessment(assessmentA).execute().body();
        
        assessmentB = new Assessment()
                .identifier(asmtBTag)
                .osName("Universal")
                .ownerId(developer.getSession().getOrgMembership())
                .title("Assessment B");
        assessmentB = asmtsApi.createAssessment(assessmentB).execute().body();
        
        Session s1 = new Session()
                .name("Session #1")
                .addStartEventIdsItem(FAKE_ENROLLMENT)
                .delay("P2D")
                .interval("P3D")
                .performanceOrder(SEQUENTIAL)
                .addAssessmentsItem(asmtToReference(assessmentA))
                .addTimeWindowsItem(new TimeWindow()
                        .startTime("08:00").expiration("PT6H"))
                .addTimeWindowsItem(new TimeWindow()
                        .startTime("16:00").expiration("PT6H").persistent(true));

        Session s2 = new Session()
                .name("Session #2")
                .addStartEventIdsItem(CLINIC_VISIT)
                .interval("P7D")
                .performanceOrder(SEQUENTIAL)
                .addAssessmentsItem(asmtToReference(assessmentA))
                .addAssessmentsItem(asmtToReference(assessmentB))
                .addTimeWindowsItem(new TimeWindow()
                        .startTime("08:00").expiration("PT12H"));
        
        Session s3 = new Session()
                .name("Session #3")
                .addStartEventIdsItem(FAKE_ENROLLMENT)
                .performanceOrder(SEQUENTIAL)
                .addAssessmentsItem(asmtToReference(assessmentB))
                .addTimeWindowsItem(new TimeWindow().startTime("08:00").persistent(true));
        
        schedule = new Schedule2()
                .name("AdheredRecordsTest Schedule")
                .duration("P22D")
                .addSessionsItem(s1)
                .addSessionsItem(s2)
                .addSessionsItem(s3);
        schedule = developersApi.saveScheduleForStudy(STUDY_ID_1, schedule).execute().body();
        session1 = schedule.getSessions().get(0);
        session2 = schedule.getSessions().get(1);
        session3 = schedule.getSessions().get(2);
    }
    
    @After
    public void after() throws Exception {
        TestUser admin = TestUserHelper.getSignedInAdmin();
        AssessmentsApi assessmentsApi = admin.getClient(AssessmentsApi.class);
        SchedulesV2Api schedulesApi = admin.getClient(SchedulesV2Api.class);
        if (participant != null) {
            participant.signOutAndDeleteUser();
        }
        if (schedule != null && schedule.getGuid() != null) {
            schedulesApi.deleteSchedule(schedule.getGuid()).execute();
        }
        if (assessmentA != null && assessmentA.getGuid() != null) {
            assessmentsApi.deleteAssessment(assessmentA.getGuid(), true).execute();
        }
        if (assessmentB != null && assessmentB.getGuid() != null) {
            assessmentsApi.deleteAssessment(assessmentB.getGuid(), true).execute();
        }
        if (developer != null) {
            developer.signOutAndDeleteUser();
        }
        if (researcher != null) {
            researcher.signOutAndDeleteUser();
        }
    }

    @Test
    public void test() throws Exception {
        participant = TestUserHelper.createAndSignInUser(AdherenceRecordsTest.class, true);
        createAdherenceRecords();
        
        // Everything
        assertRecords(new AdherenceRecordsSearch(), 
                "S1D02W1", "S1D02W1A", "S1D02W2", "S1D02W2A", "S1D05W1", "S1D05W1A", 
                "S1D05W2", "S1D05W2A", "S1D08W1", "S1D08W1A", "S1D08W2", "S1D08W2A", 
                "S1D11W1", "S1D11W1A", "S1D11W2", "S1D11W2A", "S1D11W2A", "S1D11W2A",
                "S1D14W1", "S1D14W1A", "S1D14W2", "S1D14W2A", "S1D17W1", "S1D17W1A", 
                "S1D17W2", "S1D17W2A", "S1D17W2A", "S1D20W1", "S1D20W1A", "S1D20W2", 
                "S1D20W2A", "S2D00W1 T1", "S2D00W1 T2", "S2D00W1A T1", "S2D00W1A T2",
                "S2D00W1B T1", "S2D00W1B T2", "S2D07W1 T1", "S2D07W1 T2", "S2D07W1A T1", 
                "S2D07W1A T2", "S2D07W1B T1", "S2D07W1B T2", "S2D14W1 T1", "S2D14W1 T2", 
                "S2D14W1A T1", "S2D14W1A T2", "S2D14W1B T1", "S2D14W1B T2", "S2D21W1 T1", 
                "S2D21W1 T2", "S2D21W1A T1", "S2D21W1A T2", "S2D21W1B T1", "S2D21W1B T2", 
                "S3D00W1", "S3D00W1B", "S3D03W1B", "S3D10W1B", "S3D13W1B", "S3D21W1B");        
        
        // Select by specific assessments. We’re using these tags to find the instance
        // IDs (as they’re just a random assortment from the timeline).
        List<String> instanceGuids = getInstanceGuidsByTag(false, "S1D02W1A", "S1D08W2",
                "S1D17W1A", "S2D07W1B T1", "S2D14W1B T1", "S2D21W1 T2", "S3D13W1B");
        assertEquals(7, instanceGuids.size());
        
        // However, instance GUIDs can have multiple time points in different streams,
        // so there are more records here than you might expect.
        assertRecords(new AdherenceRecordsSearch().instanceGuids(instanceGuids), 
                "S1D02W1A", "S1D08W2", "S1D17W1A", "S2D07W1B T1", "S2D07W1B T2", 
                "S2D14W1B T1", "S2D14W1B T2", "S2D21W1 T1", "S2D21W1 T2", "S3D00W1B", 
                "S3D03W1B", "S3D10W1B", "S3D13W1B", "S3D21W1B");
        
        // If you *really* want to retrieve specific records, there is a format of
        // an instance GUID that includes the startedOn timestamp. This retrieves
        // specific records.
        instanceGuids = getInstanceGuidsByTag(true, "S1D02W1A", "S1D08W2",
                "S1D17W1A", "S2D07W1B T1", "S2D14W1B T1", "S2D21W1 T2", "S3D13W1B");
        assertRecords(new AdherenceRecordsSearch().instanceGuids(instanceGuids), 
                "S1D02W1A", "S1D08W2", "S1D17W1A", "S2D07W1B T1", "S2D14W1B T1", 
                "S2D21W1 T2", "S3D13W1B");
        
        // Date-based search for assessments
        assertRecords(new AdherenceRecordsSearch().adherenceRecordType(ASSESSMENT)
                .startTime(DateTime.parse("2020-05-10T00:00:00.000Z"))
                .endTime(DateTime.parse("2020-05-17T00:00:00.000Z")), 
                "S3D00W1B", "S1D02W1A", "S1D02W2A", "S3D03W1B", "S1D05W1A", 
                "S1D05W2A", "S3D10W1B");
        
        // query by time window for assessments
        assertRecords(new AdherenceRecordsSearch().adherenceRecordType(ASSESSMENT)
                .addTimeWindowGuidsItem(session1.getTimeWindows().get(1).getGuid()), 
                "S1D02W2A", "S1D05W2A", "S1D08W2A", "S1D11W2A", "S1D11W2A", 
                "S1D11W2A", "S1D14W2A", "S1D17W2A", "S1D17W2A", "S1D20W2A");

        // query by time window, includeRepeats = false, sort ASC by default
        assertRecordsAndTimestamps(new AdherenceRecordsSearch().adherenceRecordType(ASSESSMENT)
                .addTimeWindowGuidsItem(session1.getTimeWindows().get(1).getGuid())
                .includeRepeats(false),
                "S1D02W2A@2020-05-12T16:00:00.000Z", "S1D05W2A@2020-05-15T16:00:00.000Z", 
                "S1D08W2A@2020-05-18T16:00:00.000Z", "S1D11W2A@2020-05-21T16:00:00.000Z", 
                "S1D14W2A@2020-05-24T16:00:00.000Z", "S1D17W2A@2020-05-27T17:00:00.000Z", 
                "S1D20W2A@2020-05-30T16:00:00.000Z");
        
        // query by time window, includeRepeats = false, sort DESC
        assertRecordsAndTimestamps(new AdherenceRecordsSearch().adherenceRecordType(ASSESSMENT)
                .addTimeWindowGuidsItem(session1.getTimeWindows().get(1).getGuid())
                .includeRepeats(false).sortOrder(DESC),
                "S1D02W2A@2020-05-12T16:00:00.000Z", "S1D05W2A@2020-05-15T16:00:00.000Z", 
                "S1D08W2A@2020-05-18T16:00:00.000Z", "S1D11W2A@2020-05-21T21:00:00.000Z", 
                "S1D14W2A@2020-05-24T16:00:00.000Z", "S1D17W2A@2020-05-27T21:00:00.000Z", 
                "S1D20W2A@2020-05-30T16:00:00.000Z");
        
        // query by time window, get the sessions
        assertRecords(new AdherenceRecordsSearch().adherenceRecordType(SESSION)
                .addTimeWindowGuidsItem(session1.getTimeWindows().get(1).getGuid()), 
                "S1D02W2", "S1D05W2", "S1D08W2", "S1D11W2", "S1D14W2", 
                "S1D17W2", "S1D20W2");
        
        // get session #2, all series
        assertRecords(new AdherenceRecordsSearch()
                .addSessionGuidsItem(session2.getGuid()), 
                "S2D00W1 T1", "S2D00W1A T1", "S2D00W1B T1", "S2D07W1 T1", "S2D07W1B T1", 
                "S2D07W1A T1", "S2D14W1 T1", "S2D14W1A T1", "S2D14W1B T1", "S2D21W1B T1", 
                "S2D21W1A T1", "S2D21W1 T1", "S2D00W1 T2", "S2D00W1A T2", "S2D00W1B T2", 
                "S2D07W1 T2", "S2D07W1B T2", "S2D07W1A T2", "S2D14W1 T2", "S2D14W1A T2", 
                "S2D14W1B T2", "S2D21W1B T2", "S2D21W1A T2", "S2D21W1 T2");

        // get session #2, first time series
        assertRecords(new AdherenceRecordsSearch()
                .addSessionGuidsItem(session2.getGuid())
                .putEventTimestampsItem("custom:" + CLINIC_VISIT, T1), 
                "S2D00W1 T1", "S2D00W1A T1", "S2D00W1B T1", "S2D07W1B T1", "S2D07W1A T1", 
                "S2D07W1 T1", "S2D14W1 T1", "S2D14W1B T1", "S2D14W1A T1", "S2D21W1B T1", 
                "S2D21W1A T1", "S2D21W1 T1");
        
        // get session #2, most recent series only
        assertRecords(new AdherenceRecordsSearch()
                .addSessionGuidsItem(session2.getGuid())
                .putEventTimestampsItem("custom:" + CLINIC_VISIT, T2), 
                "S2D00W1 T2", "S2D00W1A T2", "S2D00W1B T2", "S2D07W1B T2", "S2D07W1A T2", 
                "S2D07W1 T2", "S2D14W1 T2", "S2D14W1B T2", "S2D14W1A T2", "S2D21W1B T2", 
                "S2D21W1A T2", "S2D21W1 T2");
        
        // This can also be done with a flag to the server to just use current timestamps
        assertRecords(new AdherenceRecordsSearch()
                .addSessionGuidsItem(session2.getGuid())
                .currentTimestampsOnly(true), 
                "S2D00W1 T2", "S2D00W1A T2", "S2D00W1B T2", "S2D07W1B T2", "S2D07W1A T2", 
                "S2D07W1 T2", "S2D14W1 T2", "S2D14W1B T2", "S2D14W1A T2", "S2D21W1B T2", 
                "S2D21W1A T2", "S2D21W1 T2");
        
        // this also works because we're cool that way (no custom: prefix on eventId)
        assertRecords(new AdherenceRecordsSearch()
                .addSessionGuidsItem(session2.getGuid())
                .putEventTimestampsItem(CLINIC_VISIT, T2), 
                "S2D00W1 T2", "S2D00W1A T2", "S2D00W1B T2", "S2D07W1B T2", "S2D07W1A T2", 
                "S2D07W1 T2", "S2D14W1 T2", "S2D14W1B T2", "S2D14W1A T2", "S2D21W1B T2", 
                "S2D21W1A T2", "S2D21W1 T2");
        
        // get session #2, sessions only, all time series
        assertRecords(new AdherenceRecordsSearch()
                .addSessionGuidsItem(session2.getGuid())
                .adherenceRecordType(SESSION), 
                "S2D00W1 T1", "S2D00W1 T2", "S2D07W1 T1", "S2D07W1 T2", "S2D14W1 T1", 
                "S2D14W1 T2", "S2D21W1 T1", "S2D21W1 T2");

        // paging works
        AdherenceRecordsSearch ars = new AdherenceRecordsSearch().pageSize(20);
        ForConsentedUsersApi usersApi = participant.getClient(ForConsentedUsersApi.class);

        AdherenceRecordList list = usersApi.searchForAdherenceRecords(STUDY_ID_1, ars).execute().body();
        assertEquals(Integer.valueOf(61), list.getTotal());
        assertEquals(Integer.valueOf(20), list.getRequestParams().getPageSize());
        assertEquals(20, list.getItems().size());

        ars.offsetBy(20);
        list = usersApi.searchForAdherenceRecords(STUDY_ID_1, ars).execute().body();
        assertEquals(20, list.getItems().size());
        
        ars.offsetBy(40);
        list = usersApi.searchForAdherenceRecords(STUDY_ID_1, ars).execute().body();
        assertEquals(20, list.getItems().size());

        ars.offsetBy(60);
        list = usersApi.searchForAdherenceRecords(STUDY_ID_1, ars).execute().body();
        assertEquals(1, list.getItems().size());
        
        // sort order works
        list = usersApi.searchForAdherenceRecords(STUDY_ID_1, new AdherenceRecordsSearch()
                .addSessionGuidsItem(session3.getGuid())
                .sortOrder(SortOrder.ASC)).execute().body();
        assertEquals("2020-05-10T00:00:00.000Z", 
                list.getItems().get(0).getStartedOn().toString());
        
        list = usersApi.searchForAdherenceRecords(STUDY_ID_1, new AdherenceRecordsSearch()
                .addSessionGuidsItem(session3.getGuid())
                .sortOrder(SortOrder.DESC)).execute().body();
        assertEquals("2020-05-31T00:00:00.000Z", 
                list.getItems().get(0).getStartedOn().toString());

        // Test finishing and that they create events.
        instanceGuids = getInstanceGuidsByTag(false, "S1D02W1", "S1D08W1A");
        list = usersApi.searchForAdherenceRecords(STUDY_ID_1, new AdherenceRecordsSearch()
                .instanceGuids(instanceGuids)).execute().body();
        
        DateTime finishedOn = DateTime.now(DateTimeZone.UTC);
        
        List<String> tuple = findKeys(instanceGuids.get(0), instanceGuids.get(1));
        String sessKey = tuple.get(0);
        String asmtKey = tuple.get(1);
        
        list.getItems().get(0).setFinishedOn(finishedOn);
        list.getItems().get(1).setFinishedOn(finishedOn);
        usersApi.updateAdherenceRecords(STUDY_ID_1, 
                new AdherenceRecordUpdates().records(list.getItems())).execute();
        
        StudyActivityEventList activityList = usersApi.getStudyActivityEvents(STUDY_ID_1)
                .execute().body();

        boolean foundSessionEvent = false;
        boolean foundAssessmentEvent = false;
        for (StudyActivityEvent event : activityList.getItems()) {
            if (event.getEventId().equals(sessKey)) {
                foundSessionEvent = true;
                assertEquals(event.getTimestamp(), finishedOn);
            }
            if (event.getEventId().equals(asmtKey)) {
                foundAssessmentEvent = true;
                assertEquals(event.getTimestamp(), finishedOn);
            }
        }
        assertTrue(foundSessionEvent && foundAssessmentEvent);

        // Test optional fields
        instanceGuids = getInstanceGuidsByTag(false, "S1D02W1");
        list = usersApi.searchForAdherenceRecords(STUDY_ID_1, new AdherenceRecordsSearch()
                .instanceGuids(instanceGuids)).execute().body();
        AdherenceRecord record = list.getItems().get(0);
        record.setDeclined(true);
        record.setClientTimeZone("America/Los_Angeles");
        
        Map<String,String> map = new HashMap<>();
        map.put("A", "B");
        record.setClientData(map);

        usersApi.updateAdherenceRecords(STUDY_ID_1, new AdherenceRecordUpdates()
                .addRecordsItem(record)).execute();
        list = usersApi.searchForAdherenceRecords(STUDY_ID_1, new AdherenceRecordsSearch()
                .instanceGuids(instanceGuids)).execute().body();
        record = list.getItems().get(0);

        assertTrue(record.isDeclined());
        assertEquals("America/Los_Angeles", record.getClientTimeZone());
        @SuppressWarnings("unchecked")
        Map<String,String> retValue = (Map<String,String>)RestUtils.toType(record.getClientData(), Map.class); 
        assertEquals("B", retValue.get("A"));
        
        // Verify that there are references back to the schedule/timeline for each record
        list = usersApi.searchForAdherenceRecords(STUDY_ID_1, new AdherenceRecordsSearch()).execute().body();
        Set<String> sessionGuids = schedule.getSessions().stream().map(Session::getGuid).collect(Collectors.toSet());
        Set<String> assessmentGuids = schedule.getSessions().stream().flatMap(s -> s.getAssessments().stream())
                .map(AssessmentReference2::getGuid).collect(Collectors.toSet());
        for (AdherenceRecord oneRecord : list.getItems()) {
            if (oneRecord.getAssessmentGuid() != null) {
                assertTrue(assessmentGuids.contains(oneRecord.getAssessmentGuid()));
                assertNull(oneRecord.getSessionGuid());
            } else {
                assertTrue(sessionGuids.contains(oneRecord.getSessionGuid()));
                assertNull(oneRecord.getAssessmentGuid());
            }
        }

        // Deleting an adherence record from a non-persistent time window (tag: S1D02W1)
        researcher = TestUserHelper.createAndSignInUser(AdherenceRecordsTest.class, false, RESEARCHER);
        ForResearchersApi researchersApi = researcher.getClient(ForResearchersApi.class);
        
        researchersApi.deleteAdherenceRecord(STUDY_ID_1, participant.getUserId(),
                record.getInstanceGuid(),
                record.getEventTimestamp(),
                record.getStartedOn()).execute();

        instanceGuids = getInstanceGuidsByTag(false, "S1D02W1");
        assertEquals(0, instanceGuids.size());

        // Deleting an adherence record from a persistent time window (tag: S1D08W2A)
        instanceGuids = getInstanceGuidsByTag(false, "S1D08W2A");
        list = usersApi.searchForAdherenceRecords(STUDY_ID_1, new AdherenceRecordsSearch()
                .instanceGuids(instanceGuids)).execute().body();
        record = list.getItems().get(0);

        researchersApi.deleteAdherenceRecord(STUDY_ID_1, participant.getUserId(),
                record.getInstanceGuid(),
                record.getEventTimestamp(),
                record.getStartedOn()).execute();

        instanceGuids = getInstanceGuidsByTag(false, "S1D08W2A");
        assertEquals(0, instanceGuids.size());
    }
    
    @Test
    public void testSessionStateManagement() throws Exception {
        participant = TestUserHelper.createAndSignInUser(AdherenceRecordsTest.class, true);
        ForConsentedUsersApi usersApi = participant.getClient(ForConsentedUsersApi.class);
        
        // Create the fake enrollment timestamp
        usersApi.createStudyActivityEvent(STUDY_ID_1, new StudyActivityEventRequest()
                .eventId(CLINIC_VISIT).timestamp(T1), true, null).execute();
        
        timeline = usersApi.getTimelineForSelf(STUDY_ID_1, null).execute().body();
        SessionInfo session2 = timeline.getSessions().get(1); // session #2
        String sessionGuid = session2.getGuid();
        
        ScheduledSession schSession = timeline.getSchedule().stream()
            .filter(schSess -> schSess.getRefGuid().equals(sessionGuid))
            .findFirst().get();
        
        ScheduledAssessment asmt1 = schSession.getAssessments().get(0);
        ScheduledAssessment asmt2 = schSession.getAssessments().get(1);
        
        DateTime ts0 = DateTime.now(UTC).minusHours(2);
        DateTime ts1 = DateTime.now(UTC).minusHours(1);
        DateTime ts2 = DateTime.now(UTC);
        DateTime ts3 = DateTime.now(UTC).plusHours(1);
        DateTime ts4 = DateTime.now(UTC).plusHours(2);
        DateTime ts5 = DateTime.now(UTC).plusHours(3);
        
        // starting one assessments starts the session
        updateAssessmentRecord(usersApi, asmt1.getInstanceGuid(), ts1, null);
        
        AdherenceRecord sessionRecord = getSessionRecord(usersApi, schSession.getInstanceGuid());
        assertEquals(ts1, sessionRecord.getStartedOn());
        assertNull(sessionRecord.getFinishedOn());
        
        // starting both assessments doesn't change the session
        updateAssessmentRecord(usersApi, asmt2.getInstanceGuid(), ts3, null);

        sessionRecord = getSessionRecord(usersApi, schSession.getInstanceGuid());
        assertEquals(ts1, sessionRecord.getStartedOn());
        assertNull(sessionRecord.getFinishedOn());
        
        // finishing one assessment doesn't change the session
        updateAssessmentRecord(usersApi, asmt1.getInstanceGuid(), ts1, ts2);

        sessionRecord = getSessionRecord(usersApi, schSession.getInstanceGuid());
        assertEquals(ts1, sessionRecord.getStartedOn());
        assertNull(sessionRecord.getFinishedOn());

        // finishing both assessments finishes the session
        updateAssessmentRecord(usersApi, asmt2.getInstanceGuid(), ts3, ts4);
        
        sessionRecord = getSessionRecord(usersApi, schSession.getInstanceGuid());
        assertEquals(ts1, sessionRecord.getStartedOn());
        assertEquals(ts4, sessionRecord.getFinishedOn());

        // later changes DO not update the session
        updateAssessmentRecord(usersApi, asmt2.getInstanceGuid(), ts0, ts5);

        sessionRecord = getSessionRecord(usersApi, schSession.getInstanceGuid());
        assertEquals(ts0, sessionRecord.getStartedOn());
        assertEquals(ts5, sessionRecord.getFinishedOn());
        
        // declining one assessment doesn't decline the session
        declineAssessmentRecord(usersApi, asmt1.getInstanceGuid());

        sessionRecord = getSessionRecord(usersApi, schSession.getInstanceGuid());
        assertFalse(sessionRecord.isDeclined());
        assertEquals(ts0, sessionRecord.getStartedOn());
        assertNull(sessionRecord.getFinishedOn());
        
        
        Set<String> instanceGuids = usersApi.searchForAdherenceRecords(STUDY_ID_1, 
                new AdherenceRecordsSearch().declined(Boolean.TRUE)).execute().body().getItems()
                .stream().map(AdherenceRecord::getInstanceGuid).collect(Collectors.toSet());
        assertEquals(ImmutableSet.of(asmt1.getInstanceGuid()), instanceGuids);
        
        // declining both assessments declines the session. Also wipes out the 
        // finishedOn timestamp because the record is updated with no startedOn/finishedOn
        declineAssessmentRecord(usersApi, asmt2.getInstanceGuid());

        sessionRecord = getSessionRecord(usersApi, schSession.getInstanceGuid());
        assertTrue(sessionRecord.isDeclined());
        assertNull(sessionRecord.getStartedOn());
        assertNull(sessionRecord.getFinishedOn());
        
        // You can search and retrieve just these declined records.
        instanceGuids = usersApi.searchForAdherenceRecords(STUDY_ID_1, 
                new AdherenceRecordsSearch().declined(Boolean.TRUE)).execute().body().getItems()
                .stream().map(AdherenceRecord::getInstanceGuid).collect(Collectors.toSet());
        assertEquals(ImmutableSet.of(asmt1.getInstanceGuid(), asmt2.getInstanceGuid(), 
                schSession.getInstanceGuid()), instanceGuids);
        
        instanceGuids = usersApi.searchForAdherenceRecords(STUDY_ID_1, 
                new AdherenceRecordsSearch().declined(Boolean.FALSE)).execute().body().getItems()
                .stream().map(AdherenceRecord::getInstanceGuid).collect(Collectors.toSet());
        assertTrue(instanceGuids.isEmpty());
    }
    
    @Test
    public void eventStreamAdherenceReport() throws Exception { 
        participant = TestUserHelper.createAndSignInUser(AdherenceRecordsTest.class, true);
        ForConsentedUsersApi usersApi = participant.getClient(ForConsentedUsersApi.class);
        
        // Create the fake enrollment timestamp
        usersApi.createStudyActivityEvent(STUDY_ID_1, new StudyActivityEventRequest()
                .eventId(CLINIC_VISIT).timestamp(T1), true, null).execute();

        EventStreamAdherenceReport report = usersApi.getUsersStudyParticipantEventStreamAdherenceReport(STUDY_ID_1, DateTime.now(), false).execute().body();
        
        List<String> eventIds = report.getStreams().stream().map(EventStream::getStartEventId).collect(Collectors.toList());
        assertEquals(ImmutableList.of("custom:clinic_visit", "custom:fake_enrollment"), eventIds);
        
        EventStream clinicVisitStream = report.getStreams().get(0);
        assertEquals(clinicVisitStream.getByDayEntries().keySet(), ImmutableSet.of("0", "7", "14", "21"));
        
        EventStream fakeEnrollmentStream = report.getStreams().get(1);
        assertEquals(fakeEnrollmentStream.getByDayEntries().keySet(), ImmutableSet.of("2", "5", "8", "11", "14", "17", "20"));
    }
    
    private void updateAssessmentRecord(ForConsentedUsersApi usersApi, String instanceGuid, DateTime startedOn,
            DateTime finishedOn) throws Exception {
        AdherenceRecord record1 = new AdherenceRecord().instanceGuid(instanceGuid)
                .eventTimestamp(T1)
                .startedOn(startedOn).finishedOn(finishedOn);
        usersApi.updateAdherenceRecords(STUDY_ID_1, new AdherenceRecordUpdates().addRecordsItem(record1)).execute();

    }

    private void declineAssessmentRecord(ForConsentedUsersApi usersApi, String instanceGuid) throws Exception {
        AdherenceRecord record1 = new AdherenceRecord().instanceGuid(instanceGuid)
                .eventTimestamp(T1).declined(true);
        usersApi.updateAdherenceRecords(STUDY_ID_1, new AdherenceRecordUpdates().addRecordsItem(record1)).execute();
    }
    
    private AdherenceRecord getSessionRecord(ForConsentedUsersApi usersApi, String instanceGuid) throws Exception {
        return usersApi.searchForAdherenceRecords(STUDY_ID_1, 
                new AdherenceRecordsSearch().addInstanceGuidsItem(instanceGuid))
                .execute().body().getItems().get(0);
    }

    
    private AssessmentReference2 asmtToReference(Assessment asmt) {
        return new AssessmentReference2()
                .appId(TEST_APP_ID)
                .identifier(asmt.getIdentifier())
                .guid(asmt.getGuid());
    }
    private void createAdherenceRecords() throws Exception {
        ForConsentedUsersApi usersApi = participant.getClient(ForConsentedUsersApi.class);
        
        // Create the fake enrollment timestamp
        usersApi.createStudyActivityEvent(STUDY_ID_1, new StudyActivityEventRequest()
                .eventId(FAKE_ENROLLMENT).timestamp(ENROLLMENT), true, null).execute();
        
        timeline = usersApi.getTimelineForSelf(STUDY_ID_1, null).execute().body(); 

        // SESSION 1
        List<ScheduledSession> sessions = getScheduledSessions(timeline, session1.getGuid());
        session1Data(usersApi, sessions.get(0), "D02", "W1", "05-12", "00");
        session1Data(usersApi, sessions.get(1), "D02", "W2", "05-12", "16");
        session1Data(usersApi, sessions.get(2), "D05", "W1", "05-15", "00");
        session1Data(usersApi, sessions.get(3), "D05", "W2", "05-15", "16");
        session1Data(usersApi, sessions.get(4), "D08", "W1", "05-18", "00");
        session1Data(usersApi, sessions.get(5), "D08", "W2", "05-18", "16");
        session1Data(usersApi, sessions.get(6), "D11", "W1", "05-21", "00");
        session1Data(usersApi, sessions.get(7), "D11", "W2", "05-21", "16", "18", "21");
        session1Data(usersApi, sessions.get(8), "D14", "W1", "05-24", "00");
        session1Data(usersApi, sessions.get(9), "D14", "W2", "05-24", "16");
        session1Data(usersApi, sessions.get(10), "D17", "W1", "05-27", "00");
        session1Data(usersApi, sessions.get(11), "D17", "W2", "05-27", "17", "21");
        session1Data(usersApi, sessions.get(12), "D20", "W1", "05-30", "00");
        session1Data(usersApi, sessions.get(13), "D20", "W2", "05-30", "16");
        
        // SESSION 2
        sessions = getScheduledSessions(timeline, session2.getGuid());
        
        // FIRST SERIES
        usersApi.createStudyActivityEvent(STUDY_ID_1, new StudyActivityEventRequest()
                .eventId(CLINIC_VISIT).timestamp(T1), true, null).execute();
        session2Data(usersApi, sessions.get(0), T1, "T1", "D00", "05-18");
        session2Data(usersApi, sessions.get(1), T1, "T1", "D07", "05-25");
        session2Data(usersApi, sessions.get(2), T1, "T1", "D14", "06-01");
        session2Data(usersApi, sessions.get(3), T1, "T1", "D21", "06-08");

        // SECOND SERIES
        usersApi.createStudyActivityEvent(STUDY_ID_1, new StudyActivityEventRequest()
                .eventId(CLINIC_VISIT).timestamp(T2), true, null).execute();
        session2Data(usersApi, sessions.get(0), T2, "T2", "D00", "09-03");
        session2Data(usersApi, sessions.get(1), T2, "T2", "D07", "09-10");
        session2Data(usersApi, sessions.get(2), T2, "T2", "D14", "09-17");
        session2Data(usersApi, sessions.get(3), T2, "T2", "D21", "09-24");
        
        // SESSION 3 (also using fake enrollment timestamp) 
        sessions = getScheduledSessions(timeline, session3.getGuid());
        
        // one session record...
        usersApi.updateAdherenceRecords(STUDY_ID_1, new AdherenceRecordUpdates()
                .addRecordsItem(new AdherenceRecord()
                .instanceGuid(sessions.get(0).getInstanceGuid())
                .clientData("S3D00W1")
                .eventTimestamp(ENROLLMENT)
                .startedOn(getTimestamp("05-10")))).execute();
        session3Data(usersApi, sessions.get(0), "D00", "05-10");
        session3Data(usersApi, sessions.get(0), "D03", "05-13");
        session3Data(usersApi, sessions.get(0), "D10", "05-17");
        session3Data(usersApi, sessions.get(0), "D13", "05-20");
        session3Data(usersApi, sessions.get(0), "D21", "05-31");
    }
    private List<ScheduledSession> getScheduledSessions(Timeline timeline, String sessionGuid) {
        return timeline.getSchedule().stream()
                .filter(sess -> sess.getRefGuid().equals(sessionGuid))
                .collect(toList());
    }
    private void session1Data(ForConsentedUsersApi usersApi, ScheduledSession session, 
            String day, String window, String monthAndDay, String...hoursOfDay) throws IOException {
        
        usersApi.updateAdherenceRecords(STUDY_ID_1, new AdherenceRecordUpdates()
                .addRecordsItem(new AdherenceRecord()
                .instanceGuid(session.getInstanceGuid())
                .eventTimestamp(ENROLLMENT)
                .clientData("S1" + day + window)
                .startedOn(getTimestamp(monthAndDay, hoursOfDay[0])))).execute();
        for (String hod : hoursOfDay) {
            usersApi.updateAdherenceRecords(STUDY_ID_1, new AdherenceRecordUpdates()
                    .addRecordsItem(new AdherenceRecord()
                    .instanceGuid(session.getAssessments().get(0).getInstanceGuid())
                    .eventTimestamp(ENROLLMENT)
                    .clientData("S1" + day + window + "A")
                    .startedOn(getTimestamp(monthAndDay, hod)))).execute();
        }
    }
    private void session2Data(ForConsentedUsersApi usersApi, ScheduledSession session, 
            DateTime eventTimestamp, String eventTimestampTag, String day, String monthAndDay) throws IOException {
        
        usersApi.updateAdherenceRecords(STUDY_ID_1, new AdherenceRecordUpdates()
            .addRecordsItem(new AdherenceRecord()
                .instanceGuid(session.getInstanceGuid())
                .eventTimestamp(eventTimestamp)
                .clientData("S2"+day+"W1 " + eventTimestampTag)
                .startedOn(getTimestamp(monthAndDay)))
            .addRecordsItem(new AdherenceRecord()
                .instanceGuid(session.getAssessments().get(0).getInstanceGuid())
                .eventTimestamp(eventTimestamp)
                .clientData("S2"+day+"W1A " + eventTimestampTag)
                .startedOn(getTimestamp(monthAndDay)))
            .addRecordsItem(new AdherenceRecord()
                .instanceGuid(session.getAssessments().get(1).getInstanceGuid())
                .eventTimestamp(eventTimestamp)
                .clientData("S2"+day+"W1B " + eventTimestampTag)
                .startedOn(getTimestamp(monthAndDay)))).execute();
    }
    private void session3Data(ForConsentedUsersApi usersApi, 
            ScheduledSession session, String day, String monthAndDay) throws IOException {
        usersApi.updateAdherenceRecords(STUDY_ID_1, new AdherenceRecordUpdates()
            .addRecordsItem(new AdherenceRecord()
                .instanceGuid(session.getAssessments().get(0).getInstanceGuid())
                .clientData("S3" + day + "W1B")
                .eventTimestamp(ENROLLMENT)
                .startedOn(getTimestamp(monthAndDay)))).execute();
    }
    private DateTime getTimestamp(String monthAndDay) {
        return DateTime.parse("2020-" + monthAndDay + "T00:00:00.000Z");
    }
    private DateTime getTimestamp(String monthAndDay, String hourOfDay) {
        return DateTime.parse("2020-" + monthAndDay + "T" + hourOfDay + ":00:00.000Z");
    }
    private void assertRecords(AdherenceRecordsSearch search, String... expectedTags) throws Exception {
        ForConsentedUsersApi usersApi = participant.getClient(ForConsentedUsersApi.class);
        AdherenceRecordList list = usersApi.searchForAdherenceRecords(
                STUDY_ID_1, search).execute().body();
        // There will be duplicates so this has to be a list.
        List<String> tags = list.getItems().stream()
                .map(ar -> (String)ar.getClientData())
                .collect(toList());
        List<String> expectedTagList = Lists.newArrayList(expectedTags);
        Collections.sort(expectedTagList);
        Collections.sort(tags);
        assertEquals(expectedTagList, tags);   
    }
    private void assertRecordsAndTimestamps(AdherenceRecordsSearch search, String... expectedTags) throws Exception {
        ForConsentedUsersApi usersApi = participant.getClient(ForConsentedUsersApi.class);
        AdherenceRecordList list = usersApi.searchForAdherenceRecords(
                STUDY_ID_1, search).execute().body();
        // There will be duplicates so this has to be a list.
        List<String> tags = list.getItems().stream()
                .map(ar -> (String)ar.getClientData() + "@" + ar.getStartedOn())
                .collect(toList());
        List<String> expectedTagList = Lists.newArrayList(expectedTags);
        Collections.sort(expectedTagList);
        Collections.sort(tags);
        assertEquals(expectedTagList, tags);   
    }
    private List<String> getInstanceGuidsByTag(boolean includeStartedOn, String... tags) throws Exception {
        ImmutableSet<String> tagSet = ImmutableSet.copyOf(tags);
        
        AdherenceRecordsSearch search = new AdherenceRecordsSearch().pageSize(500);
        ForConsentedUsersApi usersApi = participant.getClient(ForConsentedUsersApi.class);
        AdherenceRecordList list = usersApi.searchForAdherenceRecords(
                STUDY_ID_1, search).execute().body();
        
        List<String> array = new ArrayList<>();
        for (AdherenceRecord record : list.getItems()) {
            if (tagSet.contains(record.getClientData().toString())) {
                if (includeStartedOn) {
                    array.add(record.getInstanceGuid() + "@" + record.getStartedOn().toString());
                } else {
                    array.add(record.getInstanceGuid());    
                }
            }
        }
        return array;
    }
    private List<String> findKeys(String sessInstGuid, String asmtInstGuid) {
        String sessKey = null;
        String asmtKey = null;
        for (ScheduledSession schSession : timeline.getSchedule()) {
            if (schSession.getInstanceGuid().equals(sessInstGuid)) {
                sessKey = "session:" + schSession.getRefGuid() + ":finished";
            }
            for (ScheduledAssessment schAssessment : schSession.getAssessments()) {
                if (schAssessment.getInstanceGuid().equals(asmtInstGuid)) {
                    
                    for (AssessmentInfo assessment : timeline.getAssessments()) {
                        if (assessment.getKey().equals(schAssessment.getRefKey())) {
                            asmtKey = "assessment:" + assessment.getIdentifier() + ":finished";
                        }
                    }
                }
            }
        }
        return ImmutableList.of(sessKey, asmtKey);
    }
}
