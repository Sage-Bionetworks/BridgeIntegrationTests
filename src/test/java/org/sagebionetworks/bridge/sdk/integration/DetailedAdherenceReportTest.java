package org.sagebionetworks.bridge.sdk.integration;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.RandomStringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sagebionetworks.bridge.rest.api.AssessmentsApi;
import org.sagebionetworks.bridge.rest.api.ForConsentedUsersApi;
import org.sagebionetworks.bridge.rest.api.ForStudyCoordinatorsApi;
import org.sagebionetworks.bridge.rest.api.SchedulesV2Api;
import org.sagebionetworks.bridge.rest.exceptions.EntityNotFoundException;
import org.sagebionetworks.bridge.rest.model.ActivityEventUpdateType;
import org.sagebionetworks.bridge.rest.model.AdherenceRecord;
import org.sagebionetworks.bridge.rest.model.AdherenceRecordUpdates;
import org.sagebionetworks.bridge.rest.model.Assessment;
import org.sagebionetworks.bridge.rest.model.AssessmentCompletionState;
import org.sagebionetworks.bridge.rest.model.AssessmentReference2;
import org.sagebionetworks.bridge.rest.model.DetailedAdherenceReport;
import org.sagebionetworks.bridge.rest.model.DetailedAdherenceReportAssessmentRecord;
import org.sagebionetworks.bridge.rest.model.DetailedAdherenceReportSessionRecord;
import org.sagebionetworks.bridge.rest.model.ParticipantSchedule;
import org.sagebionetworks.bridge.rest.model.PerformanceOrder;
import org.sagebionetworks.bridge.rest.model.Role;
import org.sagebionetworks.bridge.rest.model.Schedule2;
import org.sagebionetworks.bridge.rest.model.ScheduledAssessment;
import org.sagebionetworks.bridge.rest.model.ScheduledSession;
import org.sagebionetworks.bridge.rest.model.Session;
import org.sagebionetworks.bridge.rest.model.SessionCompletionState;
import org.sagebionetworks.bridge.rest.model.StudyBurst;
import org.sagebionetworks.bridge.rest.model.TimeWindow;
import org.sagebionetworks.bridge.user.TestUser;
import org.sagebionetworks.bridge.user.TestUserHelper;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.sagebionetworks.bridge.sdk.integration.Tests.STUDY_ID_1;
import static org.sagebionetworks.bridge.util.IntegTestUtils.SAGE_ID;
import static org.sagebionetworks.bridge.util.IntegTestUtils.TEST_APP_ID;

public class DetailedAdherenceReportTest {
    
    private static final String TIME_ZONE = "America/Chicago";
    private static final DateTimeZone DATE_TIME_ZONE = DateTimeZone.forID(TIME_ZONE);
    private DateTime startedOn;
    private DateTime finishedOn;
    private DateTime uploadedOn;
    private TestUser studyDesigner;
    private TestUser user;
    
    private Schedule2 schedule;
    private Assessment assessmentA;
    private Assessment assessmentB;
    private Assessment assessmentC;
    
    SchedulesV2Api scheduleApi;
    ForStudyCoordinatorsApi coordApi;
    
    @Before
    public void before() throws Exception {
        studyDesigner = TestUserHelper.createAndSignInUser(getClass(), false, Role.STUDY_DESIGNER);
        user = TestUserHelper.createAndSignInUser(getClass(), true);
        
        AssessmentsApi asmtsApi = studyDesigner.getClient(AssessmentsApi.class);
        String asmtATag = RandomStringUtils.randomAlphabetic(5);
        String asmtBTag = RandomStringUtils.randomAlphabetic(5);
        String asmtCTag = RandomStringUtils.randomAlphabetic(5);
        
        assessmentA = new Assessment()
                .phase(Assessment.PhaseEnum.DRAFT)
                .identifier(asmtATag)
                .osName("Universal")
                .ownerId(SAGE_ID)
                .title("Survey");
        assessmentA = asmtsApi.createAssessment(assessmentA).execute().body();
        
        assessmentB = new Assessment()
                .phase(Assessment.PhaseEnum.DRAFT)
                .identifier(asmtBTag)
                .osName("Universal")
                .ownerId(SAGE_ID)
                .title("tappingTest");
        assessmentB = asmtsApi.createAssessment(assessmentB).execute().body();
        
        assessmentC = new Assessment()
                .phase(Assessment.PhaseEnum.DRAFT)
                .identifier(asmtCTag)
                .osName("Universal")
                .ownerId(SAGE_ID)
                .title("finalSurvey");
        assessmentC = asmtsApi.createAssessment(assessmentC).execute().body();
        
        schedule = createSchedule(assessmentA, assessmentB, assessmentC);
        scheduleApi = studyDesigner.getClient(SchedulesV2Api.class);
        coordApi = studyDesigner.getClient(ForStudyCoordinatorsApi.class);
    }
    
    @After
    public void after() throws Exception {
        TestUser admin = TestUserHelper.getSignedInAdmin();
        AssessmentsApi assessmentsApi = admin.getClient(AssessmentsApi.class);
        
        if (schedule != null && schedule.getGuid() != null) {
            admin.getClient(SchedulesV2Api.class).deleteSchedule(schedule.getGuid()).execute();
        }
        if (assessmentA != null && assessmentA.getGuid() != null) {
            assessmentsApi.deleteAssessment(assessmentA.getGuid(), true).execute();
        }
        if (assessmentB != null && assessmentB.getGuid() != null) {
            assessmentsApi.deleteAssessment(assessmentB.getGuid(), true).execute();
        }
        if (assessmentC != null && assessmentC.getGuid() != null) {
            assessmentsApi.deleteAssessment(assessmentC.getGuid(), true).execute();
        }
        if (user != null) {
            user.signOutAndDeleteUser();
        }
        if (studyDesigner != null) {
            studyDesigner.signOutAndDeleteUser();
        }
    }
    
    @Test
    public void test() throws Exception {
        try {
            Schedule2 existing = scheduleApi.getScheduleForStudy(STUDY_ID_1).execute().body();
            schedule.setGuid(existing.getGuid());
            schedule.setVersion(existing.getVersion());
        } catch(EntityNotFoundException e) {
        }
        
        schedule = scheduleApi.saveScheduleForStudy(STUDY_ID_1, schedule).execute().body();
        
        ForConsentedUsersApi userApi = user.getClient(ForConsentedUsersApi.class);
        
        // generate timeline_retrieved event
        userApi.getParticipantScheduleForSelf(STUDY_ID_1, TIME_ZONE).execute();
        
        ParticipantSchedule participantSchedule = userApi.getParticipantScheduleForSelf(
                STUDY_ID_1, TIME_ZONE).execute().body();
        completeAssessments(userApi, participantSchedule);
        
        DetailedAdherenceReport report = coordApi.getDetailedParticipantAdherenceReport(STUDY_ID_1, user.getUserId())
                .execute().body();
    
        assertEquals(user.getUserId(), report.getParticipant().getIdentifier());
        assertTrue(report.isTestAccount());
        assertNotNull(report.getJoinedDate());
        assertEquals(TIME_ZONE, report.getClientTimeZone());
    
        List<DetailedAdherenceReportSessionRecord> sessionRecords = report.getSessionRecords();
        
        // verify burst session
        DetailedAdherenceReportSessionRecord burstSession = sessionRecords.get(0);
        assertEquals("Week 1/Burst 1", burstSession.getBurstName());
        assertEquals("Study Burst", burstSession.getBurstId());
        assertEquals("Study Burst Tapping Test", burstSession.getSessionName());
        assertEquals(SessionCompletionState.COMPLETED, burstSession.getSessionStatus());
        // dates should be formatted to match the client's time zone even when submitted in another zone initially.
        assertEquals(startedOn.withZone(DATE_TIME_ZONE).toString(), burstSession.getSessionStart().toString());
        assertEquals(finishedOn.withZone(DATE_TIME_ZONE).toString(), burstSession.getSessionCompleted().toString());
        
        List<DetailedAdherenceReportAssessmentRecord> burstAssessmentRecords = burstSession.getAssessmentRecords();
        
        DetailedAdherenceReportAssessmentRecord burstAssessmentRecord = burstAssessmentRecords.get(0);
        assertEquals("tappingTest", burstAssessmentRecord.getAssessmentName());
        assertEquals(AssessmentCompletionState.COMPLETED, burstAssessmentRecord.getAssessmentStatus());
        assertEquals(startedOn.withZone(DATE_TIME_ZONE).toString(), burstAssessmentRecord.getAssessmentStart().toString());
        assertEquals(finishedOn.withZone(DATE_TIME_ZONE).toString(), burstAssessmentRecord.getAssessmentCompleted().toString());
        assertEquals(uploadedOn.withZone(DATE_TIME_ZONE).toString(), burstAssessmentRecord.getAssessmentUploadedOn().toString());
        
        
        // verify non-burst session
        DetailedAdherenceReportSessionRecord basicSession = sessionRecords.get(3);
        assertNull(basicSession.getBurstName());
        assertNull(basicSession.getBurstId());
        assertEquals("Initial Survey", basicSession.getSessionName());
        assertEquals(SessionCompletionState.COMPLETED, basicSession.getSessionStatus());
        assertEquals(startedOn.withZone(DATE_TIME_ZONE).toString(), basicSession.getSessionStart().toString());
        assertEquals(finishedOn.withZone(DATE_TIME_ZONE).toString(), basicSession.getSessionCompleted().toString());
    
        List<DetailedAdherenceReportAssessmentRecord> basicSessionAssessmentRecords = basicSession.getAssessmentRecords();
    
        DetailedAdherenceReportAssessmentRecord basicSessionAssessmentRecord = basicSessionAssessmentRecords.get(0);
        assertEquals("Survey", basicSessionAssessmentRecord.getAssessmentName());
        assertEquals(AssessmentCompletionState.COMPLETED, basicSessionAssessmentRecord.getAssessmentStatus());
        assertEquals(startedOn.withZone(DATE_TIME_ZONE).toString(), basicSessionAssessmentRecord.getAssessmentStart().toString());
        assertEquals(finishedOn.withZone(DATE_TIME_ZONE).toString(), basicSessionAssessmentRecord.getAssessmentCompleted().toString());
        assertEquals(uploadedOn.withZone(DATE_TIME_ZONE).toString(), basicSessionAssessmentRecord.getAssessmentUploadedOn().toString());
        
        
        // verify persistent window is not included
        for (DetailedAdherenceReportSessionRecord sessionRecord : sessionRecords) {
            if (sessionRecord.getSessionName().equals("Baseline Tapping Test")) {
                fail("The Baseline Tapping Test session is persistent and should not be included in the report.");
            }
        }
    }
    
    protected void completeAssessments(ForConsentedUsersApi userApi, ParticipantSchedule schedule) throws IOException {
        startedOn = DateTime.now();
        finishedOn = startedOn.plusSeconds(2);
        uploadedOn = startedOn.plusSeconds(2);
        AdherenceRecordUpdates updates = new AdherenceRecordUpdates();
        for (ScheduledSession schSession : schedule.getSchedule()) {
            for (ScheduledAssessment schAssessment : schSession.getAssessments()) {
                DateTime timestamp = schedule.getEventTimestamps().get(schSession.getStartEventId());
                if (timestamp == null) {
                    continue;
                }
                AdherenceRecord record = new AdherenceRecord();
                record.setInstanceGuid(schAssessment.getInstanceGuid());
                record.setStartedOn(startedOn.withZone(DateTimeZone.forID("America/Los_Angeles")));
                record.setFinishedOn(finishedOn.withZone(DateTimeZone.forID("America/Los_Angeles")));
                record.setUploadedOn(uploadedOn.withZone(DateTimeZone.forID("America/Los_Angeles")));
                record.setEventTimestamp(timestamp);
                updates.addRecordsItem(record);
            }
        }
        if (!updates.getRecords().isEmpty()) {
            userApi.updateAdherenceRecords(Tests.STUDY_ID_1, updates).execute().body();
        }
    }
    
    private Schedule2 createSchedule(Assessment asmtA, Assessment asmtB, Assessment asmtC) {
        // survey
        AssessmentReference2 ref1 = new AssessmentReference2();
        ref1.setAppId(TEST_APP_ID);
        ref1.setGuid(asmtA.getGuid());
        ref1.setIdentifier(asmtA.getIdentifier());
        ref1.setTitle(asmtA.getTitle());
        
        // tapping test
        AssessmentReference2 ref2 = new AssessmentReference2();
        ref2.setAppId(TEST_APP_ID);
        ref2.setGuid(asmtB.getGuid());
        ref2.setIdentifier(asmtB.getIdentifier());
        ref2.setTitle(asmtB.getTitle());
        
        AssessmentReference2 ref3 = new AssessmentReference2();
        ref3.setAppId(TEST_APP_ID);
        ref3.setGuid(asmtC.getGuid());
        ref3.setIdentifier(asmtC.getIdentifier());
        ref3.setTitle(asmtC.getTitle());
        
        Schedule2 schedule = new Schedule2();
        
        // Initial survey
        TimeWindow win1 = new TimeWindow();
        win1.setGuid("win1");
        win1.setStartTime("00:00");
        win1.setExpiration("P1D");
        
        Session s1 = new Session();
        s1.setAssessments(ImmutableList.of(ref1));
        s1.setDelay("P1D");
        s1.setStartEventIds(ImmutableList.of("timeline_retrieved"));
        s1.setTimeWindows(ImmutableList.of(win1));
        s1.setGuid("initialSurveyGuid");
        s1.setName("Initial Survey");
        s1.setPerformanceOrder(PerformanceOrder.SEQUENTIAL);
        
        // Baseline tapping test
        TimeWindow win2 = new TimeWindow();
        win2.setGuid("win2");
        win2.setStartTime("00:00");
        win2.setExpiration("P1D");
        win2.setPersistent(true);
        
        Session s2 = new Session();
        s2.setAssessments(ImmutableList.of(ref2));
        s2.setDelay("P2D");
        s2.setStartEventIds(ImmutableList.of("timeline_retrieved"));
        s2.setTimeWindows(ImmutableList.of(win2));
        s2.setGuid("baselineGuid");
        s2.setName("Baseline Tapping Test");
        s2.setPerformanceOrder(PerformanceOrder.SEQUENTIAL);
        
        // Study Burst
        StudyBurst burst = new StudyBurst();
        burst.setIdentifier("Study Burst");
        burst.setOriginEventId("timeline_retrieved");
        burst.setUpdateType(ActivityEventUpdateType.IMMUTABLE);
        burst.setDelay("P1W");
        burst.setOccurrences(3);
        burst.setInterval("P1W");
        
        TimeWindow win3 = new TimeWindow();
        win3.setGuid("win3");
        win3.setStartTime("00:00");
        win3.setExpiration("P1D");
        
        Session s3 = new Session();
        s3.setAssessments(ImmutableList.of(ref2));
        s3.setStudyBurstIds(ImmutableList.of("Study Burst"));
        s3.setTimeWindows(ImmutableList.of(win3));
        s3.setGuid("burstTappingGuid");
        s3.setName("Study Burst Tapping Test");
        s3.setPerformanceOrder(PerformanceOrder.SEQUENTIAL);
        
        schedule.setSessions(ImmutableList.of(s1, s2, s3));
        schedule.setName("Test Schedule");
        schedule.setOwnerId("sage-bionetworks");
        schedule.setDuration("P4W");
        schedule.setStudyBursts(ImmutableList.of(burst));
        
        return schedule;
    }
}
