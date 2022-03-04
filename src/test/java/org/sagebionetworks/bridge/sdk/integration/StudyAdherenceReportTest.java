package org.sagebionetworks.bridge.sdk.integration;

import static org.junit.Assert.*;
import static org.sagebionetworks.bridge.rest.model.ParticipantStudyProgress.DONE;
import static org.sagebionetworks.bridge.rest.model.ParticipantStudyProgress.IN_PROGRESS;
import static org.sagebionetworks.bridge.rest.model.ParticipantStudyProgress.UNSTARTED;
import static org.sagebionetworks.bridge.sdk.integration.Tests.STUDY_ID_1;
import static org.sagebionetworks.bridge.util.IntegTestUtils.SAGE_ID;
import static org.sagebionetworks.bridge.util.IntegTestUtils.TEST_APP_ID;

import java.io.IOException;

import org.apache.commons.lang3.RandomStringUtils;
import org.joda.time.DateTime;
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
import org.sagebionetworks.bridge.rest.model.AssessmentReference2;
import org.sagebionetworks.bridge.rest.model.ParticipantSchedule;
import org.sagebionetworks.bridge.rest.model.PerformanceOrder;
import org.sagebionetworks.bridge.rest.model.Role;
import org.sagebionetworks.bridge.rest.model.Schedule2;
import org.sagebionetworks.bridge.rest.model.ScheduledAssessment;
import org.sagebionetworks.bridge.rest.model.ScheduledSession;
import org.sagebionetworks.bridge.rest.model.Session;
import org.sagebionetworks.bridge.rest.model.StudyActivityEventRequest;
import org.sagebionetworks.bridge.rest.model.StudyAdherenceReport;
import org.sagebionetworks.bridge.rest.model.StudyBurst;
import org.sagebionetworks.bridge.rest.model.TimeWindow;
import org.sagebionetworks.bridge.user.TestUser;
import org.sagebionetworks.bridge.user.TestUserHelper;

import com.google.common.collect.ImmutableList;

public class StudyAdherenceReportTest {
    
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
                .identifier(asmtATag)
                .osName("Universal")
                .ownerId(SAGE_ID)
                .title("Survey");
        assessmentA = asmtsApi.createAssessment(assessmentA).execute().body();
        
        assessmentB = new Assessment()
                .identifier(asmtBTag)
                .osName("Universal")
                .ownerId(SAGE_ID)
                .title("tappingTest");
        assessmentB = asmtsApi.createAssessment(assessmentB).execute().body();

        assessmentC = new Assessment()
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
        
        StudyAdherenceReport report = coordApi.getStudyParticipantAdherenceReport(
                STUDY_ID_1, user.getUserId()).execute().body();
        assertEquals(UNSTARTED, report.getProgression());
        assertEquals(Integer.valueOf(100), report.getAdherencePercent());
        
        ForConsentedUsersApi userApi = user.getClient(ForConsentedUsersApi.class);
        
        // generate timeline_retrieved event
        userApi.getParticipantScheduleForSelf(Tests.STUDY_ID_1).execute();
        
        report = coordApi.getStudyParticipantAdherenceReport(STUDY_ID_1, user.getUserId()).execute().body();
        assertEquals(IN_PROGRESS, report.getProgression());
        assertEquals(Integer.valueOf(0), report.getAdherencePercent());
        
        ParticipantSchedule participantSchedule = userApi.getParticipantScheduleForSelf(STUDY_ID_1).execute().body();
        completeAssessments(userApi, participantSchedule);

        // The supplemental survey has not been done. If we add it, adherence goes down.
        StudyActivityEventRequest request = new StudyActivityEventRequest()
                .eventId("custom:event1").timestamp(DateTime.now());
        userApi.createStudyActivityEvent(STUDY_ID_1, request, true, false).execute();
        
        report = coordApi.getStudyParticipantAdherenceReport(STUDY_ID_1, user.getUserId()).execute().body();
        assertEquals(IN_PROGRESS, report.getProgression());
        assertEquals(Integer.valueOf(85), report.getAdherencePercent());
        
        // do those activities
        participantSchedule = userApi.getParticipantScheduleForSelf(STUDY_ID_1).execute().body();
        completeAssessments(userApi, participantSchedule);
        
        report = coordApi.getStudyParticipantAdherenceReport(STUDY_ID_1, user.getUserId()).execute().body();
        assertEquals(DONE, report.getProgression());
        assertEquals(Integer.valueOf(100), report.getAdherencePercent());
    }

    protected void completeAssessments(ForConsentedUsersApi userApi, ParticipantSchedule schedule) throws IOException {
        AdherenceRecordUpdates updates = new AdherenceRecordUpdates();
        for (ScheduledSession schSession : schedule.getSchedule()) {
            for (ScheduledAssessment schAssessment : schSession.getAssessments()) {
                DateTime timestamp = schedule.getEventTimestamps().get(schSession.getStartEventId());
                if (timestamp == null) {
                    continue;
                }
                AdherenceRecord record = new AdherenceRecord();
                record.setInstanceGuid(schAssessment.getInstanceGuid());
                record.setStartedOn(DateTime.now());
                record.setFinishedOn(DateTime.now());
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
        
        // tapping test
        AssessmentReference2 ref2 = new AssessmentReference2();
        ref2.setAppId(TEST_APP_ID);
        ref2.setGuid(asmtB.getGuid());
        ref2.setIdentifier(asmtB.getIdentifier());
        
        AssessmentReference2 ref3 = new AssessmentReference2();
        ref3.setAppId(TEST_APP_ID);
        ref3.setGuid(asmtC.getGuid());
        ref3.setIdentifier(asmtC.getIdentifier());
        
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
        
        // Final survey
        TimeWindow win4 = new TimeWindow();
        win4.setGuid("win4");
        win4.setStartTime("00:00");
        win4.setExpiration("P3D");
        
        Session s4 = new Session();
        s4.setAssessments(ImmutableList.of(ref3));
        s4.setDelay("P24D");
        s4.setStartEventIds(ImmutableList.of("timeline_retrieved"));
        s4.setTimeWindows(ImmutableList.of(win4));
        s4.setGuid("finalSurveyGuid");
        s4.setName("Final Survey");
        s4.setPerformanceOrder(PerformanceOrder.SEQUENTIAL);
        
        // Supplemental survey that does not fire for our test user
        TimeWindow win5 = new TimeWindow();
        win5.setGuid("win5");
        win5.setStartTime("00:00");
        win5.setExpiration("PT12H");

        Session s5 = new Session(); 
        s5.setAssessments(ImmutableList.of(ref1));
        s5.setStartEventIds(ImmutableList.of("custom:event1"));
        s5.setTimeWindows(ImmutableList.of(win5));
        s5.setGuid("session5");
        s5.setName("Supplemental Survey");
        s5.setPerformanceOrder(PerformanceOrder.SEQUENTIAL);

        schedule.setSessions(ImmutableList.of(s1, s2, s3, s4, s5));
        schedule.setName("Test Schedule");
        schedule.setOwnerId("sage-bionetworks");
        schedule.setDuration("P4W");
        schedule.setStudyBursts(ImmutableList.of(burst));
        
        return schedule;        
    }
    
    
}
