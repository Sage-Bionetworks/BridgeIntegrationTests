package org.sagebionetworks.bridge.sdk.integration;

import static org.junit.Assert.*;
import static org.sagebionetworks.bridge.rest.model.PerformanceOrder.SEQUENTIAL;
import static org.sagebionetworks.bridge.rest.model.Role.DEVELOPER;
import static org.sagebionetworks.bridge.sdk.integration.InitListener.CLINIC_VISIT;
import static org.sagebionetworks.bridge.sdk.integration.Tests.STUDY_ID_1;
import static org.sagebionetworks.bridge.util.IntegTestUtils.TEST_APP_ID;

import java.util.Set;
import java.util.stream.Collectors;

import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sagebionetworks.bridge.rest.api.AssessmentsApi;
import org.sagebionetworks.bridge.rest.api.ForConsentedUsersApi;
import org.sagebionetworks.bridge.rest.api.ForDevelopersApi;
import org.sagebionetworks.bridge.rest.api.SchedulesV2Api;
import org.sagebionetworks.bridge.rest.model.AdherenceRecord;
import org.sagebionetworks.bridge.rest.model.AdherenceRecordUpdates;
import org.sagebionetworks.bridge.rest.model.Assessment;
import org.sagebionetworks.bridge.rest.model.AssessmentReference2;
import org.sagebionetworks.bridge.rest.model.EventStreamDay;
import org.sagebionetworks.bridge.rest.model.EventStreamWindow;
import org.sagebionetworks.bridge.rest.model.Schedule2;
import org.sagebionetworks.bridge.rest.model.Session;
import org.sagebionetworks.bridge.rest.model.SessionCompletionState;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.rest.model.StudyActivityEvent;
import org.sagebionetworks.bridge.rest.model.StudyActivityEventList;
import org.sagebionetworks.bridge.rest.model.TimeWindow;
import org.sagebionetworks.bridge.rest.model.WeeklyAdherenceReport;
import org.sagebionetworks.bridge.user.TestUser;
import org.sagebionetworks.bridge.user.TestUserHelper;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class WeeklyAdherenceReportTest {

    private TestUser participant;
    private TestUser developer;
    
    private Schedule2 schedule;
    private Assessment assessmentA;
    private Assessment assessmentB;
    private String asmtATag;
    private String asmtBTag;

    @Before
    public void before() throws Exception {
        developer = TestUserHelper.createAndSignInUser(EventStreamAdherenceReportTest.class, false, DEVELOPER);
        ForDevelopersApi developersApi = developer.getClient(ForDevelopersApi.class);
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
                .addStartEventIdsItem("enrollment")
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
                .addStartEventIdsItem("enrollment")
                .interval("P7D")
                .performanceOrder(SEQUENTIAL)
                .addAssessmentsItem(asmtToReference(assessmentA))
                .addAssessmentsItem(asmtToReference(assessmentB))
                .addTimeWindowsItem(new TimeWindow()
                        .startTime("08:00").expiration("PT12H"));
        
        Session s3 = new Session()
                .name("Session #3")
                .addStartEventIdsItem(CLINIC_VISIT)
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
    }

    @Test
    public void test() throws Exception {
        participant = TestUserHelper.createAndSignInUser(getClass(), true);
        
        ForConsentedUsersApi userApi = participant.getClient(ForConsentedUsersApi.class);
        
        // let's do the first available thing in the report
        StudyActivityEventList events = userApi.getStudyActivityEvents(STUDY_ID_1).execute().body();
        StudyActivityEvent enrollmentEvent = events.getItems().stream()
                .filter(event -> event.getEventId().equals("enrollment"))
                .findFirst().get();
        
        WeeklyAdherenceReport report = userApi.getUsersStudyParticipantWeeklyAdherenceReport(STUDY_ID_1).execute().body();
        
        assertEquals(report.getRowLabels(), ImmutableList.of("Week 1 : Session #1", "Week 1 : Session #2"));
        assertEquals(report.getParticipant().getIdentifier(), participant.getUserId());
        assertEquals(report.getParticipant().getEmail(), participant.getEmail());
        assertEquals(report.getWeeklyAdherencePercent(), Integer.valueOf(0));
        
        // There's no Session #3 because it's not applicable to this user (no event for it)
        Set<String> sessionNames = report.getByDayEntries().values().stream()
            .flatMap(list -> list.stream())
            .map(EventStreamDay::getSessionName)
            .collect(Collectors.toSet());
        assertEquals(ImmutableSet.of("Session #1", "Session #2"), sessionNames);
        
        EventStreamWindow win = report.getByDayEntries().get("0").get(0).getTimeWindows().get(0);
        assertEquals(win.getState(), SessionCompletionState.UNSTARTED);
        String instanceGuid = win.getSessionInstanceGuid();
        
        AdherenceRecord rec = new AdherenceRecord();
        rec.setInstanceGuid(instanceGuid);
        rec.setEventTimestamp(enrollmentEvent.getTimestamp());
        rec.setStartedOn(DateTime.now());
        rec.setFinishedOn(DateTime.now());
        
        AdherenceRecordUpdates updates = new AdherenceRecordUpdates();
        updates.setRecords(ImmutableList.of(rec));
        userApi.updateAdherenceRecords(STUDY_ID_1, updates).execute();
        
        report = userApi.getUsersStudyParticipantWeeklyAdherenceReport(STUDY_ID_1).execute().body();
        assertEquals(report.getWeeklyAdherencePercent(), Integer.valueOf(100));

        win = report.getByDayEntries().get("0").get(0).getTimeWindows().get(0);
        assertEquals(win.getState(), SessionCompletionState.COMPLETED);
    }
    
    private AssessmentReference2 asmtToReference(Assessment asmt) {
        return new AssessmentReference2()
                .appId(TEST_APP_ID)
                .identifier(asmt.getIdentifier())
                .guid(asmt.getGuid());
    }
}
