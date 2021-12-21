package org.sagebionetworks.bridge.sdk.integration;

import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.*;
import static org.sagebionetworks.bridge.rest.model.PerformanceOrder.SEQUENTIAL;
import static org.sagebionetworks.bridge.rest.model.Role.DEVELOPER;
import static org.sagebionetworks.bridge.rest.model.SessionCompletionState.EXPIRED;
import static org.sagebionetworks.bridge.rest.model.SessionCompletionState.NOT_APPLICABLE;
import static org.sagebionetworks.bridge.rest.model.SessionCompletionState.NOT_YET_AVAILABLE;
import static org.sagebionetworks.bridge.rest.model.SessionCompletionState.UNSTARTED;
import static org.sagebionetworks.bridge.sdk.integration.InitListener.CLINIC_VISIT;
import static org.sagebionetworks.bridge.sdk.integration.InitListener.FAKE_ENROLLMENT;
import static org.sagebionetworks.bridge.sdk.integration.Tests.STUDY_ID_1;
import static org.sagebionetworks.bridge.util.IntegTestUtils.TEST_APP_ID;

import java.util.Set;

import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sagebionetworks.bridge.rest.api.AssessmentsApi;
import org.sagebionetworks.bridge.rest.api.ForConsentedUsersApi;
import org.sagebionetworks.bridge.rest.api.ForDevelopersApi;
import org.sagebionetworks.bridge.rest.api.SchedulesV2Api;
import org.sagebionetworks.bridge.rest.model.Assessment;
import org.sagebionetworks.bridge.rest.model.AssessmentReference2;
import org.sagebionetworks.bridge.rest.model.CustomActivityEventRequest;
import org.sagebionetworks.bridge.rest.model.EventStreamAdherenceReport;
import org.sagebionetworks.bridge.rest.model.EventStreamWindow;
import org.sagebionetworks.bridge.rest.model.Schedule2;
import org.sagebionetworks.bridge.rest.model.Session;
import org.sagebionetworks.bridge.rest.model.SessionCompletionState;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.rest.model.StudyActivityEventRequest;
import org.sagebionetworks.bridge.rest.model.TimeWindow;
import org.sagebionetworks.bridge.user.TestUserHelper;
import org.sagebionetworks.bridge.user.TestUserHelper.TestUser;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class EventStreamAdherenceReportTest {
    
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
        participant = TestUserHelper.createAndSignInUser(EventStreamAdherenceReportTest.class, true);
        
        ForConsentedUsersApi userApi = participant.getClient(ForConsentedUsersApi.class);
        EventStreamAdherenceReport report = userApi.getUsersStudyParticipantEventStreamAdherenceReport(STUDY_ID_1, null, null).execute().body();
        
        // Everything is, at this point, in compliance
        assertEquals(Integer.valueOf(100), report.getAdherencePercent());
        assertEquals(ImmutableSet.of(NOT_APPLICABLE), getStates(report, null));
        
        StudyActivityEventRequest request = new StudyActivityEventRequest()
                .eventId(FAKE_ENROLLMENT).timestamp(DateTime.now());
        userApi.createStudyActivityEvent(STUDY_ID_1, request, true, false).execute();
        
        report = userApi.getUsersStudyParticipantEventStreamAdherenceReport(STUDY_ID_1, 
                DateTime.now(), null).execute().body();
        assertEquals(report.getStreams().size(), 2);
        assertEquals(ImmutableSet.of("0", "7", "14", "21"), report.getStreams().get(0).getByDayEntries().keySet());
        assertEquals(ImmutableSet.of("2", "5", "8", "11", "14", "17", "20"),
                report.getStreams().get(1).getByDayEntries().keySet());
        
        assertEquals(Integer.valueOf(100), report.getAdherencePercent());
        assertEquals(ImmutableSet.of(NOT_APPLICABLE), getStates(report, CLINIC_VISIT));
        assertEquals(ImmutableSet.of(NOT_YET_AVAILABLE), getStates(report, FAKE_ENROLLMENT));

        // This is in the far future when everything will have been expired
        report = userApi.getUsersStudyParticipantEventStreamAdherenceReport(STUDY_ID_1, 
                DateTime.now().plusYears(2), null).execute().body();
        assertEquals(Integer.valueOf(0), report.getAdherencePercent());
        assertEquals(ImmutableSet.of(NOT_APPLICABLE), getStates(report, CLINIC_VISIT));
        assertEquals(ImmutableSet.of(EXPIRED), getStates(report, FAKE_ENROLLMENT));
        
        // Now lets' step into the schedule...
        report = userApi.getUsersStudyParticipantEventStreamAdherenceReport(STUDY_ID_1, 
                DateTime.now().plusDays(11), null).execute().body();
        assertEquals(ImmutableSet.of(EXPIRED, UNSTARTED, NOT_YET_AVAILABLE), getStates(report, FAKE_ENROLLMENT));
    }
    
    private Set<SessionCompletionState> getStates(EventStreamAdherenceReport report, String eventId) {
        return report.getStreams().stream()
            .filter(stream -> eventId == null || stream.getStartEventId().equals("custom:"+eventId))
            .flatMap(stream -> stream.getByDayEntries().values().stream())
            .flatMap(list -> list.stream())
            .flatMap(day -> day.getTimeWindows().stream())
            .map(EventStreamWindow::getState)
            .collect(toSet());
    }

    private AssessmentReference2 asmtToReference(Assessment asmt) {
        return new AssessmentReference2()
                .appId(TEST_APP_ID)
                .identifier(asmt.getIdentifier())
                .guid(asmt.getGuid());
    }
}
