package org.sagebionetworks.bridge.sdk.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.sagebionetworks.bridge.rest.model.NotificationType.AFTER_WINDOW_START;
import static org.sagebionetworks.bridge.rest.model.PerformanceOrder.SEQUENTIAL;
import static org.sagebionetworks.bridge.rest.model.Role.DEVELOPER;
import static org.sagebionetworks.bridge.rest.model.Role.STUDY_COORDINATOR;
import static org.sagebionetworks.bridge.rest.model.Role.STUDY_DESIGNER;
import static org.sagebionetworks.bridge.sdk.integration.Tests.ORG_ID_1;
import static org.sagebionetworks.bridge.sdk.integration.Tests.ORG_ID_2;
import static org.sagebionetworks.bridge.sdk.integration.Tests.STUDY_ID_1;
import static org.sagebionetworks.bridge.sdk.integration.Tests.STUDY_ID_2;
import static org.sagebionetworks.bridge.util.IntegTestUtils.SAGE_ID;
import static org.sagebionetworks.bridge.util.IntegTestUtils.TEST_APP_ID;

import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.sagebionetworks.bridge.rest.api.AssessmentsApi;
import org.sagebionetworks.bridge.rest.api.ForConsentedUsersApi;
import org.sagebionetworks.bridge.rest.api.ForStudyCoordinatorsApi;
import org.sagebionetworks.bridge.rest.api.OrganizationsApi;
import org.sagebionetworks.bridge.rest.api.SchedulesV2Api;
import org.sagebionetworks.bridge.rest.api.StudiesApi;
import org.sagebionetworks.bridge.rest.exceptions.EntityNotFoundException;
import org.sagebionetworks.bridge.rest.exceptions.InvalidEntityException;
import org.sagebionetworks.bridge.rest.exceptions.UnauthorizedException;
import org.sagebionetworks.bridge.rest.model.Assessment;
import org.sagebionetworks.bridge.rest.model.AssessmentInfo;
import org.sagebionetworks.bridge.rest.model.AssessmentReference2;
import org.sagebionetworks.bridge.rest.model.ColorScheme;
import org.sagebionetworks.bridge.rest.model.Label;
import org.sagebionetworks.bridge.rest.model.Notification;
import org.sagebionetworks.bridge.rest.model.NotificationInfo;
import org.sagebionetworks.bridge.rest.model.NotificationMessage;
import org.sagebionetworks.bridge.rest.model.PerformanceOrder;
import org.sagebionetworks.bridge.rest.model.Schedule2;
import org.sagebionetworks.bridge.rest.model.ScheduledAssessment;
import org.sagebionetworks.bridge.rest.model.ScheduledSession;
import org.sagebionetworks.bridge.rest.model.Session;
import org.sagebionetworks.bridge.rest.model.SessionInfo;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.rest.model.TimeWindow;
import org.sagebionetworks.bridge.rest.model.Timeline;
import org.sagebionetworks.bridge.user.TestUserHelper;
import org.sagebionetworks.bridge.user.TestUserHelper.TestUser;

import retrofit2.Response;

public class Schedule2Test {

    TestUser developer;
    TestUser studyDesigner;
    TestUser studyCoordinator;
    TestUser user;
    Assessment assessment;
    Schedule2 schedule;
    
    String org1ScheduleGuid;
    String org2ScheduleGuid;
    
    @Before
    public void before() throws Exception {
        developer = TestUserHelper.createAndSignInUser(Schedule2Test.class, false, DEVELOPER);
        studyDesigner = TestUserHelper.createAndSignInUser(Schedule2Test.class, false, STUDY_DESIGNER);
        
        assessment = new Assessment().title(Schedule2Test.class.getSimpleName()).osName("Universal").ownerId(SAGE_ID)
                .identifier(Tests.randomIdentifier(Schedule2Test.class));
        
        assessment = developer.getClient(AssessmentsApi.class).createAssessment(assessment).execute().body();
        
        // If there's a schedule associated to study 1, we need to delete it.
        TestUser admin = TestUserHelper.getSignedInAdmin();
        StudiesApi studiesApi = admin.getClient(StudiesApi.class);
        Study study = studiesApi.getStudy(STUDY_ID_1).execute().body();
        if (study.getScheduleGuid() != null) {
            admin.getClient(SchedulesV2Api.class).deleteSchedule(study.getScheduleGuid()).execute();
        }
        study = studiesApi.getStudy(STUDY_ID_2).execute().body();
        if (study.getScheduleGuid() != null) {
            admin.getClient(SchedulesV2Api.class).deleteSchedule(study.getScheduleGuid()).execute();
        }
    }
    
    @After
    public void after() throws Exception {
        TestUser admin = TestUserHelper.getSignedInAdmin();
        SchedulesV2Api adminSchedulesApi = admin.getClient(SchedulesV2Api.class);
        
        if (org1ScheduleGuid != null) {
            adminSchedulesApi.deleteSchedule(org1ScheduleGuid).execute();
        }
        if (org2ScheduleGuid != null) {
            adminSchedulesApi.deleteSchedule(org2ScheduleGuid).execute();
        }
        if (assessment != null && assessment.getGuid() != null) {
            admin.getClient(AssessmentsApi.class).deleteAssessment(assessment.getGuid(), true).execute();
        }
        if (user != null) {
            user.signOutAndDeleteUser();
        }
        if (studyDesigner != null) {
            studyDesigner.signOutAndDeleteUser();
        }
        if (studyCoordinator != null) {
            studyCoordinator.signOutAndDeleteUser();
        }
        if (developer != null) {
            developer.signOutAndDeleteUser();
        }
    }

    @Test
    public void crudWorks() throws Exception {
        SchedulesV2Api schedulesApi = studyDesigner.getClient(SchedulesV2Api.class);
        // create schedule that is invalid, fails.
        
        schedule = new Schedule2();
        try {
            schedulesApi.saveScheduleForStudy(STUDY_ID_1, schedule).execute();
            fail("Should have thrown exception");
        } catch(InvalidEntityException e) {
        }
        
        schedule.setName("Test Schedule [Schedule2Test]");
        schedule.setDuration("P10W");
        // These will all be ignored
        schedule.setDeleted(true);
        schedule.setGuid("ABC");
        schedule.setOwnerId("somebody");
        schedule.setPublished(true);
        schedule.setVersion(10L);
        
        // create schedule.
        schedule = schedulesApi.saveScheduleForStudy(STUDY_ID_1, schedule).execute().body();
        assertEquals("Test Schedule [Schedule2Test]", schedule.getName());
        assertEquals("P10W", schedule.getDuration());
        assertFalse(schedule.isDeleted());
        assertFalse(schedule.isPublished());
        assertEquals(SAGE_ID, schedule.getOwnerId());
        assertNotEquals("ABC", schedule.getGuid());
        assertNotEquals(Long.valueOf(10), schedule.getVersion());
        
        // add a session
        Session session = new Session();
        session.setName("Simple repeating assessment");
        session.addLabelsItem(new Label().lang("en").value("Take the assessment"));
        session.addStartEventIdsItem("enrollment");
        session.setDelay("P1W");
        session.setInterval("P4W");
        session.setPerformanceOrder(SEQUENTIAL);
        
        ColorScheme colorScheme = new ColorScheme()
                .background("#111111")
                .foreground("#222222")
                .activated("#333333")
                .inactivated("#444444");
        
        AssessmentReference2 ref = new AssessmentReference2()
                .guid(assessment.getGuid())
                .appId(TEST_APP_ID)
                .colorScheme(colorScheme)
                .revision(5)
                .addLabelsItem(new Label().lang("en").value("Test Value"))
                .minutesToComplete(10)
                .title("A title")
                .identifier(assessment.getIdentifier());
        session.addAssessmentsItem(ref);
        session.addTimeWindowsItem(new TimeWindow().startTime("08:00").expiration("P3D"));
        Notification enNotification = new Notification()
                .notifyAt(AFTER_WINDOW_START)
                .offset("PT10M")
                .allowSnooze(true)
                .addMessagesItem(new NotificationMessage()
                        .lang("en")
                        .subject("subject")
                        .message("body"))
                .addMessagesItem(new NotificationMessage()
                        .lang("fr")
                        .subject("subject in French")
                        .message("body in French"));
        session.addNotificationsItem(enNotification);
        
        schedule.addSessionsItem(session);

        schedule = schedulesApi.saveScheduleForStudy(STUDY_ID_1, schedule).execute().body();
        assertSchedule(schedule);
        
        // get schedule
        schedule = schedulesApi.getScheduleForStudy(STUDY_ID_1).execute().body();
        assertSchedule(schedule);
        
        // update schedule, fails validation
        schedule.getSessions().get(0).setName("Updated name for session");
        schedule.getSessions().get(0).addLabelsItem(new Label().lang("ja").value("評価を受ける"));

        // update schedule, succeeds
        schedule = schedulesApi.saveScheduleForStudy(STUDY_ID_1, schedule).execute().body();
        assertEquals("Updated name for session", schedule.getSessions().get(0).getName());
        assertEquals("ja", schedule.getSessions().get(0).getLabels().get(1).getLang());
        assertEquals("評価を受ける", schedule.getSessions().get(0).getLabels().get(1).getValue());
        
        String timeWindowGuid = schedule.getSessions().get(0).getTimeWindows().get(0).getGuid();
        assertNotNull(timeWindowGuid);
        
        // You can retrieve the timeline for this schedule
        Timeline timeline = schedulesApi.getTimelineForStudy(STUDY_ID_1).execute().body();
        assertEquals(schedule.getDuration(), timeline.getDuration());
        assertFalse(timeline.getAssessments().isEmpty());
        assertFalse(timeline.getSessions().isEmpty());
        assertFalse(timeline.getSchedule().isEmpty());
        assertEquals(timeline.getTotalMinutes(), Integer.valueOf(30));
        assertEquals(timeline.getTotalNotifications(), Integer.valueOf(3));
        
        AssessmentInfo assessmentInfo = timeline.getAssessments().get(0);
        assertEquals(schedule.getSessions().get(0).getAssessments().get(0).getGuid(), assessmentInfo.getGuid());
        assertEquals("api", assessmentInfo.getAppId());
        assertEquals("Test Value", assessmentInfo.getLabel());
        assertEquals(Integer.valueOf(5), assessmentInfo.getRevision());
        assertEquals(Integer.valueOf(10), assessmentInfo.getMinutesToComplete());
        assertEquals("#222222", assessmentInfo.getColorScheme().getForeground());
        assertEquals("#111111", assessmentInfo.getColorScheme().getBackground());
        assertEquals("#333333", assessmentInfo.getColorScheme().getActivated());
        assertEquals("#444444", assessmentInfo.getColorScheme().getInactivated());
        String url = "/v1/assessments/" + assessmentInfo.getGuid() + "/config";
        assertTrue(assessmentInfo.getConfigUrl().contains(url));
        assertEquals(schedule.getSessions().get(0).getAssessments().get(0).getIdentifier(), assessmentInfo.getIdentifier());
        
        SessionInfo sessionInfo = timeline.getSessions().get(0);
        assertEquals(schedule.getSessions().get(0).getGuid(), sessionInfo.getGuid());
        assertEquals("Take the assessment", sessionInfo.getLabel());
        assertEquals(PerformanceOrder.SEQUENTIAL, sessionInfo.getPerformanceOrder());
        assertEquals(1, sessionInfo.getTimeWindowGuids().size());
        assertEquals(timeWindowGuid, sessionInfo.getTimeWindowGuids().get(0));
        assertEquals(Integer.valueOf(10), sessionInfo.getMinutesToComplete());
        
        NotificationInfo notificationInfo = timeline.getSessions().get(0).getNotifications().get(0);
        assertEquals(AFTER_WINDOW_START, notificationInfo.getNotifyAt());
        assertEquals("PT10M", notificationInfo.getOffset());
        assertTrue(notificationInfo.isAllowSnooze());
        assertEquals(notificationInfo.getMessage().getLang(), "en");
        assertEquals(notificationInfo.getMessage().getSubject(), "subject");
        assertEquals(notificationInfo.getMessage().getMessage(), "body");
        
        // the references in the timeline work...
        Set<String> sessionInstanceGuids = new HashSet<>();
        int scheduledSessionCount = 0;
        
        Set<String> asmtInstanceGuids = new HashSet<>();
        int scheduledAssessmentCount = 0;
        
        for (ScheduledSession scheduledSession : timeline.getSchedule()) {
            
            assertEquals(timeWindowGuid, scheduledSession.getTimeWindowGuid());
            assertEquals("enrollment", scheduledSession.getStartEventId());
            
            scheduledSessionCount++;
            sessionInstanceGuids.add(scheduledSession.getInstanceGuid());
            for (ScheduledAssessment schAssessment : scheduledSession.getAssessments()) {
                scheduledAssessmentCount++;
                asmtInstanceGuids.add(schAssessment.getInstanceGuid());
            }
        }
        assertEquals(scheduledSessionCount, sessionInstanceGuids.size());
        assertEquals(scheduledAssessmentCount, asmtInstanceGuids.size());
        
        // And, these values are identical between runs
        Timeline timeline2 = schedulesApi.getTimelineForStudy(STUDY_ID_1).execute().body();
        Set<String> sessionInstanceGuids2 = new HashSet<>();
        Set<String> asmtInstanceGuids2 = new HashSet<>();
        for (ScheduledSession scheduledSession : timeline2.getSchedule()) {
            sessionInstanceGuids2.add(scheduledSession.getInstanceGuid());
            for (ScheduledAssessment schAssessment : scheduledSession.getAssessments()) {
                asmtInstanceGuids2.add(schAssessment.getInstanceGuid());
            }
        }
        assertEquals(sessionInstanceGuids, sessionInstanceGuids2);
        assertEquals(asmtInstanceGuids, asmtInstanceGuids2);
        
        // physically delete it
        TestUser admin = TestUserHelper.getSignedInAdmin();
        admin.getClient(SchedulesV2Api.class).deleteSchedule(schedule.getGuid()).execute();
        
        try {
            schedulesApi.getScheduleForStudy(STUDY_ID_1).execute();
            fail("Should have thrown exception");
        } catch(EntityNotFoundException e) {
        }
        schedule = null;
    }
    
    @Test
    public void schedulesScopedToOrganization() throws Exception {
        TestUser admin = TestUserHelper.getSignedInAdmin();
        OrganizationsApi adminOrgApi = admin.getClient(OrganizationsApi.class);
        
        SchedulesV2Api schedulesApi = studyDesigner.getClient(SchedulesV2Api.class);
        
        adminOrgApi.removeMember(SAGE_ID, studyDesigner.getUserId()).execute();
        adminOrgApi.addMember(ORG_ID_1, studyDesigner.getUserId()).execute();
        
        schedule = new Schedule2();
        schedule.setName("ORG1: Test Schedule [Schedule2Test]");
        schedule.setDuration("P30D");
        org1ScheduleGuid = schedulesApi.saveScheduleForStudy(STUDY_ID_1, schedule).execute().body().getGuid();
        
        adminOrgApi.removeMember(ORG_ID_1, studyDesigner.getUserId()).execute();
        adminOrgApi.addMember(ORG_ID_2, studyDesigner.getUserId()).execute();
        
        schedule.setName("ORG2: Test Schedule [Schedule2Test]");
        org2ScheduleGuid = schedulesApi.saveScheduleForStudy(STUDY_ID_2, schedule).execute().body().getGuid();
        
        // Designer should not be able to see study schedule1
        try {
            schedulesApi.getScheduleForStudy(STUDY_ID_1).execute().body();
            fail("Should have thrown exception");
        } catch(UnauthorizedException e) {
        }
        Schedule2 schedule = schedulesApi.getScheduleForStudy(STUDY_ID_2).execute().body();
        assertEquals(ORG_ID_2, schedule.getOwnerId());
        
        adminOrgApi.removeMember(ORG_ID_2, studyDesigner.getUserId()).execute();
        adminOrgApi.addMember(Tests.ORG_ID_1, studyDesigner.getUserId()).execute();

        try {
            schedulesApi.getScheduleForStudy(STUDY_ID_2).execute().body();
            fail("Should have thrown exception");
        } catch(UnauthorizedException e) {
        }
        schedule = schedulesApi.getScheduleForStudy(STUDY_ID_1).execute().body();
        assertEquals(ORG_ID_1, schedule.getOwnerId());
        
        // Developers see everything
        SchedulesV2Api devSchedulesApi = developer.getClient(SchedulesV2Api.class);
        devSchedulesApi.getScheduleForStudy(STUDY_ID_1).execute().body();
        devSchedulesApi.getScheduleForStudy(STUDY_ID_2).execute().body();
    }
    
    @Test
    public void getTimelineForStudyParticipant() throws Exception {
        studyCoordinator = TestUserHelper.createAndSignInUser(Schedule2Test.class, false, STUDY_COORDINATOR);
        
        SchedulesV2Api schedulesApi = studyDesigner.getClient(SchedulesV2Api.class);
        StudiesApi studiesApi = studyDesigner.getClient(StudiesApi.class);
        
        AssessmentReference2 ref = new AssessmentReference2()
                .appId(TEST_APP_ID)
                .guid(assessment.getGuid())
                .identifier(assessment.getIdentifier());

        schedule = new Schedule2();
        schedule.setName("Test Schedule [Schedule2Test]");
        schedule.setDuration("P1W");
        Session session = new Session();
        session.setName("Simple repeating assessment");
        session.setInterval("P1D");
        session.setAssessments(null);
        session.addStartEventIdsItem("enrollment");
        session.setPerformanceOrder(SEQUENTIAL);
        session.addAssessmentsItem(ref);
        session.addTimeWindowsItem(new TimeWindow().startTime("08:00").expiration("PT1H"));
        schedule.addSessionsItem(session);
        
        // create schedule.
        schedule = schedulesApi.saveScheduleForStudy(STUDY_ID_1, schedule).execute().body();
        
        // Add it to study 1
        Study study = studiesApi.getStudy(STUDY_ID_1).execute().body();
        user = TestUserHelper.createAndSignInUser(Schedule2Test.class, true);

        // This user should now have a timeline via study1:
        ForStudyCoordinatorsApi coordsApi = studyCoordinator.getClient(ForStudyCoordinatorsApi.class);
        Timeline timeline = coordsApi.getStudyParticipantTimeline(STUDY_ID_1, user.getUserId()).execute().body();
        
        // it's there
        assertEquals(7, timeline.getSchedule().size());
        
        ForConsentedUsersApi userApi = user.getClient(ForConsentedUsersApi.class);
        timeline = userApi.getTimelineForSelf(STUDY_ID_1, null).execute().body();

        // it's there
        assertEquals(7, timeline.getSchedule().size());
        
        // Let's add the cache header and see what happens.
        Response<Timeline> res = userApi.getTimelineForSelf(STUDY_ID_1, schedule.getModifiedOn().plusHours(1)).execute();
        assertEquals(304, res.code());
        assertNull(res.body());

        res = userApi.getTimelineForSelf(STUDY_ID_1, schedule.getModifiedOn().minusHours(1)).execute();
        assertEquals(200, res.code());
        assertNotNull(res.body());
        
        TestUser admin = TestUserHelper.getSignedInAdmin();
        admin.getClient(SchedulesV2Api.class).deleteSchedule(study.getScheduleGuid()).execute();
        
        // and this is just a flat-out error
        try {
            userApi.getTimelineForSelf(STUDY_ID_2, null).execute();
        } catch(UnauthorizedException e) {
            assertEquals("Caller is not enrolled in study 'study2'", e.getMessage());
        }
        try {
            coordsApi.getStudyParticipantTimeline(STUDY_ID_2, user.getUserId()).execute();
        } catch(EntityNotFoundException e) {
            assertEquals("Account not found.", e.getMessage());
        }
    }
    
    private void assertSchedule(Schedule2 schedule) {
        assertEquals("Test Schedule [Schedule2Test]", schedule.getName());
        assertEquals("P10W", schedule.getDuration());
        assertFalse(schedule.isDeleted());
        assertFalse(schedule.isPublished());
        assertEquals(SAGE_ID, schedule.getOwnerId());
        assertNotEquals("ABC", schedule.getGuid());
        assertNotEquals(Long.valueOf(10), schedule.getVersion());
        
        Session session = schedule.getSessions().get(0);
        assertEquals("Simple repeating assessment", session.getName());
        assertEquals(1, session.getLabels().size());
        assertEquals("en", session.getLabels().get(0).getLang());
        assertEquals("Take the assessment", session.getLabels().get(0).getValue());
        assertEquals("enrollment", session.getStartEventIds().get(0));
        assertEquals(SEQUENTIAL, session.getPerformanceOrder());
        assertEquals("P1W", session.getDelay());
        assertEquals("P4W", session.getInterval());
        
        assertEquals(1, session.getAssessments().size());
        AssessmentReference2 ref = session.getAssessments().get(0);
        assertEquals(TEST_APP_ID, ref.getAppId());
        assertEquals(assessment.getGuid(), ref.getGuid());
        assertEquals(assessment.getIdentifier(), ref.getIdentifier());
        assertEquals("en", ref.getLabels().get(0).getLang());
        assertEquals("Test Value", ref.getLabels().get(0).getValue());
        assertEquals("#111111", ref.getColorScheme().getBackground());
        assertEquals("#222222", ref.getColorScheme().getForeground());
        assertEquals("#333333", ref.getColorScheme().getActivated());
        assertEquals("#444444", ref.getColorScheme().getInactivated());
        assertEquals(Integer.valueOf(10), ref.getMinutesToComplete());
        assertEquals("A title", ref.getTitle());
        
        assertEquals(1, session.getTimeWindows().size());
        TimeWindow window = session.getTimeWindows().get(0);
        assertEquals("08:00", window.getStartTime());
        assertEquals("P3D", window.getExpiration());
        
        Notification notification = session.getNotifications().get(0);
        assertEquals(AFTER_WINDOW_START, notification.getNotifyAt());
        assertEquals("PT10M", notification.getOffset());
        assertTrue(notification.isAllowSnooze());
        
        NotificationMessage msg1 = notification.getMessages().get(0);
        assertEquals("en", msg1.getLang());
        assertEquals("subject", msg1.getSubject());
        assertEquals("body", msg1.getMessage());
        
        NotificationMessage msg2 = notification.getMessages().get(1);
        assertEquals("fr", msg2.getLang());
        assertEquals("subject in French", msg2.getSubject());
        assertEquals("body in French", msg2.getMessage());
    }

}
