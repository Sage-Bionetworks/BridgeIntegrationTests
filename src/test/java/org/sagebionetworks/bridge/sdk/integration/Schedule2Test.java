package org.sagebionetworks.bridge.sdk.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.sagebionetworks.bridge.rest.model.ActivityEventUpdateType.MUTABLE;
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

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.http.HttpResponse;
import org.apache.http.client.fluent.Request;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sagebionetworks.bridge.rest.api.AssessmentsApi;
import org.sagebionetworks.bridge.rest.api.ForAdminsApi;
import org.sagebionetworks.bridge.rest.api.ForConsentedUsersApi;
import org.sagebionetworks.bridge.rest.api.ForStudyCoordinatorsApi;
import org.sagebionetworks.bridge.rest.api.ForWorkersApi;
import org.sagebionetworks.bridge.rest.api.OrganizationsApi;
import org.sagebionetworks.bridge.rest.api.SchedulesV2Api;
import org.sagebionetworks.bridge.rest.api.StudiesApi;
import org.sagebionetworks.bridge.rest.api.StudyParticipantsApi;
import org.sagebionetworks.bridge.rest.exceptions.EntityNotFoundException;
import org.sagebionetworks.bridge.rest.exceptions.InvalidEntityException;
import org.sagebionetworks.bridge.rest.exceptions.UnauthorizedException;
import org.sagebionetworks.bridge.rest.model.Assessment;
import org.sagebionetworks.bridge.rest.model.AssessmentInfo;
import org.sagebionetworks.bridge.rest.model.AssessmentReference2;
import org.sagebionetworks.bridge.rest.model.ColorScheme;
import org.sagebionetworks.bridge.rest.model.ImageResource;
import org.sagebionetworks.bridge.rest.model.Label;
import org.sagebionetworks.bridge.rest.model.Notification;
import org.sagebionetworks.bridge.rest.model.NotificationInfo;
import org.sagebionetworks.bridge.rest.model.NotificationMessage;
import org.sagebionetworks.bridge.rest.model.ParticipantSchedule;
import org.sagebionetworks.bridge.rest.model.PerformanceOrder;
import org.sagebionetworks.bridge.rest.model.Schedule2;
import org.sagebionetworks.bridge.rest.model.ScheduledAssessment;
import org.sagebionetworks.bridge.rest.model.ScheduledSession;
import org.sagebionetworks.bridge.rest.model.Session;
import org.sagebionetworks.bridge.rest.model.SessionInfo;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.rest.model.StudyActivityEvent;
import org.sagebionetworks.bridge.rest.model.StudyBurst;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;
import org.sagebionetworks.bridge.rest.model.TimeWindow;
import org.sagebionetworks.bridge.rest.model.Timeline;
import org.sagebionetworks.bridge.rest.model.TimelineMetadata;
import org.sagebionetworks.bridge.user.TestUser;
import org.sagebionetworks.bridge.user.TestUserHelper;

import com.google.common.collect.ImmutableList;
import com.google.common.net.HttpHeaders;

import retrofit2.Response;

public class Schedule2Test {

    private static final String TIME_ZONE = "America/Chicago";
    private static final String PARTICIPANT_API = "/v5/studies/study1/participants/self/schedule?clientTimeZone=";
    private static final String IMAGE_RESOURCE_NAME = "default";
    private static final String IMAGE_RESOURCE_MODULE = "sage_survey";
    private static final String IMAGE_RESOURCE_LABEL_LANG = "en";
    private static final String IMAGE_RESOURCE_LABEL_VALUE = "english label for image";

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
                .identifier(Tests.randomIdentifier(getClass()));
        
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
        if (schedule != null && schedule.getGuid() != null) {
            admin.getClient(ForAdminsApi.class).deleteSchedule(schedule.getGuid()).execute();
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
        
        StudyBurst burst = new StudyBurst()
                .identifier("burst1")
                .originEventId("timeline_retrieved")
                .interval("P1W")
                .occurrences(3)
                .updateType(MUTABLE);
        schedule.setStudyBursts(ImmutableList.of(burst));
        
        // add a session
        Session session = new Session();
        session.setName("Simple repeating assessment");
        session.setSymbol("✯");
        session.addLabelsItem(new Label().lang("en").value("Take the assessment"));
        session.addStartEventIdsItem("enrollment");
        session.addStudyBurstIdsItem("burst1");
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
        assertEquals(Integer.valueOf(120), timeline.getTotalMinutes()); // 30 + (30 * 3 study burst scheduled sessions)
        assertEquals(Integer.valueOf(12), timeline.getTotalNotifications()); // 3 + (3 * 3 study burst notifications)
        
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
        assertEquals("✯", sessionInfo.getSymbol());
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
        
        List<String> eventIds = ImmutableList.of("enrollment", "study_burst:burst1:01",
                "study_burst:burst1:02", "study_burst:burst1:03");
        
        for (ScheduledSession scheduledSession : timeline.getSchedule()) {
            assertEquals(timeWindowGuid, scheduledSession.getTimeWindowGuid());
            assertTrue(eventIds.contains(scheduledSession.getStartEventId()));
            
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
        
        // A worker can retrieve timeline data
        TestUser admin = TestUserHelper.getSignedInAdmin();
        ForWorkersApi workerApi = admin.getClient(ForWorkersApi.class);
        ScheduledSession schSession = timeline.getSchedule().get(0);
        sessionInfo = timeline.getSessions().stream()
                .filter(s -> s.getGuid().equals(schSession.getRefGuid())).findFirst().get();
        
        String instanceGuid = schSession.getInstanceGuid();
        TimelineMetadata metadata = workerApi.getTimelineMetadata(admin.getAppId(), instanceGuid).execute().body();
        Map<String,String> map = metadata.getMetadata();
        assertEquals(schSession.getStartDay().toString(), map.get("sessionInstanceStartDay"));
        assertEquals(schSession.getEndDay().toString(), map.get("sessionInstanceEndDay"));
        assertEquals(schSession.getStartEventId(), map.get("sessionStartEventId"));
        assertEquals(schSession.getTimeWindowGuid(), map.get("timeWindowGuid"));
        assertNull(map.get("timeWindowPersistent"));
        assertNull(map.get("schedulePublished"));
        assertEquals(schedule.getGuid(), map.get("scheduleGuid"));
        assertEquals(schedule.getModifiedOn().toString(), map.get("scheduleModifiedOn"));
        assertEquals(schSession.getInstanceGuid(), map.get("sessionInstanceGuid"));
        assertEquals(sessionInfo.getGuid(), map.get("sessionGuid"));
        
        // physically delete it
        admin.getClient(SchedulesV2Api.class).deleteSchedule(schedule.getGuid()).execute();
        schedule = null;

        // Now there is no metadata (retrieval of timeline metadata is
        ForWorkersApi workersApi = admin.getClient(ForWorkersApi.class);
        String anInstanceGuid = timeline.getSchedule().get(0).getInstanceGuid();
        metadata = workersApi.getTimelineMetadata(admin.getAppId(), anInstanceGuid)
                .execute().body();
        assertTrue(metadata.getMetadata().isEmpty());

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
        Schedule2 newSchedule = schedulesApi.getScheduleForStudy(STUDY_ID_2).execute().body();
        assertEquals(ORG_ID_2, newSchedule.getOwnerId());
        
        adminOrgApi.removeMember(ORG_ID_2, studyDesigner.getUserId()).execute();
        adminOrgApi.addMember(Tests.ORG_ID_1, studyDesigner.getUserId()).execute();

        try {
            schedulesApi.getScheduleForStudy(STUDY_ID_2).execute().body();
            fail("Should have thrown exception");
        } catch(UnauthorizedException e) {
        }
        newSchedule = schedulesApi.getScheduleForStudy(STUDY_ID_1).execute().body();
        assertEquals(ORG_ID_1, newSchedule.getOwnerId());
        
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
        schedule = null;
        
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
    
    @Test
    public void getParticipantScheduleCachesTimeZoneAppropriately() throws IOException {
        studyCoordinator = TestUserHelper.createAndSignInUser(Schedule2Test.class, false, STUDY_COORDINATOR);
        SchedulesV2Api schedulesApi = studyDesigner.getClient(SchedulesV2Api.class);
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
        
        user = TestUserHelper.createAndSignInUser(Schedule2Test.class, true);
        
        // set timezone, get back 200
        ForConsentedUsersApi usersApi = user.getClient(ForConsentedUsersApi.class);
        Response<ParticipantSchedule> res = usersApi.getParticipantScheduleForSelf(STUDY_ID_1, "America/Chicago").execute();
        String etag = res.headers().get(HttpHeaders.ETAG);
        assertNotNull(etag);
        
        // request again, get 304
        HttpResponse noModResponse = Request.Get(user.getClientManager().getHostUrl() + PARTICIPANT_API + "America/Chicago")
                .setHeader("Bridge-Session", user.getSession().getSessionToken())
                .setHeader(HttpHeaders.IF_NONE_MATCH, etag).execute().returnResponse();
        assertEquals(304, noModResponse.getStatusLine().getStatusCode());

        // change timezone get 200
        HttpResponse modResponse = Request.Get(user.getClientManager().getHostUrl() + PARTICIPANT_API + "America/Los_Angeles")
                .setHeader("Bridge-Session", user.getSession().getSessionToken())
                .setHeader(HttpHeaders.IF_NONE_MATCH, etag).execute().returnResponse();
        assertEquals(200, modResponse.getStatusLine().getStatusCode());
        String newEtag = modResponse.getFirstHeader(HttpHeaders.ETAG).getValue();
        
        // request again, get 304
        noModResponse = Request.Get(user.getClientManager().getHostUrl() + PARTICIPANT_API  + "America/Los_Angeles")
                .setHeader("Bridge-Session", user.getSession().getSessionToken())
                .setHeader(HttpHeaders.IF_NONE_MATCH, newEtag).execute().returnResponse();
        assertEquals(304, noModResponse.getStatusLine().getStatusCode());        
        
        // delete timezone
        StudyParticipantsApi participantsApi = studyCoordinator.getClient(StudyParticipantsApi.class);
        StudyParticipant participant = participantsApi.getStudyParticipantById(
                STUDY_ID_1, user.getUserId(), false).execute().body();
        assertEquals("America/Los_Angeles", participant.getClientTimeZone());
        participant.setClientTimeZone(null);
        participantsApi.updateStudyParticipant(STUDY_ID_1, user.getUserId(), participant).execute().body();
        
        participant = usersApi.getUsersParticipantRecord(false).execute().body();
        assertNull(participant.getClientTimeZone());
        
        // request again, get 200
        noModResponse = Request.Get(user.getClientManager().getHostUrl() + PARTICIPANT_API  + "America/Los_Angeles")
                .setHeader("Bridge-Session", user.getSession().getSessionToken())
                .setHeader(HttpHeaders.IF_NONE_MATCH, newEtag).execute().returnResponse();
        assertEquals(200, noModResponse.getStatusLine().getStatusCode());
    }
    
    @Test
    public void getParticipantScheduleForStudyParticipant() throws Exception {
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
        
        StudyParticipant participant = user.getClient(ForConsentedUsersApi.class).getUsersParticipantRecord(false).execute().body();
        participant.setClientTimeZone(TIME_ZONE);
        user.getClient(ForConsentedUsersApi.class).updateUsersParticipantRecord(participant).execute();

        // This user should now have a timeline via study1:
        ForStudyCoordinatorsApi coordsApi = studyCoordinator.getClient(ForStudyCoordinatorsApi.class);
        ParticipantSchedule participantSchedule = coordsApi.getParticipantSchedule(STUDY_ID_1, user.getUserId()).execute().body();
        
        // it's there
        assertEquals(7, participantSchedule.getSchedule().size());
        
        // Check the events (only enrollment is in the schedule)
        Map<String, DateTime> eventTimestamps = coordsApi.getStudyParticipantStudyActivityEvents(STUDY_ID_1, user.getUserId())
                .execute().body().getItems().stream()
                .collect(Collectors.toMap(StudyActivityEvent::getEventId, StudyActivityEvent::getTimestamp));
        
        assertEquals(1, participantSchedule.getEventTimestamps().size());
        for (Map.Entry<String, DateTime> entry : participantSchedule.getEventTimestamps().entrySet()) {
            assertEquals(entry.getValue(), eventTimestamps.get(entry.getKey()));
        }
        
        // This has to be requested twice, happen twice, because the first time, it's setting the timestamp for 
        // the timeline_retrieved event and that changes the etag when it is returned. It's only the second time 
        // that the etag will be stable (assuming no other event has happened). It's just a limitation of the 
        // caching at this point.
        ForConsentedUsersApi userApi = user.getClient(ForConsentedUsersApi.class);
        Response<ParticipantSchedule> response = userApi.getParticipantScheduleForSelf(
                STUDY_ID_1, TIME_ZONE).execute();
        participantSchedule = response.body();
        
        userApi = user.getClient(ForConsentedUsersApi.class);
        response = userApi.getParticipantScheduleForSelf(STUDY_ID_1, TIME_ZONE).execute();
        participantSchedule = response.body();
        
        HttpResponse noModResponse = Request.Get(user.getClientManager().getHostUrl() + PARTICIPANT_API + "America/Chicago")
                .setHeader("Bridge-Session", user.getSession().getSessionToken())
                .setHeader(HttpHeaders.IF_NONE_MATCH, response.headers().get(HttpHeaders.ETAG))
                .execute().returnResponse();
        assertEquals(304, noModResponse.getStatusLine().getStatusCode());        

        TestUser admin = TestUserHelper.getSignedInAdmin();
        admin.getClient(SchedulesV2Api.class).deleteSchedule(study.getScheduleGuid()).execute();
        schedule = null;
        
        // and this is just a flat-out error
        try {
            userApi.getParticipantScheduleForSelf(STUDY_ID_2, TIME_ZONE).execute();
        } catch(UnauthorizedException e) {
            assertEquals("Caller is not enrolled in study 'study2'", e.getMessage());
        }
        try {
            coordsApi.getParticipantSchedule(STUDY_ID_2, user.getUserId()).execute();
        } catch(EntityNotFoundException e) {
            assertEquals("Account not found.", e.getMessage());
        }
    }

    @Test
    public void assessmentReferenceImageResource() throws Exception {
        studyCoordinator = TestUserHelper.createAndSignInUser(Schedule2Test.class, false, STUDY_COORDINATOR);

        SchedulesV2Api schedulesApi = studyDesigner.getClient(SchedulesV2Api.class);

        // create AssessmentReference2s with different types of ImageResources
        // make identifier a sortable key for easy checking later
        AssessmentReference2 nullImageResource = new AssessmentReference2()
                .appId(TEST_APP_ID)
                .guid(assessment.getGuid())
                .identifier("0")
                .imageResource(null);
        AssessmentReference2 moduleAndLabel = new AssessmentReference2()
                .appId(TEST_APP_ID)
                .guid(assessment.getGuid())
                .identifier("1")
                .imageResource(new ImageResource().name(IMAGE_RESOURCE_NAME).module(IMAGE_RESOURCE_MODULE)
                        .label(new Label().lang(IMAGE_RESOURCE_LABEL_LANG).value(IMAGE_RESOURCE_LABEL_VALUE)));
        AssessmentReference2 nullModule = new AssessmentReference2()
                .appId(TEST_APP_ID)
                .guid(assessment.getGuid())
                .identifier("2")
                .imageResource(new ImageResource().name(IMAGE_RESOURCE_NAME).module(null)
                        .label(new Label().lang(IMAGE_RESOURCE_LABEL_LANG).value(IMAGE_RESOURCE_LABEL_VALUE)));
        AssessmentReference2 nullLabel = new AssessmentReference2()
                .appId(TEST_APP_ID)
                .guid(assessment.getGuid())
                .identifier("3")
                .imageResource(new ImageResource().name(IMAGE_RESOURCE_NAME).module(IMAGE_RESOURCE_MODULE).label(null));
        AssessmentReference2 nullModuleAndLabel = new AssessmentReference2()
                .appId(TEST_APP_ID)
                .guid(assessment.getGuid())
                .identifier("4")
                .imageResource(new ImageResource().name(IMAGE_RESOURCE_NAME).module(null).label(null));

        schedule = new Schedule2();
        schedule.setName("Test Schedule [Schedule2Test]");
        schedule.setDuration("P1W");
        Session session = new Session();
        session.setName("Simple repeating assessment");
        session.setAssessments(
                ImmutableList.of(nullImageResource, moduleAndLabel, nullModule, nullLabel, nullModuleAndLabel));
        session.addStartEventIdsItem("enrollment");
        session.setPerformanceOrder(SEQUENTIAL);
        session.addTimeWindowsItem(new TimeWindow().startTime("08:00").expiration("PT1H"));
        schedule.addSessionsItem(session);

        schedule = schedulesApi.saveScheduleForStudy(STUDY_ID_1, schedule).execute().body();
        user = TestUserHelper.createAndSignInUser(Schedule2Test.class, true);

        // check ImageResources in ParticipantSchedule
        ForStudyCoordinatorsApi coordsApi = studyCoordinator.getClient(ForStudyCoordinatorsApi.class);
        ParticipantSchedule participantSchedule = coordsApi.getParticipantSchedule(STUDY_ID_1, user.getUserId())
                .execute().body();
        Collections.sort(participantSchedule.getAssessments(),
                Comparator.comparing(assessment -> assessment.getIdentifier()));

        assertEquals(5, participantSchedule.getAssessments().size());
        assertNull(participantSchedule.getAssessments().get(0).getImageResource());
        assertImageResource(
                participantSchedule.getAssessments().get(1).getImageResource(),
                IMAGE_RESOURCE_NAME,
                IMAGE_RESOURCE_MODULE,
                new Label().lang(IMAGE_RESOURCE_LABEL_LANG).value(IMAGE_RESOURCE_LABEL_VALUE));
        assertImageResource(
                participantSchedule.getAssessments().get(2).getImageResource(),
                IMAGE_RESOURCE_NAME,
                null,
                new Label().lang(IMAGE_RESOURCE_LABEL_LANG).value(IMAGE_RESOURCE_LABEL_VALUE));
        assertImageResource(
                participantSchedule.getAssessments().get(3).getImageResource(),
                IMAGE_RESOURCE_NAME,
                IMAGE_RESOURCE_MODULE,
                null);
        assertImageResource(
                participantSchedule.getAssessments().get(4).getImageResource(),
                IMAGE_RESOURCE_NAME,
                null,
                null);

        // check ImageResources in Timeline
        Timeline timeline = coordsApi.getStudyParticipantTimeline(STUDY_ID_1, user.getUserId()).execute().body();
        Collections.sort(timeline.getAssessments(), Comparator.comparing(assessment -> assessment.getIdentifier()));

        assertEquals(5, timeline.getAssessments().size());
        assertNull(timeline.getAssessments().get(0).getImageResource());
        assertImageResource(
                timeline.getAssessments().get(1).getImageResource(),
                IMAGE_RESOURCE_NAME,
                IMAGE_RESOURCE_MODULE,
                new Label().lang(IMAGE_RESOURCE_LABEL_LANG).value(IMAGE_RESOURCE_LABEL_VALUE));
        assertImageResource(
                timeline.getAssessments().get(2).getImageResource(),
                IMAGE_RESOURCE_NAME,
                null,
                new Label().lang(IMAGE_RESOURCE_LABEL_LANG).value(IMAGE_RESOURCE_LABEL_VALUE));
        assertImageResource(
                timeline.getAssessments().get(3).getImageResource(),
                IMAGE_RESOURCE_NAME,
                IMAGE_RESOURCE_MODULE,
                null);
        assertImageResource(
                timeline.getAssessments().get(4).getImageResource(),
                IMAGE_RESOURCE_NAME,
                null,
                null);
    }

    public static void assertImageResource(ImageResource imageResource, String expectedName, String expectedModule,
            Label expectedLabel) {
        assertNotNull(imageResource);
        assertEquals(expectedName, imageResource.getName());
        assertEquals(expectedModule, imageResource.getModule());
        if (expectedLabel == null) {
            assertNull(imageResource.getLabel());
        } else {
            assertEquals(expectedLabel.getLang(), imageResource.getLabel().getLang());
            assertEquals(expectedLabel.getValue(), imageResource.getLabel().getValue());
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
        
        StudyBurst burst = schedule.getStudyBursts().get(0);
        assertEquals("burst1", burst.getIdentifier());
        assertEquals("timeline_retrieved", burst.getOriginEventId());
        assertEquals("P1W", burst.getInterval());
        assertEquals(Integer.valueOf(3), burst.getOccurrences());
        assertEquals(MUTABLE, burst.getUpdateType());
                
        Session session = schedule.getSessions().get(0);
        assertEquals("Simple repeating assessment", session.getName());
        assertEquals("✯", session.getSymbol());
        assertEquals(1, session.getLabels().size());
        assertEquals("en", session.getLabels().get(0).getLang());
        assertEquals("Take the assessment", session.getLabels().get(0).getValue());
        assertEquals("enrollment", session.getStartEventIds().get(0));
        assertEquals("burst1", session.getStudyBurstIds().get(0));
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
