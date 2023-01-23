package org.sagebionetworks.bridge.sdk.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.sagebionetworks.bridge.sdk.integration.Tests.STUDY_ID_1;
import static org.sagebionetworks.bridge.util.IntegTestUtils.TEST_APP_ID;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sagebionetworks.bridge.rest.api.AlertsApi;
import org.sagebionetworks.bridge.rest.api.ForConsentedUsersApi;
import org.sagebionetworks.bridge.rest.api.ForDevelopersApi;
import org.sagebionetworks.bridge.rest.api.ForWorkersApi;
import org.sagebionetworks.bridge.rest.api.SchedulesV2Api;
import org.sagebionetworks.bridge.rest.api.StudiesApi;
import org.sagebionetworks.bridge.rest.model.ActivityEventUpdateType;
import org.sagebionetworks.bridge.rest.model.AdherenceRecord;
import org.sagebionetworks.bridge.rest.model.AdherenceRecordUpdates;
import org.sagebionetworks.bridge.rest.model.Alert;
import org.sagebionetworks.bridge.rest.model.Alert.CategoryEnum;
import org.sagebionetworks.bridge.rest.model.AlertList;
import org.sagebionetworks.bridge.rest.model.Assessment;
import org.sagebionetworks.bridge.rest.model.AssessmentReference2;
import org.sagebionetworks.bridge.rest.model.Enrollment;
import org.sagebionetworks.bridge.rest.model.EventStreamWindow;
import org.sagebionetworks.bridge.rest.model.PerformanceOrder;
import org.sagebionetworks.bridge.rest.model.Role;
import org.sagebionetworks.bridge.rest.model.Schedule2;
import org.sagebionetworks.bridge.rest.model.Session;
import org.sagebionetworks.bridge.rest.model.StudyActivityEvent;
import org.sagebionetworks.bridge.rest.model.StudyActivityEventList;
import org.sagebionetworks.bridge.rest.model.StudyActivityEventRequest;
import org.sagebionetworks.bridge.rest.model.StudyBurst;
import org.sagebionetworks.bridge.rest.model.TimeWindow;
import org.sagebionetworks.bridge.rest.model.WeeklyAdherenceReport;
import org.sagebionetworks.bridge.user.TestUser;
import org.sagebionetworks.bridge.user.TestUserHelper;

import com.google.common.collect.ImmutableList;

public class AlertsTest {
    private static final String CUSTOM_EVENT = "custom:event1";

    private TestUser admin;
    private TestUser researcherStudy1;
    private TestUser worker;
    private TestUser user;
    private TestUser developer;

    private AlertsApi researcherAlertsApi;
    private ForConsentedUsersApi usersApi;
    private SchedulesV2Api schedulesApi;
    private ForWorkersApi workerApi;
    private ForDevelopersApi developersApi;

    private Assessment assessment;
    private Schedule2 schedule;

    @Before
    public void before() throws IOException {
        admin = TestUserHelper.getSignedInAdmin();
        researcherStudy1 = TestUserHelper.createAndSignInUser(AlertsTest.class, true, Role.RESEARCHER);
        worker = TestUserHelper.createAndSignInUser(AlertsTest.class, true, Role.WORKER);
        user = TestUserHelper.createAndSignInUser(AlertsTest.class, true);
        developer = TestUserHelper.createAndSignInUser(AlertsTest.class, true, Role.DEVELOPER, Role.STUDY_DESIGNER);

        researcherAlertsApi = researcherStudy1.getClient(AlertsApi.class);
        usersApi = user.getClient(ForConsentedUsersApi.class);
        schedulesApi = admin.getClient(SchedulesV2Api.class);
        workerApi = worker.getClient(ForWorkersApi.class);
        developersApi = developer.getClient(ForDevelopersApi.class);

        // create schedule
        StudyBurst burst = new StudyBurst()
                .identifier("burst1")
                .originEventId(CUSTOM_EVENT)
                .interval("P1D")
                .occurrences(4)
                .updateType(ActivityEventUpdateType.MUTABLE);

        assessment = new Assessment()
                .identifier(Tests.randomIdentifier(getClass()))
                .osName("Universal")
                .ownerId(developer.getSession().getOrgMembership())
                .title("Assessment A");
        assessment = developersApi.createAssessment(assessment).execute().body();
        AssessmentReference2 assessmentReference = new AssessmentReference2()
                .appId(TEST_APP_ID)
                .identifier(assessment.getIdentifier())
                .guid(assessment.getGuid());

        List<Session> sessions = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Session session = new Session()
                    .name("Session #1")
                    .addStartEventIdsItem("enrollment")
                    // .delay("P2D")
                    .interval("P3D")
                    .performanceOrder(PerformanceOrder.SEQUENTIAL)
                    .addAssessmentsItem(assessmentReference)
                    .addTimeWindowsItem(new TimeWindow()
                            .startTime("08:00").expiration("PT6H"));
            sessions.add(session);
        }

        schedule = new Schedule2()
                .name("AlertsTest Schedule")
                .duration("P7D")
                .sessions(sessions)
                .studyBursts(ImmutableList.of(burst));
        schedule = schedulesApi.saveScheduleForStudy(STUDY_ID_1, schedule).execute().body();

        // enroll user in study
        StudiesApi studiesApi = admin.getClient(StudiesApi.class);
        studiesApi.enrollParticipant(STUDY_ID_1, new Enrollment().userId(user.getUserId())).execute();
    }

    @After
    public void after() throws IOException {
        // delete schedule
        if (schedule != null && schedule.getGuid() != null) {
            schedulesApi.deleteSchedule(schedule.getGuid()).execute();
        }

        // delete assessment
        if (assessment != null) {
            developersApi.deleteAssessment(assessment.getGuid(), true);
        }

        // delete alerts
        deleteAlerts();

        // delete users
        researcherStudy1.signOutAndDeleteUser();
        worker.signOutAndDeleteUser();
        user.signOutAndDeleteUser();
    }

    private void deleteAlerts() throws IOException {
        AlertList alerts;
        do {
            alerts = researcherAlertsApi.getAlerts(STUDY_ID_1, 0, 100).execute().body();
            researcherAlertsApi
                    .deleteAlerts(STUDY_ID_1,
                            alerts.getItems().stream().map(alert -> alert.getId()).collect(Collectors.toList()))
                    .execute();
        } while (alerts.getItems().size() > 0);
    }

    @Test
    public void newEnrollment() throws IOException {
        // user has already been enrolled in study
        //
        // make sure there is an alert for their enrollment
        AlertList alerts = researcherAlertsApi.getAlerts(STUDY_ID_1, 0, 100).execute().body();
        Alert enrollmentAlert = assertOneMatchingAlert(alerts, CategoryEnum.NEW_ENROLLMENT, user.getUserId());

        // delete the enrollment alert
        researcherAlertsApi.deleteAlerts(STUDY_ID_1, ImmutableList.of(enrollmentAlert.getId())).execute();

        // make sure the enrollment alert no longer exists
        AlertList alertsAfterDelete = researcherAlertsApi.getAlerts(STUDY_ID_1, 0, 100).execute().body();
        assertNoMatchingAlerts(alertsAfterDelete, CategoryEnum.NEW_ENROLLMENT, user.getUserId());
    }

    @Test
    public void timelineAccessed() throws IOException {
        // access the timeline
        usersApi.createStudyActivityEvent(STUDY_ID_1, new StudyActivityEventRequest().eventId("fake_enrollment")
                .timestamp(DateTime.parse("2020-05-10T00:00:00.000Z")), true, null).execute();
        usersApi.getTimelineForSelf(STUDY_ID_1, null).execute();

        // make sure there is an alert for timeline access
        AlertList alerts = researcherAlertsApi.getAlerts(STUDY_ID_1, 0, 100).execute().body();
        Alert timelineAccessAlert = assertOneMatchingAlert(alerts, CategoryEnum.TIMELINE_ACCESSED,
                user.getUserId());

        // delete the timeline access alert
        researcherAlertsApi.deleteAlerts(STUDY_ID_1, ImmutableList.of(timelineAccessAlert.getId())).execute();

        // make sure the timeline access alert no longer exists
        AlertList alertsAfterDelete = researcherAlertsApi.getAlerts(STUDY_ID_1, 0, 100).execute().body();
        assertNoMatchingAlerts(alertsAfterDelete, CategoryEnum.TIMELINE_ACCESSED, user.getUserId());
    }

    @Test
    public void lowAdherence() throws IOException {
        // get initial report
        WeeklyAdherenceReport report = workerApi
                .getWeeklyAdherenceReportForWorker(admin.getAppId(), STUDY_ID_1, user.getUserId()).execute()
                .body();

        // that may have triggered an alert because adherence is technically 0% so
        // delete alerts
        deleteAlerts();

        // get enrollment event to be able to perform adherence updates
        StudyActivityEventList events = usersApi.getStudyActivityEvents(STUDY_ID_1).execute().body();
        StudyActivityEvent enrollmentEvent = events.getItems().stream()
                .filter(event -> event.getEventId().equals("enrollment"))
                .findFirst().get();

        // complete 6 out of 10 sessions (60% adherence)
        List<AdherenceRecord> records = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            EventStreamWindow eventStreamWindow = report.getByDayEntries().get("0").get(i).getTimeWindows().get(0);
            AdherenceRecord adherenceRecord = new AdherenceRecord()
                    .instanceGuid(eventStreamWindow.getSessionInstanceGuid())
                    .eventTimestamp(enrollmentEvent.getTimestamp())
                    .startedOn(DateTime.now())
                    .finishedOn(DateTime.now());
            records.add(adherenceRecord);
        }
        AdherenceRecordUpdates updates = new AdherenceRecordUpdates().records(records);
        usersApi.updateAdherenceRecords(STUDY_ID_1, updates).execute();

        // get weekly adherence report (should trigger adherence alert because adherence
        // is <=60%)
        report = workerApi
                .getWeeklyAdherenceReportForWorker(admin.getAppId(), STUDY_ID_1, user.getUserId()).execute()
                .body();
        assertEquals(Integer.valueOf(60), report.getWeeklyAdherencePercent());

        // make sure there is an alert for low adherence
        AlertList alerts = researcherAlertsApi.getAlerts(STUDY_ID_1, 0, 100).execute().body();
        Alert lowAdherenceAlert = assertOneMatchingAlert(alerts, CategoryEnum.LOW_ADHERENCE, user.getUserId());

        // delete the alert
        researcherAlertsApi.deleteAlerts(STUDY_ID_1, ImmutableList.of(lowAdherenceAlert.getId())).execute();

        // make sure there are no alerts for low adherence
        AlertList alertsAfterDelete = researcherAlertsApi.getAlerts(STUDY_ID_1, 0, 100).execute().body();
        assertNoMatchingAlerts(alertsAfterDelete, CategoryEnum.LOW_ADHERENCE, user.getUserId());

        // submit another adherence record, adherence should now be 70%
        EventStreamWindow eventStreamWindow = report.getByDayEntries().get("0").get(6).getTimeWindows().get(0);
        AdherenceRecord adherenceRecord = new AdherenceRecord()
                .instanceGuid(eventStreamWindow.getSessionInstanceGuid())
                .eventTimestamp(enrollmentEvent.getTimestamp())
                .startedOn(DateTime.now())
                .finishedOn(DateTime.now());
        updates = new AdherenceRecordUpdates().records(ImmutableList.of(adherenceRecord));
        usersApi.updateAdherenceRecords(STUDY_ID_1, updates).execute();

        // get weekly adherence report (should NOT trigger adherence alert because
        // adherence is now 70% which is >60%)
        report = workerApi
                .getWeeklyAdherenceReportForWorker(admin.getAppId(), STUDY_ID_1, user.getUserId()).execute()
                .body();
        assertEquals(Integer.valueOf(70), report.getWeeklyAdherencePercent());

        // make sure there are no alerts for low adherence
        AlertList alertsAfterGoodAdherence = researcherAlertsApi.getAlerts(STUDY_ID_1, 0, 100).execute().body();
        assertNoMatchingAlerts(alertsAfterGoodAdherence, CategoryEnum.LOW_ADHERENCE, user.getUserId());
    }

    @Test
    public void studyBurstChange() throws IOException {
        // update study burst event
        StudyActivityEventRequest request = new StudyActivityEventRequest().clientTimeZone("America/Los_Angeles")
                .eventId(CUSTOM_EVENT).timestamp(DateTime.now());
        usersApi.createStudyActivityEvent(STUDY_ID_1, request, true, true).execute();

        // make sure there is an alert for study burst change
        AlertList alerts = researcherAlertsApi.getAlerts(STUDY_ID_1, 0, 100).execute().body();
        Alert studyBurstChangeAlert = assertOneMatchingAlert(alerts, CategoryEnum.STUDY_BURST_CHANGE, user.getUserId());

        // delete the alert
        researcherAlertsApi.deleteAlerts(STUDY_ID_1, ImmutableList.of(studyBurstChangeAlert.getId())).execute();

        // make sure there are no alerts for study burst change
        AlertList alertsAfterDelete = researcherAlertsApi.getAlerts(STUDY_ID_1, 0, 100).execute().body();
        assertNoMatchingAlerts(alertsAfterDelete, CategoryEnum.STUDY_BURST_CHANGE, user.getUserId());

        // update study burst event with updateBursts = false
        request = new StudyActivityEventRequest().clientTimeZone("America/Los_Angeles")
                .eventId(CUSTOM_EVENT).timestamp(DateTime.now());
        usersApi.createStudyActivityEvent(STUDY_ID_1, request, true, false).execute();

        // no study burst alerts should exist
        AlertList alertsAfterEvent = researcherAlertsApi.getAlerts(STUDY_ID_1, 0, 100).execute().body();
        assertNoMatchingAlerts(alertsAfterEvent, CategoryEnum.STUDY_BURST_CHANGE, user.getUserId());
    }

    private Alert assertOneMatchingAlert(AlertList alerts, CategoryEnum category, String userId) {
        List<Alert> foundAlerts = alerts.getItems().stream().filter(
                alert -> alert.getCategory() == category
                        && alert.getParticipant().getIdentifier().equals(userId))
                .collect(Collectors.toList());
        assertEquals(1, foundAlerts.size());
        return foundAlerts.get(0);
    }

    private void assertNoMatchingAlerts(AlertList alerts, CategoryEnum category, String userId) {
        assertTrue(alerts.getItems().stream()
                .noneMatch(alert -> alert.getCategory() == category
                        && alert.getParticipant().getIdentifier().equals(userId)));
    }
}
