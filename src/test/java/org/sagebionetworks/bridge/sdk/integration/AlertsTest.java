package org.sagebionetworks.bridge.sdk.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.sagebionetworks.bridge.sdk.integration.Tests.PASSWORD;
import static org.sagebionetworks.bridge.sdk.integration.Tests.STUDY_ID_1;
import static org.sagebionetworks.bridge.util.IntegTestUtils.TEST_APP_ID;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sagebionetworks.bridge.rest.ApiClientProvider;
import org.sagebionetworks.bridge.rest.api.AlertsApi;
import org.sagebionetworks.bridge.rest.api.AuthenticationApi;
import org.sagebionetworks.bridge.rest.api.ConsentsApi;
import org.sagebionetworks.bridge.rest.api.ForAdminsApi;
import org.sagebionetworks.bridge.rest.api.ForConsentedUsersApi;
import org.sagebionetworks.bridge.rest.api.ForDevelopersApi;
import org.sagebionetworks.bridge.rest.api.ForWorkersApi;
import org.sagebionetworks.bridge.rest.api.SchedulesV2Api;
import org.sagebionetworks.bridge.rest.api.StudiesApi;
import org.sagebionetworks.bridge.rest.exceptions.ConsentRequiredException;
import org.sagebionetworks.bridge.rest.exceptions.EntityNotFoundException;
import org.sagebionetworks.bridge.rest.model.ActivityEventUpdateType;
import org.sagebionetworks.bridge.rest.model.AdherenceRecord;
import org.sagebionetworks.bridge.rest.model.AdherenceRecordUpdates;
import org.sagebionetworks.bridge.rest.model.Alert;
import org.sagebionetworks.bridge.rest.model.Alert.CategoryEnum;
import org.sagebionetworks.bridge.rest.model.AlertCategoriesAndCounts;
import org.sagebionetworks.bridge.rest.model.AlertCategoryAndCount;
import org.sagebionetworks.bridge.rest.model.AlertFilter;
import org.sagebionetworks.bridge.rest.model.AlertFilter.AlertCategoriesEnum;
import org.sagebionetworks.bridge.rest.model.AlertIdCollection;
import org.sagebionetworks.bridge.rest.model.AlertList;
import org.sagebionetworks.bridge.rest.model.Assessment;
import org.sagebionetworks.bridge.rest.model.AssessmentReference2;
import org.sagebionetworks.bridge.rest.model.ConsentSignature;
import org.sagebionetworks.bridge.rest.model.EventStreamWindow;
import org.sagebionetworks.bridge.rest.model.PerformanceOrder;
import org.sagebionetworks.bridge.rest.model.Role;
import org.sagebionetworks.bridge.rest.model.Schedule2;
import org.sagebionetworks.bridge.rest.model.Session;
import org.sagebionetworks.bridge.rest.model.SharingScope;
import org.sagebionetworks.bridge.rest.model.SignIn;
import org.sagebionetworks.bridge.rest.model.SignUp;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.rest.model.StudyActivityEvent;
import org.sagebionetworks.bridge.rest.model.StudyActivityEventList;
import org.sagebionetworks.bridge.rest.model.StudyActivityEventRequest;
import org.sagebionetworks.bridge.rest.model.StudyBurst;
import org.sagebionetworks.bridge.rest.model.TimeWindow;
import org.sagebionetworks.bridge.rest.model.WeeklyAdherenceReport;
import org.sagebionetworks.bridge.rest.model.Withdrawal;
import org.sagebionetworks.bridge.user.TestUser;
import org.sagebionetworks.bridge.user.TestUserHelper;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class AlertsTest {
    private static final String CUSTOM_EVENT = "custom:event1";
    private static final String EXTERNAL_ID = "external-id";

    private TestUser admin;
    private TestUser researcher;
    private TestUser worker;
    private TestUser user;
    private TestUser user2;
    private TestUser user3;
    private TestUser developer;
    private String externalUserId;

    private ForAdminsApi adminsApi;
    private AlertsApi researcherAlertsApi;
    private ForConsentedUsersApi usersApi;
    private SchedulesV2Api schedulesApi;
    private ForWorkersApi workerApi;
    private ForDevelopersApi developersApi;
    private StudiesApi studiesApi;

    private Assessment assessment;
    private Schedule2 schedule;

    @Before
    public void before() throws IOException {
        admin = TestUserHelper.getSignedInAdmin();
        researcher = TestUserHelper.createAndSignInUser(AlertsTest.class, true, Role.RESEARCHER);
        worker = TestUserHelper.createAndSignInUser(AlertsTest.class, true, Role.WORKER);
        user = TestUserHelper.createAndSignInUser(AlertsTest.class, true);
        developer = TestUserHelper.createAndSignInUser(AlertsTest.class, true, Role.DEVELOPER, Role.STUDY_DESIGNER);

        adminsApi = admin.getClient(ForAdminsApi.class);
        researcherAlertsApi = researcher.getClient(AlertsApi.class);
        usersApi = user.getClient(ForConsentedUsersApi.class);
        schedulesApi = admin.getClient(SchedulesV2Api.class);
        workerApi = worker.getClient(ForWorkersApi.class);
        developersApi = developer.getClient(ForDevelopersApi.class);
        studiesApi = admin.getClient(StudiesApi.class);

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
                    .name("Session #" + i)
                    .addStartEventIdsItem("enrollment")
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
    }

    @SuppressWarnings("deprecation")
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

        // delete external id
        try {
            adminsApi.deleteExternalId(EXTERNAL_ID).execute();
        } catch (EntityNotFoundException e) {
        }

        // delete users
        researcher.signOutAndDeleteUser();
        worker.signOutAndDeleteUser();
        user.signOutAndDeleteUser();
        if (user2 != null) {
            user2.signOutAndDeleteUser();
        }
        if (user3 != null) {
            user3.signOutAndDeleteUser();
        }
        if (externalUserId != null) {
            adminsApi.deleteUser(externalUserId).execute();
        }
        developer.signOutAndDeleteUser();
    }

    private void deleteAlerts() throws IOException {
        AlertList alerts;
        do {
            alerts = researcherAlertsApi
                    .getAlerts(STUDY_ID_1, new AlertFilter().alertCategories(ImmutableList.of()), 0, 100).execute()
                    .body();
            researcherAlertsApi
                    .deleteAlerts(STUDY_ID_1,
                            new AlertIdCollection().alertIds(
                                    alerts.getItems().stream().map(alert -> alert.getId())
                                            .collect(Collectors.toList())))
                    .execute();
        } while (alerts.getItems().size() > 0);
    }

    @Test
    public void newEnrollment() throws IOException {
        // user was already been enrolled in study when account was created
        //
        // make sure there is an alert for their enrollment
        AlertList alerts = researcherAlertsApi
                .getAlerts(STUDY_ID_1, new AlertFilter().alertCategories(ImmutableList.of()), 0, 100).execute().body();
        Alert enrollmentAlert = assertOneMatchingAlert(alerts, CategoryEnum.NEW_ENROLLMENT, user.getUserId());

        // delete the enrollment alert
        researcherAlertsApi
                .deleteAlerts(STUDY_ID_1, new AlertIdCollection().alertIds(ImmutableList.of(enrollmentAlert.getId())))
                .execute();

        // make sure the enrollment alert no longer exists
        AlertList alertsAfterDelete = researcherAlertsApi
                .getAlerts(STUDY_ID_1, new AlertFilter().alertCategories(ImmutableList.of()), 0, 100).execute().body();
        assertNoMatchingAlerts(alertsAfterDelete, CategoryEnum.NEW_ENROLLMENT, user.getUserId());
    }

    @Test
    public void newEnrollment_externalIdSignUp() throws IOException {
        ApiClientProvider unauthenticatedProvider = Tests.getUnauthenticatedClientProvider(admin.getClientManager(),
                TEST_APP_ID);
        AuthenticationApi authApi = unauthenticatedProvider.getAuthenticationApi();

        // creates enrollments then creates the account
        SignUp signUp = new SignUp().appId(TEST_APP_ID).addDataGroupsItem("test_user")
                .putExternalIdsItem(STUDY_ID_1, EXTERNAL_ID).password(PASSWORD);
        authApi.signUp(signUp).execute();

        // sign in
        SignIn signIn = new SignIn().appId(TEST_APP_ID).externalId(EXTERNAL_ID).password(PASSWORD);
        try {
            authApi.signInV4(signIn).execute();
            fail("signIn should have thrown ConsentRequiredException");
        } catch (ConsentRequiredException ex) {
            externalUserId = ex.getSession().getId();
        }
        assertNotNull(externalUserId);

        // make sure there is a new enrollment alert
        AlertList alerts = researcherAlertsApi
                .getAlerts(STUDY_ID_1, new AlertFilter().alertCategories(ImmutableList.of()), 0, 100).execute().body();
        assertOneMatchingAlert(alerts, CategoryEnum.NEW_ENROLLMENT, externalUserId);

        // consent
        ApiClientProvider.AuthenticatedClientProvider authenticatedProvider = unauthenticatedProvider
                .getAuthenticatedClientProviderBuilder().withExternalId(EXTERNAL_ID).withPassword(PASSWORD).build();
        ConsentSignature signature = new ConsentSignature().name("Eggplant McTester")
                .birthdate(LocalDate.parse("1970-04-04"))
                .scope(SharingScope.NO_SHARING);
        authenticatedProvider.getClient(ConsentsApi.class).createConsentSignature(TEST_APP_ID, signature).execute();

        TestUser testUser = TestUserHelper.getSignedInUser(signIn);
        ForConsentedUsersApi consentedUsersApi = testUser.getClient(ForConsentedUsersApi.class);

        // withdraw
        consentedUsersApi.withdrawFromApp(new Withdrawal().reason("withdrawal reason")).execute();

        // alert should be deleted after withdrawal
        AlertList alertsAfterWithdrawal = researcherAlertsApi
                .getAlerts(STUDY_ID_1, new AlertFilter().alertCategories(ImmutableList.of()), 0, 100).execute().body();
        assertNoMatchingAlerts(alertsAfterWithdrawal, CategoryEnum.NEW_ENROLLMENT, externalUserId);
    }

    @Test
    public void newEnrollment_updateAccount() throws IOException {
        ApiClientProvider unauthenticatedProvider = Tests.getUnauthenticatedClientProvider(admin.getClientManager(),
                TEST_APP_ID);
        AuthenticationApi authApi = unauthenticatedProvider.getAuthenticationApi();

        // sign up using externalId field instead of external id map
        SignUp signUp = new SignUp().appId(TEST_APP_ID).addDataGroupsItem("test_user")
                .externalId(EXTERNAL_ID).password(PASSWORD);
        authApi.signUp(signUp).execute();

        // sign in
        SignIn signIn = new SignIn().appId(TEST_APP_ID).externalId(EXTERNAL_ID).password(PASSWORD);
        try {
            authApi.signInV4(signIn).execute();
            fail("signIn should have thrown ConsentRequiredException");
        } catch (ConsentRequiredException ex) {
            externalUserId = ex.getSession().getId();
        }
        assertNotNull(externalUserId);

        // no alert yet
        AlertList alerts = researcherAlertsApi
                .getAlerts(STUDY_ID_1, new AlertFilter().alertCategories(ImmutableList.of()), 0, 100).execute().body();
        assertNoMatchingAlerts(alerts, CategoryEnum.NEW_ENROLLMENT, externalUserId);

        // consent, which calls AccountService.updateAccount to add the enrollment
        ApiClientProvider.AuthenticatedClientProvider authenticatedProvider = unauthenticatedProvider
                .getAuthenticatedClientProviderBuilder().withExternalId(EXTERNAL_ID).withPassword(PASSWORD).build();
        ConsentSignature signature = new ConsentSignature().name("Eggplant McTester")
                .birthdate(LocalDate.parse("1970-04-04"))
                .scope(SharingScope.NO_SHARING);
        authenticatedProvider.getClient(ConsentsApi.class).createConsentSignature(TEST_APP_ID, signature).execute();

        // there should be an alert now
        AlertList alertsAfterConsent = researcherAlertsApi
                .getAlerts(STUDY_ID_1, new AlertFilter().alertCategories(ImmutableList.of()), 0, 100).execute().body();
        assertOneMatchingAlert(alertsAfterConsent, CategoryEnum.NEW_ENROLLMENT, externalUserId);

        TestUser testUser = TestUserHelper.getSignedInUser(signIn);
        ForConsentedUsersApi consentedUsersApi = testUser.getClient(ForConsentedUsersApi.class);

        // withdraw consent
        consentedUsersApi.withdrawConsentFromSubpopulation(TEST_APP_ID, new Withdrawal().reason("withdrawal reason"))
                .execute();

        // alert should be deleted after withdrawal of consent
        AlertList alertsAfterWithdrawal = researcherAlertsApi
                .getAlerts(STUDY_ID_1, new AlertFilter().alertCategories(ImmutableList.of()), 0, 100).execute().body();
        assertNoMatchingAlerts(alertsAfterWithdrawal, CategoryEnum.NEW_ENROLLMENT, externalUserId);
    }

    @Test
    public void timelineAccessed() throws IOException {
        // access the timeline
        usersApi.getTimelineForSelf(STUDY_ID_1, null).execute();

        // make sure there is an alert for timeline access
        AlertList alerts = researcherAlertsApi
                .getAlerts(STUDY_ID_1, new AlertFilter().alertCategories(ImmutableList.of()), 0, 100).execute().body();
        Alert timelineAccessAlert = assertOneMatchingAlert(alerts, CategoryEnum.TIMELINE_ACCESSED,
                user.getUserId());

        // delete the timeline access alert
        researcherAlertsApi.deleteAlerts(STUDY_ID_1,
                new AlertIdCollection().alertIds(ImmutableList.of(timelineAccessAlert.getId()))).execute();

        // make sure the timeline access alert no longer exists
        AlertList alertsAfterDelete = researcherAlertsApi
                .getAlerts(STUDY_ID_1, new AlertFilter().alertCategories(ImmutableList.of()), 0, 100).execute().body();
        assertNoMatchingAlerts(alertsAfterDelete, CategoryEnum.TIMELINE_ACCESSED, user.getUserId());
    }

    @Test
    public void lowAdherence() throws IOException {
        // set adherence threshold
        Study study = studiesApi.getStudy(STUDY_ID_1).execute().body();
        study.setAdherenceThresholdPercentage(60);
        studiesApi.updateStudy(STUDY_ID_1, study).execute();

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
        AlertList alerts = researcherAlertsApi
                .getAlerts(STUDY_ID_1, new AlertFilter().alertCategories(ImmutableList.of()), 0, 100).execute().body();
        Alert lowAdherenceAlert = assertOneMatchingAlert(alerts, CategoryEnum.LOW_ADHERENCE, user.getUserId());

        // delete the alert
        researcherAlertsApi
                .deleteAlerts(STUDY_ID_1, new AlertIdCollection().alertIds(ImmutableList.of(lowAdherenceAlert.getId())))
                .execute();

        // make sure there are no alerts for low adherence
        AlertList alertsAfterDelete = researcherAlertsApi
                .getAlerts(STUDY_ID_1, new AlertFilter().alertCategories(ImmutableList.of()), 0, 100).execute().body();
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
        AlertList alertsAfterGoodAdherence = researcherAlertsApi
                .getAlerts(STUDY_ID_1, new AlertFilter().alertCategories(ImmutableList.of()), 0, 100).execute().body();
        assertNoMatchingAlerts(alertsAfterGoodAdherence, CategoryEnum.LOW_ADHERENCE, user.getUserId());
    }

    @Test
    public void studyBurstChange() throws IOException {
        // update study burst event
        StudyActivityEventRequest request = new StudyActivityEventRequest().clientTimeZone("America/Los_Angeles")
                .eventId(CUSTOM_EVENT).timestamp(DateTime.now());
        usersApi.createStudyActivityEvent(STUDY_ID_1, request, true, true).execute();

        // make sure there is an alert for study burst change
        AlertList alerts = researcherAlertsApi
                .getAlerts(STUDY_ID_1, new AlertFilter().alertCategories(ImmutableList.of()), 0, 100).execute().body();
        Alert studyBurstChangeAlert = assertOneMatchingAlert(alerts, CategoryEnum.STUDY_BURST_CHANGE, user.getUserId());

        // delete the alert
        researcherAlertsApi.deleteAlerts(STUDY_ID_1,
                new AlertIdCollection().alertIds(ImmutableList.of(studyBurstChangeAlert.getId()))).execute();

        // make sure there are no alerts for study burst change
        AlertList alertsAfterDelete = researcherAlertsApi
                .getAlerts(STUDY_ID_1, new AlertFilter().alertCategories(ImmutableList.of()), 0, 100).execute().body();
        assertNoMatchingAlerts(alertsAfterDelete, CategoryEnum.STUDY_BURST_CHANGE, user.getUserId());

        // update study burst event with updateBursts = false
        request = new StudyActivityEventRequest().clientTimeZone("America/Los_Angeles")
                .eventId(CUSTOM_EVENT).timestamp(DateTime.now());
        usersApi.createStudyActivityEvent(STUDY_ID_1, request, true, false).execute();

        // no study burst alerts should exist
        AlertList alertsAfterEvent = researcherAlertsApi
                .getAlerts(STUDY_ID_1, new AlertFilter().alertCategories(ImmutableList.of()), 0, 100).execute().body();
        assertNoMatchingAlerts(alertsAfterEvent, CategoryEnum.STUDY_BURST_CHANGE, user.getUserId());
    }

    @Test
    public void filtering() throws IOException {
        // clear existing alerts
        deleteAlerts();

        // new enrollment alert
        user2 = TestUserHelper.createAndSignInUser(AlertsTest.class, true);
        // timeline retrieved alert
        usersApi.getTimelineForSelf(STUDY_ID_1, null).execute();

        // no filters
        AlertList alerts = researcherAlertsApi
                .getAlerts(STUDY_ID_1, new AlertFilter().alertCategories(ImmutableList.of()), 0, 100).execute().body();
        assertEquals(2, alerts.getItems().size());
        assertOneMatchingAlert(alerts, Alert.CategoryEnum.NEW_ENROLLMENT, user2.getUserId());
        assertOneMatchingAlert(alerts, Alert.CategoryEnum.TIMELINE_ACCESSED, user.getUserId());

        // new enrollment only
        alerts = researcherAlertsApi
                .getAlerts(STUDY_ID_1,
                        new AlertFilter().alertCategories(ImmutableList.of(AlertCategoriesEnum.NEW_ENROLLMENT)), 0, 100)
                .execute().body();
        assertEquals(1, alerts.getItems().size());
        assertOneMatchingAlert(alerts, Alert.CategoryEnum.NEW_ENROLLMENT, user2.getUserId());

        // timeline retrieved only
        alerts = researcherAlertsApi
                .getAlerts(STUDY_ID_1,
                        new AlertFilter().alertCategories(ImmutableList.of(AlertCategoriesEnum.TIMELINE_ACCESSED)), 0,
                        100)
                .execute().body();
        assertEquals(1, alerts.getItems().size());
        assertOneMatchingAlert(alerts, Alert.CategoryEnum.TIMELINE_ACCESSED, user.getUserId());

        // both filters
        alerts = researcherAlertsApi
                .getAlerts(STUDY_ID_1, new AlertFilter().alertCategories(
                        ImmutableList.of(AlertCategoriesEnum.NEW_ENROLLMENT, AlertCategoriesEnum.TIMELINE_ACCESSED)), 0,
                        100)
                .execute().body();
        assertEquals(2, alerts.getItems().size());
        assertOneMatchingAlert(alerts, Alert.CategoryEnum.NEW_ENROLLMENT, user2.getUserId());
        assertOneMatchingAlert(alerts, Alert.CategoryEnum.TIMELINE_ACCESSED, user.getUserId());

        // duplicate filters are ignored
        alerts = researcherAlertsApi
                .getAlerts(STUDY_ID_1, new AlertFilter().alertCategories(
                        ImmutableList.of(
                                AlertCategoriesEnum.NEW_ENROLLMENT,
                                AlertCategoriesEnum.NEW_ENROLLMENT,
                                AlertCategoriesEnum.NEW_ENROLLMENT,
                                AlertCategoriesEnum.NEW_ENROLLMENT,
                                AlertCategoriesEnum.TIMELINE_ACCESSED)),
                        0,
                        100)
                .execute().body();
        assertEquals(2, alerts.getItems().size());
        assertOneMatchingAlert(alerts, Alert.CategoryEnum.NEW_ENROLLMENT, user2.getUserId());
        assertOneMatchingAlert(alerts, Alert.CategoryEnum.TIMELINE_ACCESSED, user.getUserId());
    }

    @Test
    public void readUnreadAndCategoryCounts() throws IOException {
        // clear existing alerts
        deleteAlerts();

        // new enrollment alert
        user3 = TestUserHelper.createAndSignInUser(AlertsTest.class, true);

        // check categories and counts
        assertCategoriesAndCounts(ImmutableMap.of(AlertCategoryAndCount.CategoryEnum.NEW_ENROLLMENT, 1));

        // new enrollment alert
        user2 = TestUserHelper.createAndSignInUser(AlertsTest.class, true);

        // check categories and counts
        assertCategoriesAndCounts(ImmutableMap.of(AlertCategoryAndCount.CategoryEnum.NEW_ENROLLMENT, 2));

        // trigger low adherence alert
        workerApi.getWeeklyAdherenceReportForWorker(admin.getAppId(), STUDY_ID_1, user3.getUserId()).execute();

        // verify 1 low adherence alert
        AlertList alerts = researcherAlertsApi
                .getAlerts(STUDY_ID_1, new AlertFilter().alertCategories(ImmutableList.of()), 0, 100).execute().body();
        Alert lowAdherenceAlert1 = assertOneMatchingAlert(alerts, CategoryEnum.LOW_ADHERENCE, user3.getUserId());
        // alert should be unread
        assertFalse(lowAdherenceAlert1.isRead());

        // check categories and counts
        assertCategoriesAndCounts(ImmutableMap.of(AlertCategoryAndCount.CategoryEnum.NEW_ENROLLMENT, 2,
                AlertCategoryAndCount.CategoryEnum.LOW_ADHERENCE, 1));

        // trigger low adherence alert again, should overwrite alert (testing
        // overwriting in unread state)
        workerApi.getWeeklyAdherenceReportForWorker(admin.getAppId(), STUDY_ID_1, user3.getUserId()).execute();

        // verify new adherence alert
        alerts = researcherAlertsApi
                .getAlerts(STUDY_ID_1, new AlertFilter().alertCategories(ImmutableList.of()), 0, 100).execute().body();
        Alert lowAdherenceAlert2 = assertOneMatchingAlert(alerts, CategoryEnum.LOW_ADHERENCE, user3.getUserId());
        // alert should be unread
        assertFalse(lowAdherenceAlert2.isRead());
        // alert should be newer
        assertTrue(lowAdherenceAlert2.getCreatedOn().isAfter(lowAdherenceAlert1.getCreatedOn()));

        // check categories and counts
        assertCategoriesAndCounts(ImmutableMap.of(AlertCategoryAndCount.CategoryEnum.NEW_ENROLLMENT, 2,
                AlertCategoryAndCount.CategoryEnum.LOW_ADHERENCE, 1));

        // mark as read
        researcherAlertsApi.markAlertsRead(STUDY_ID_1,
                new AlertIdCollection().alertIds(ImmutableList.of(lowAdherenceAlert2.getId()))).execute();
        // make sure alert is marked as read but otherwise unchanged
        alerts = researcherAlertsApi
                .getAlerts(STUDY_ID_1, new AlertFilter().alertCategories(ImmutableList.of()), 0, 100).execute().body();
        Alert lowAdherenceAlert2Read = assertOneMatchingAlert(alerts, CategoryEnum.LOW_ADHERENCE, user3.getUserId());
        assertTrue(lowAdherenceAlert2Read.isRead());
        assertEquals(lowAdherenceAlert2.getId(), lowAdherenceAlert2Read.getId());
        assertEquals(lowAdherenceAlert2.getCategory(), lowAdherenceAlert2Read.getCategory());
        assertEquals(lowAdherenceAlert2.getParticipant().getIdentifier(),
                lowAdherenceAlert2Read.getParticipant().getIdentifier());
        assertEquals(lowAdherenceAlert2.getCreatedOn(), lowAdherenceAlert2Read.getCreatedOn());

        // check categories and counts
        assertCategoriesAndCounts(ImmutableMap.of(AlertCategoryAndCount.CategoryEnum.NEW_ENROLLMENT, 2,
                AlertCategoryAndCount.CategoryEnum.LOW_ADHERENCE, 1));

        // trigger low adherence alert again, should overwrite alert (testing
        // overwriting in read state)
        workerApi.getWeeklyAdherenceReportForWorker(admin.getAppId(), STUDY_ID_1, user3.getUserId()).execute();

        // verify new adherence alert
        alerts = researcherAlertsApi
                .getAlerts(STUDY_ID_1, new AlertFilter().alertCategories(ImmutableList.of()), 0, 100).execute().body();
        Alert lowAdherenceAlert3 = assertOneMatchingAlert(alerts, CategoryEnum.LOW_ADHERENCE, user3.getUserId());
        // alert should be unread
        assertFalse(lowAdherenceAlert3.isRead());
        // alert should be newer
        assertTrue(lowAdherenceAlert3.getCreatedOn().isAfter(lowAdherenceAlert2.getCreatedOn()));

        // check categories and counts
        assertCategoriesAndCounts(ImmutableMap.of(AlertCategoryAndCount.CategoryEnum.NEW_ENROLLMENT, 2,
                AlertCategoryAndCount.CategoryEnum.LOW_ADHERENCE, 1));

        // still need to test unread -> read -> unread
        // mark as read
        researcherAlertsApi.markAlertsRead(STUDY_ID_1,
                new AlertIdCollection().alertIds(ImmutableList.of(lowAdherenceAlert3.getId()))).execute();
        // make sure alert is marked as read but otherwise unchanged
        alerts = researcherAlertsApi
                .getAlerts(STUDY_ID_1, new AlertFilter().alertCategories(ImmutableList.of()), 0, 100).execute().body();
        Alert lowAdherenceAlert3Read = assertOneMatchingAlert(alerts, CategoryEnum.LOW_ADHERENCE, user3.getUserId());
        assertTrue(lowAdherenceAlert3Read.isRead());
        assertEquals(lowAdherenceAlert3.getId(), lowAdherenceAlert3Read.getId());
        assertEquals(lowAdherenceAlert3.getCategory(), lowAdherenceAlert3Read.getCategory());
        assertEquals(lowAdherenceAlert3.getParticipant().getIdentifier(),
                lowAdherenceAlert3Read.getParticipant().getIdentifier());
        assertEquals(lowAdherenceAlert3.getCreatedOn(), lowAdherenceAlert3Read.getCreatedOn());

        // check categories and counts
        assertCategoriesAndCounts(ImmutableMap.of(AlertCategoryAndCount.CategoryEnum.NEW_ENROLLMENT, 2,
                AlertCategoryAndCount.CategoryEnum.LOW_ADHERENCE, 1));

        // mark as unread
        researcherAlertsApi.markAlertsUnread(STUDY_ID_1,
                new AlertIdCollection().alertIds(ImmutableList.of(lowAdherenceAlert3.getId()))).execute();
        // make sure alert is marked as unread but otherwise unchanged
        alerts = researcherAlertsApi
                .getAlerts(STUDY_ID_1, new AlertFilter().alertCategories(ImmutableList.of()), 0, 100).execute().body();
        Alert lowAdherenceAlert3Unread = assertOneMatchingAlert(alerts, CategoryEnum.LOW_ADHERENCE, user3.getUserId());
        assertFalse(lowAdherenceAlert3Unread.isRead());
        assertEquals(lowAdherenceAlert3.getId(), lowAdherenceAlert3Unread.getId());
        assertEquals(lowAdherenceAlert3.getCategory(), lowAdherenceAlert3Unread.getCategory());
        assertEquals(lowAdherenceAlert3.getParticipant().getIdentifier(),
                lowAdherenceAlert3Unread.getParticipant().getIdentifier());
        assertEquals(lowAdherenceAlert3.getCreatedOn(), lowAdherenceAlert3Unread.getCreatedOn());

        // check categories and counts
        assertCategoriesAndCounts(ImmutableMap.of(AlertCategoryAndCount.CategoryEnum.NEW_ENROLLMENT, 2,
                AlertCategoryAndCount.CategoryEnum.LOW_ADHERENCE, 1));

        // delete low adherence alert
        researcherAlertsApi.deleteAlerts(STUDY_ID_1,
                new AlertIdCollection().alertIds(ImmutableList.of(lowAdherenceAlert3.getId()))).execute();

        // verify deletion
        alerts = researcherAlertsApi
                .getAlerts(STUDY_ID_1, new AlertFilter().alertCategories(ImmutableList.of()), 0, 100).execute().body();
        assertNoMatchingAlerts(alerts, CategoryEnum.LOW_ADHERENCE, user3.getUserId());

        // check categories and counts
        assertCategoriesAndCounts(ImmutableMap.of(AlertCategoryAndCount.CategoryEnum.NEW_ENROLLMENT, 2));
    }

    private void assertCategoriesAndCounts(Map<AlertCategoryAndCount.CategoryEnum, Integer> expectedCategoriesAndCounts)
            throws IOException {
        AlertCategoriesAndCounts alertCategoriesAndCounts = researcherAlertsApi.getAlertCategoriesAndCounts(STUDY_ID_1)
                .execute().body();
        assertEquals(expectedCategoriesAndCounts.size(), alertCategoriesAndCounts.getAlertCategoriesAndCounts().size());
        for (AlertCategoryAndCount categoryAndCount : alertCategoriesAndCounts.getAlertCategoriesAndCounts()) {
            assertTrue(expectedCategoriesAndCounts.containsKey(categoryAndCount.getCategory()));
            assertTrue(expectedCategoriesAndCounts.get(categoryAndCount.getCategory())
                    .equals(categoryAndCount.getCount()));
        }
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
