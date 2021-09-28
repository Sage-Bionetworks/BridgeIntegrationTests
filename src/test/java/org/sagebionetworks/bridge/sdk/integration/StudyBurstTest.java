package org.sagebionetworks.bridge.sdk.integration;

import static org.joda.time.DateTimeZone.UTC;
import static org.junit.Assert.*;
import static org.sagebionetworks.bridge.rest.model.ActivityEventUpdateType.IMMUTABLE;
import static org.sagebionetworks.bridge.rest.model.ActivityEventUpdateType.MUTABLE;
import static org.sagebionetworks.bridge.rest.model.PerformanceOrder.SEQUENTIAL;
import static org.sagebionetworks.bridge.rest.model.Role.STUDY_DESIGNER;
import static org.sagebionetworks.bridge.sdk.integration.Tests.STUDY_ID_1;
import static org.sagebionetworks.bridge.util.IntegTestUtils.SAGE_ID;
import static org.sagebionetworks.bridge.util.IntegTestUtils.TEST_APP_ID;

import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sagebionetworks.bridge.rest.api.ForConsentedUsersApi;
import org.sagebionetworks.bridge.rest.api.ForStudyDesignersApi;
import org.sagebionetworks.bridge.rest.api.SchedulesV2Api;
import org.sagebionetworks.bridge.rest.exceptions.EntityNotFoundException;
import org.sagebionetworks.bridge.rest.model.ActivityEventUpdateType;
import org.sagebionetworks.bridge.rest.model.Assessment;
import org.sagebionetworks.bridge.rest.model.AssessmentReference2;
import org.sagebionetworks.bridge.rest.model.Label;
import org.sagebionetworks.bridge.rest.model.Schedule2;
import org.sagebionetworks.bridge.rest.model.Session;
import org.sagebionetworks.bridge.rest.model.StudyActivityEvent;
import org.sagebionetworks.bridge.rest.model.StudyActivityEventList;
import org.sagebionetworks.bridge.rest.model.StudyActivityEventRequest;
import org.sagebionetworks.bridge.rest.model.StudyBurst;
import org.sagebionetworks.bridge.rest.model.TimeWindow;
import org.sagebionetworks.bridge.user.TestUserHelper;
import org.sagebionetworks.bridge.user.TestUserHelper.TestUser;

import com.google.common.collect.ImmutableList;

/**
 * Test the whole thing round trip.
 */
public class StudyBurstTest {
    
    private TestUser user;
    private TestUser studyDesigner;
    private Schedule2 schedule;
    private Assessment assessment;
    
    private SchedulesV2Api designerSchedulesApi;
    private ForConsentedUsersApi usersApi;
    
    @Before
    public void before() throws Exception { 
        user = TestUserHelper.createAndSignInUser(StudyBurstTest.class, true);
        studyDesigner = TestUserHelper.createAndSignInUser(StudyBurstTest.class, false, STUDY_DESIGNER);
        
        usersApi = user.getClient(ForConsentedUsersApi.class);
        
        assessment = new Assessment().title(StudyBurstTest.class.getSimpleName()).osName("Universal").ownerId(SAGE_ID)
                .identifier(Tests.randomIdentifier(StudyBurstTest.class));
        
        assessment = studyDesigner.getClient(ForStudyDesignersApi.class)
                .createAssessment(assessment).execute().body();
    }
    
    @After
    public void after() throws Exception {
        TestUser admin = TestUserHelper.getSignedInAdmin();
        if (schedule != null && schedule.getGuid() != null) {
            admin.getClient(SchedulesV2Api.class)
                .deleteSchedule(schedule.getGuid()).execute();
        }
        if (assessment != null && assessment.getGuid() != null) {
            admin.getClient(ForStudyDesignersApi.class)
                .deleteAssessment(assessment.getGuid(), true).execute();
        }
        if (user != null) {
            user.signOutAndDeleteUser();
        }
        if (studyDesigner != null) {
            studyDesigner.signOutAndDeleteUser();
        }
    }
    
    private void setupSchedule(String originEventId, ActivityEventUpdateType burstUpdateType)
            throws Exception {
        // clean up any schedule that is there
        
        try {
            TestUser admin = TestUserHelper.getSignedInAdmin();
            schedule = admin.getClient(SchedulesV2Api.class).getScheduleForStudy(STUDY_ID_1).execute().body();
            admin.getClient(SchedulesV2Api.class).deleteSchedule(schedule.getGuid()).execute();
        } catch(EntityNotFoundException e) {
            
        }
        // Create a schedule that has study burst based on mutable event (event1 in study1).
        
        designerSchedulesApi = studyDesigner.getClient(SchedulesV2Api.class);
        schedule = new Schedule2();
        schedule.setName("Test Schedule [StudyBurstTest]");
        schedule.setDuration("P10D");
        
        StudyBurst burst = new StudyBurst()
                .identifier("burst1")
                .originEventId(originEventId)
                .interval("P1D")
                .occurrences(4)
                .updateType(burstUpdateType);
        schedule.setStudyBursts(ImmutableList.of(burst));
        
        // add a session
        Session session = new Session();
        session.setName("Simple assessment");
        session.addLabelsItem(new Label().lang("en").value("Take the assessment"));
        session.addStartEventIdsItem("timeline_retrieved");
        session.addStudyBurstIdsItem("burst1");
        session.setPerformanceOrder(SEQUENTIAL);
        
        AssessmentReference2 ref = new AssessmentReference2()
                .guid(assessment.getGuid())
                .appId(TEST_APP_ID)
                .revision(5)
                .addLabelsItem(new Label().lang("en").value("Test Value"))
                .minutesToComplete(10)
                .title("A title")
                .identifier(assessment.getIdentifier());
        session.addAssessmentsItem(ref);
        session.addTimeWindowsItem(new TimeWindow().startTime("08:00").expiration("PT3H"));
        schedule.addSessionsItem(session);

        schedule = designerSchedulesApi.saveScheduleForStudy(STUDY_ID_1, schedule).execute().body();
    }
    
    @Test
    public void mutableOriginEventMutableStudyBurst() throws Exception {
        setupSchedule(/* MUTABLE */ "custom:event1", MUTABLE);
        
        // Now submit that event
        DateTime timestamp1 = DateTime.now(UTC);
        createOrUpdateEvent("custom:event1", timestamp1);
        // Verify the follow-on events were created
        verifyTimestampsStartFrom("custom:event1", timestamp1, timestamp1);
        // Try changing one of these events, it works
        createOrUpdateEvent("study_burst:burst1:02", timestamp1.plusDays(10));
        assertEventTimestamp("study_burst:burst1:02", timestamp1.plusDays(10));
        // Try deleting one of these study burst events, it works
        assertEventTimestampDelete("study_burst:burst1:02", true);
        
        // Update the original mutable event
        DateTime timestamp2 = DateTime.now(UTC).plusDays(1);
        createOrUpdateEvent("custom:event1", timestamp2);
        // All the study burst events should be changed in line with the new timestamp
        verifyTimestampsStartFrom("custom:event1", timestamp2, timestamp2);
    }
    
    @Test
    public void mutableOriginEventImmutableStudyBurst() throws Exception {
        setupSchedule(/* MUTABLE */ "custom:event1", IMMUTABLE);
        
        // Now submit that event
        DateTime timestamp1 = DateTime.now(UTC);
        createOrUpdateEvent("custom:event1", timestamp1);
        // Verify the follow-on events were created
        verifyTimestampsStartFrom("custom:event1", timestamp1, timestamp1);
        // Try changing one of these events, it does not work
        createOrUpdateEvent("study_burst:burst1:02", timestamp1.plusDays(10));
        assertEventTimestamp("study_burst:burst1:02", timestamp1.plusDays(2)); // NOT CHANGED
        // Try deleting one of these study burst events, it does not work
        assertEventTimestampDelete("study_burst:burst1:02", false);
        
        // Update the original mutable event
        DateTime timestamp2 = DateTime.now(UTC).plusDays(1);
        createOrUpdateEvent("custom:event1", timestamp2);
        // All the study burst events should be unchanged, because they are immutable
        verifyTimestampsStartFrom("custom:event1", timestamp2, timestamp1);
    }
    
    @Test
    public void immutableOriginEventImmutableStudyBurst() throws Exception {
        setupSchedule(/* IMMUTABLE */ "custom:event2", IMMUTABLE);
        
        // Now submit that event
        DateTime timestamp1 = DateTime.now(UTC);
        createOrUpdateEvent("custom:event2", timestamp1);
        // Verify the follow-on events were created
        verifyTimestampsStartFrom("custom:event2", timestamp1, timestamp1);
        // Try changing one of these events, it doesn't work
        createOrUpdateEvent("study_burst:burst1:02", timestamp1.plusDays(10));
        assertEventTimestamp("study_burst:burst1:02", timestamp1.plusDays(2));
        // Try deleting one of these study burst events, it doesn't work
        assertEventTimestampDelete("study_burst:burst1:02", false);
        
        // Update the original mutable event
        DateTime timestamp2 = DateTime.now(UTC).plusDays(1);
        createOrUpdateEvent("custom:event2", timestamp2);
        // All the study burst events should be unchanged
        verifyTimestampsStartFrom("custom:event2", timestamp1, timestamp1);
    }
    
    private void createOrUpdateEvent(String eventId, DateTime timestamp) throws Exception {
        StudyActivityEventRequest request = new StudyActivityEventRequest()
                .eventId(eventId).timestamp(timestamp);
        usersApi.createStudyActivityEvent(STUDY_ID_1, request).execute();
    }
    
    private void verifyTimestampsStartFrom(String eventId, DateTime eventTimestamp, DateTime timestamp) throws Exception {
        StudyActivityEventList events = usersApi.getStudyActivityEvents(STUDY_ID_1).execute().body();
        
        StudyActivityEvent event = findEventById(events, eventId);
        StudyActivityEvent burst1 = findEventById(events, "study_burst:burst1:01");
        StudyActivityEvent burst2 = findEventById(events, "study_burst:burst1:02");
        StudyActivityEvent burst3 = findEventById(events, "study_burst:burst1:03");
        StudyActivityEvent burst4 = findEventById(events, "study_burst:burst1:04");
        
        assertEquals(eventTimestamp, event.getTimestamp());
        assertEquals(timestamp.plusDays(1), burst1.getTimestamp());
        assertEquals(timestamp.plusDays(2), burst2.getTimestamp());
        assertEquals(timestamp.plusDays(3), burst3.getTimestamp());
        assertEquals(timestamp.plusDays(4), burst4.getTimestamp());
    }
    
    private void assertEventTimestamp(String eventId, DateTime timestamp) throws Exception {
        StudyActivityEventList events = usersApi.getStudyActivityEvents(STUDY_ID_1).execute().body();
        StudyActivityEvent event = findEventById(events, eventId);
        assertEquals(timestamp, event.getTimestamp());
    }
    
    private void assertEventTimestampDelete(String eventId, boolean shouldBeDeleted) throws Exception {
        usersApi.deleteStudyActivityEvent(STUDY_ID_1, eventId).execute();
        StudyActivityEventList events = usersApi.getStudyActivityEvents(STUDY_ID_1).execute().body();
        StudyActivityEvent event = findEventById(events, eventId);
        if (shouldBeDeleted) {
            assertNull(event);
        } else {
            assertNotNull(event);    
        }
    }

    private StudyActivityEvent findEventById(StudyActivityEventList list, String eventId) {
        for (StudyActivityEvent event : list.getItems()) {
            if (event.getEventId().equals(eventId)) {
                return event;
            }
        }
        return null;
    }

}
