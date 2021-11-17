package org.sagebionetworks.bridge.sdk.integration;

import static java.util.stream.Collectors.toSet;
import static org.joda.time.DateTimeZone.UTC;
import static org.junit.Assert.*;
import static org.sagebionetworks.bridge.rest.model.ActivityEventUpdateType.IMMUTABLE;
import static org.sagebionetworks.bridge.rest.model.ActivityEventUpdateType.MUTABLE;
import static org.sagebionetworks.bridge.rest.model.PerformanceOrder.SEQUENTIAL;
import static org.sagebionetworks.bridge.rest.model.Role.STUDY_DESIGNER;
import static org.sagebionetworks.bridge.sdk.integration.Tests.STUDY_ID_1;
import static org.sagebionetworks.bridge.util.IntegTestUtils.SAGE_ID;
import static org.sagebionetworks.bridge.util.IntegTestUtils.TEST_APP_ID;

import java.util.Set;

import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sagebionetworks.bridge.rest.api.ForConsentedUsersApi;
import org.sagebionetworks.bridge.rest.api.ForStudyDesignersApi;
import org.sagebionetworks.bridge.rest.api.SchedulesV2Api;
import org.sagebionetworks.bridge.rest.exceptions.BadRequestException;
import org.sagebionetworks.bridge.rest.exceptions.EntityNotFoundException;
import org.sagebionetworks.bridge.rest.exceptions.InvalidEntityException;
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
    
    // These are established in the initializer for study 1.
    private static final String MUTABLE_EVENT = "custom:event1";
    private static final String IMMUTABLE_EVENT = "custom:event2";
    
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
    
    private void setupSchedule(String originEventId, ActivityEventUpdateType burstUpdateType, String delayPeriod)
            throws Exception {
        // clean up any schedule that is there
        try {
            TestUser admin = TestUserHelper.getSignedInAdmin();
            schedule = admin.getClient(SchedulesV2Api.class).getScheduleForStudy(STUDY_ID_1).execute().body();
            admin.getClient(SchedulesV2Api.class).deleteSchedule(schedule.getGuid()).execute();
        } catch(EntityNotFoundException e) {
            
        }

        designerSchedulesApi = studyDesigner.getClient(SchedulesV2Api.class);
        schedule = new Schedule2();
        schedule.setName("Test Schedule [StudyBurstTest]");
        schedule.setDuration("P10D");
        
        StudyBurst burst = new StudyBurst()
                .identifier("burst1")
                .originEventId(originEventId)
                .delay(delayPeriod)
                .interval("P1D")
                .occurrences(4)
                .updateType(burstUpdateType);
        schedule.setStudyBursts(ImmutableList.of(burst));
        
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
    public void studyBurstGeneratesEvents_noDelay() throws Exception {
        setupSchedule(MUTABLE_EVENT, MUTABLE, null);
        
        // This should trigger the study burst. We should see those events in the map.
        DateTime timestamp1 = DateTime.now(UTC);
        createOrUpdateEvent(MUTABLE_EVENT, timestamp1, null);

        StudyActivityEventList list = usersApi.getStudyActivityEvents(STUDY_ID_1).execute().body();

        StudyActivityEvent originEvent = findEventById(list, "custom:event1");
        assertNull(originEvent.getAnswerValue());
        assertNull(originEvent.getOriginEventId());
        assertNull(originEvent.getStudyBurstId());
        assertNull(originEvent.getPeriodFromOrigin());
        assertEquals(originEvent.getClientTimeZone(), "America/Los_Angeles");
        assertEquals(Integer.valueOf(1), originEvent.getRecordCount());
        assertEquals(timestamp1, originEvent.getTimestamp());
        
        StudyActivityEvent burst1 = findEventById(list, "study_burst:burst1:01");
        assertEquals("01", burst1.getAnswerValue());
        assertEquals("custom:event1", burst1.getOriginEventId());
        assertEquals("burst1", burst1.getStudyBurstId());
        assertNull(burst1.getPeriodFromOrigin());
        assertEquals(burst1.getClientTimeZone(), "America/Los_Angeles");
        assertEquals(Integer.valueOf(1), burst1.getRecordCount());
        assertEquals(timestamp1, burst1.getTimestamp());
        
        StudyActivityEvent burst2 = findEventById(list, "study_burst:burst1:02");
        assertEquals("02", burst2.getAnswerValue());
        assertEquals("custom:event1", burst2.getOriginEventId());
        assertEquals("burst1", burst2.getStudyBurstId());
        assertEquals("P1D", burst2.getPeriodFromOrigin());
        assertEquals(burst2.getClientTimeZone(), "America/Los_Angeles");
        assertEquals(Integer.valueOf(1), burst2.getRecordCount());
        assertEquals(timestamp1.plusDays(1), burst2.getTimestamp());
        
        StudyActivityEvent burst3 = findEventById(list, "study_burst:burst1:03");
        assertEquals("03", burst3.getAnswerValue());
        assertEquals("custom:event1", burst3.getOriginEventId());
        assertEquals("burst1", burst3.getStudyBurstId());
        assertEquals("P2D", burst3.getPeriodFromOrigin());
        assertEquals(burst3.getClientTimeZone(), "America/Los_Angeles");
        assertEquals(Integer.valueOf(1), burst3.getRecordCount());
        assertEquals(timestamp1.plusDays(2), burst3.getTimestamp());
        
        StudyActivityEvent burst4 = findEventById(list, "study_burst:burst1:04");
        assertEquals("04", burst4.getAnswerValue());
        assertEquals("custom:event1", burst4.getOriginEventId());
        assertEquals("burst1", burst4.getStudyBurstId());
        assertEquals("P3D", burst4.getPeriodFromOrigin());
        assertEquals(burst4.getClientTimeZone(), "America/Los_Angeles");
        assertEquals(Integer.valueOf(1), burst4.getRecordCount());
        assertEquals(timestamp1.plusDays(3), burst4.getTimestamp());
        
        // Verify the “always added” events for this user, who was enrolled.
        StudyActivityEvent en = findEventById(list, "enrollment");
        assertNotNull(en.getTimestamp());
        assertNotNull(en.getCreatedOn());
        assertEquals(Integer.valueOf(1), en.getRecordCount());
        
        StudyActivityEvent tr = findEventById(list, "timeline_retrieved");
        assertNotNull(tr.getTimestamp());
        assertNotNull(tr.getCreatedOn());
        assertEquals(Integer.valueOf(1), tr.getRecordCount());
        
        StudyActivityEvent co = findEventById(list, "created_on");
        assertEquals(co.getTimestamp(), user.getSession().getCreatedOn());
        assertNotNull(co.getCreatedOn());
        assertEquals(Integer.valueOf(1), co.getRecordCount());
    }
    
    @Test
    public void studyBurstGeneratesEvents_zeroDelay() throws Exception {
        try {
            setupSchedule(MUTABLE_EVENT, MUTABLE, "P0D");    
            fail("Should have thrown exception");
        } catch(InvalidEntityException e) {
            assertTrue(e.getMessage().contains("studyBursts[0].delay cannot be of no duration"));
        }
    }

    @Test
    public void mutableOriginEventMutableStudyBurst() throws Exception {
        setupSchedule(MUTABLE_EVENT, MUTABLE, "P1D");
        
        // Now submit that event
        DateTime timestamp1 = DateTime.now(UTC);
        createOrUpdateEvent(MUTABLE_EVENT, timestamp1, null);
        // Verify the follow-on events were created
        verifyTimestampsStartFrom(MUTABLE_EVENT, timestamp1, timestamp1);
        // Try changing one of these events, it works
        createOrUpdateEvent("study_burst:burst1:02", timestamp1.plusDays(10), null);
        assertEventTimestamp("study_burst:burst1:02", timestamp1.plusDays(10));
        // Try deleting one of these study burst events, it works
        assertEventTimestampDelete("study_burst:burst1:02", true);
        
        // Update the original mutable event
        DateTime timestamp2 = DateTime.now(UTC).plusDays(1);
        createOrUpdateEvent(MUTABLE_EVENT, timestamp2, null);
        // All the study burst events should be changed in line with the new timestamp
        verifyTimestampsStartFrom(MUTABLE_EVENT, timestamp2, timestamp2);
    }
    
    @Test
    public void mutableOriginEventImmutableStudyBurst() throws Exception {
        setupSchedule(MUTABLE_EVENT, IMMUTABLE, "P1D");
        
        // Now submit that event
        DateTime timestamp1 = DateTime.now(UTC);
        createOrUpdateEvent(MUTABLE_EVENT, timestamp1, null);
        // Verify the follow-on events were created
        verifyTimestampsStartFrom(MUTABLE_EVENT, timestamp1, timestamp1);
        // Try changing one of these events, it does not work
        failToCreateOrUpdateEvent("study_burst:burst1:02", timestamp1.plusDays(10));
        assertEventTimestamp("study_burst:burst1:02", timestamp1.plusDays(2)); // NOT CHANGED
        // Try deleting one of these study burst events, it does not work
        assertEventTimestampDelete("study_burst:burst1:02", false);
        
        // Update the original mutable event
        DateTime timestamp2 = DateTime.now(UTC).plusDays(1);
        failToCreateOrUpdateEvent(MUTABLE_EVENT, timestamp2);
        // All the study burst events should be unchanged, because they are immutable
        verifyTimestampsStartFrom(MUTABLE_EVENT, timestamp2, timestamp1);
    }
    
    @Test
    public void immutableOriginEventMutableStudyBurst() throws Exception {
        setupSchedule(IMMUTABLE_EVENT, MUTABLE, "P1D");
        
        // Now submit that event
        DateTime timestamp1 = DateTime.now(UTC);
        createOrUpdateEvent(IMMUTABLE_EVENT, timestamp1, null);
        // Verify the follow-on events were created
        verifyTimestampsStartFrom(IMMUTABLE_EVENT, timestamp1, timestamp1);
        // Try changing one of these events, it works
        createOrUpdateEvent("study_burst:burst1:02", timestamp1.plusDays(10), null);
        assertEventTimestamp("study_burst:burst1:02", timestamp1.plusDays(10));
        // Try deleting one of these study burst events, it works
        assertEventTimestampDelete("study_burst:burst1:02", true);
        
        // Update the original immutable event
        DateTime timestamp2 = DateTime.now(UTC).plusDays(1);
        failToCreateOrUpdateEvent(IMMUTABLE_EVENT, timestamp2);
        // All the study burst events should be unchanged, because they are immutable
        verifyTimestampsStartFrom(IMMUTABLE_EVENT, timestamp1, timestamp2);
    }
    
    @Test
    public void immutableOriginEventImmutableStudyBurst() throws Exception {
        setupSchedule(IMMUTABLE_EVENT, IMMUTABLE, "P1D");
        
        // Now submit that event
        DateTime timestamp1 = DateTime.now(UTC);
        createOrUpdateEvent(IMMUTABLE_EVENT, timestamp1, null);
        // Verify the follow-on events were created
        verifyTimestampsStartFrom(IMMUTABLE_EVENT, timestamp1, timestamp1);
        // Try changing one of these events, it doesn't work
        failToCreateOrUpdateEvent("study_burst:burst1:02", timestamp1.plusDays(10));
        assertEventTimestamp("study_burst:burst1:02", timestamp1.plusDays(2));
        // Try deleting one of these study burst events, it doesn't work
        assertEventTimestampDelete("study_burst:burst1:02", false);
        
        // Update the original mutable event
        DateTime timestamp2 = DateTime.now(UTC).plusDays(1);
        failToCreateOrUpdateEvent(IMMUTABLE_EVENT, timestamp2);
        // All the study burst events should be unchanged
        verifyTimestampsStartFrom(IMMUTABLE_EVENT, timestamp1, timestamp1);
    }
    
    @Test
    public void verifyStudyBurstDependencies() throws Exception {
        setupSchedule(MUTABLE_EVENT, MUTABLE, "P1D");
        
        // Now submit that event
        DateTime timestamp1 = DateTime.now(UTC);
        createOrUpdateEvent(MUTABLE_EVENT, timestamp1, false);
        // Verify the follow-on events were created
        verifyTimestampsStartFrom(MUTABLE_EVENT, timestamp1, timestamp1);
        
        // Update the original mutable event
        DateTime timestamp2 = DateTime.now(UTC).plusDays(1);
        createOrUpdateEvent(MUTABLE_EVENT, timestamp2, false);
        // The study burst event should *not* be changed
        verifyTimestampsStartFrom(MUTABLE_EVENT, timestamp2, timestamp1);
        
        // delete the origin event, the study burst events are also deleted
        assertEventTimestampDelete(MUTABLE_EVENT, true);
        
        Set<String> eventIds = usersApi.getStudyActivityEvents(STUDY_ID_1).execute().body()
                .getItems().stream().map(StudyActivityEvent::getEventId).collect(toSet());
        assertFalse(eventIds.contains("study_burst:burst1:01"));
        assertFalse(eventIds.contains("study_burst:burst1:02"));
        assertFalse(eventIds.contains("study_burst:burst1:03"));
        assertFalse(eventIds.contains("study_burst:burst1:04"));
    }
    
    private void createOrUpdateEvent(String eventId, DateTime timestamp, Boolean updateBursts) throws Exception {
        StudyActivityEventRequest request = new StudyActivityEventRequest()
                .clientTimeZone("America/Los_Angeles").eventId(eventId).timestamp(timestamp);
        usersApi.createStudyActivityEvent(STUDY_ID_1, request, true, updateBursts).execute();
    }
    
    private void failToCreateOrUpdateEvent(String eventId, DateTime timestamp) throws Exception {
        StudyActivityEventRequest request = new StudyActivityEventRequest()
                .eventId(eventId).timestamp(timestamp);
        try {
            usersApi.createStudyActivityEvent(STUDY_ID_1, request, true, null).execute();
            fail("Should have thrown exception");
        } catch(BadRequestException e) {
            // this was expected.
        }
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
        if (shouldBeDeleted) {
            usersApi.deleteStudyActivityEvent(STUDY_ID_1, eventId, false).execute();
            StudyActivityEventList events = usersApi.getStudyActivityEvents(STUDY_ID_1).execute().body();
            StudyActivityEvent event = findEventById(events, eventId);
            assertNull(event);   
        } else {
            try {
                usersApi.deleteStudyActivityEvent(STUDY_ID_1, eventId, true).execute();
                fail("Should have thrown exception");
            } catch(BadRequestException e) {
            }
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
