package org.sagebionetworks.bridge.sdk.integration;

import static org.joda.time.DateTimeZone.UTC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.sagebionetworks.bridge.sdk.integration.InitListener.EVENT_KEY1;
import static org.sagebionetworks.bridge.sdk.integration.InitListener.EVENT_KEY2;
import static org.sagebionetworks.bridge.sdk.integration.InitListener.EVENT_KEY3;
import static org.sagebionetworks.bridge.sdk.integration.Tests.STUDY_ID_1;
import static org.sagebionetworks.bridge.sdk.integration.Tests.STUDY_ID_2;

import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.sagebionetworks.bridge.rest.api.ForConsentedUsersApi;
import org.sagebionetworks.bridge.rest.api.ForResearchersApi;
import org.sagebionetworks.bridge.rest.exceptions.EntityNotFoundException;
import org.sagebionetworks.bridge.rest.model.ActivityEvent;
import org.sagebionetworks.bridge.rest.model.ActivityEventList;
import org.sagebionetworks.bridge.rest.model.CustomActivityEventRequest;
import org.sagebionetworks.bridge.rest.model.Role;
import org.sagebionetworks.bridge.rest.model.StudyActivityEvent;
import org.sagebionetworks.bridge.rest.model.StudyActivityEventList;
import org.sagebionetworks.bridge.rest.model.StudyActivityEventPagedList;
import org.sagebionetworks.bridge.rest.model.StudyActivityEventRequest;
import org.sagebionetworks.bridge.user.TestUserHelper;

public class StudyActivityEventTest {
    
    private static TestUserHelper.TestUser researcher;
    private static TestUserHelper.TestUser user;
    private static ForConsentedUsersApi usersApi;
    private static ForResearchersApi researchersApi;
    
    @Before
    public void beforeAll() throws Exception {
        researcher = TestUserHelper.createAndSignInUser(StudyActivityEventTest.class, true, Role.RESEARCHER);
        researchersApi = researcher.getClient(ForResearchersApi.class);
        
        // Create user last, so the automatic custom events are created
        user = TestUserHelper.createAndSignInUser(StudyActivityEventTest.class, true);
        usersApi = user.getClient(ForConsentedUsersApi.class);
    }
    
    @After
    public void after() throws Exception {
        if (user != null) {
            user.signOutAndDeleteUser();
        }
        if (researcher != null) {
            researcher.signOutAndDeleteUser();
        }
    }

    @SuppressWarnings("deprecation")
    @Test
    public void createStudyActivityEvent() throws Exception {
        // Because this is set first, it is immutable and will not be changed by the study-scoped value.
        // Both with exist side-by-side
        DateTime globalTimestamp = DateTime.now(UTC).minusDays(2);
        CustomActivityEventRequest globalRequest = new CustomActivityEventRequest()
                .eventId(EVENT_KEY1).timestamp(globalTimestamp);
        researchersApi.createActivityEventForParticipant(user.getUserId(), globalRequest).execute();
        
        DateTime studyScopedTimestamp = DateTime.now(UTC);  
        StudyActivityEventRequest studyScopedRequest = new StudyActivityEventRequest()
            .eventId(EVENT_KEY1).timestamp(studyScopedTimestamp);
        researchersApi.createStudyParticipantStudyActivityEvent(STUDY_ID_1, user.getUserId(), studyScopedRequest).execute();
        
        ActivityEventList globalList = researchersApi.getActivityEventsForParticipant(user.getUserId()).execute().body();
        ActivityEvent globalEvent = findEventByKey(globalList, "custom:"+EVENT_KEY1);
        assertEquals(globalTimestamp, globalEvent.getTimestamp());
        
        StudyActivityEventList scopedList = researchersApi.getStudyParticipantStudyActivityEvents(STUDY_ID_1, user.getUserId()).execute().body();
        StudyActivityEvent scopedEvent = findEventByKey(scopedList, "custom:"+EVENT_KEY1);
        assertEquals(studyScopedTimestamp, scopedEvent.getTimestamp());

        // User can also retrieve these two different events.
        scopedList = usersApi.getStudyActivityEvents(STUDY_ID_1).execute().body();
        scopedEvent = findEventByKey(scopedList, "custom:"+EVENT_KEY1);
        assertEquals(studyScopedTimestamp, scopedEvent.getTimestamp());
        
        globalList= usersApi.getActivityEvents().execute().body();
        globalEvent = findEventByKey(globalList, "custom:"+EVENT_KEY1);
        assertEquals(globalTimestamp, globalEvent.getTimestamp());
        
        // Verify that a user can create a custom event, and that it is visible to researchers
        globalTimestamp = globalTimestamp.minusWeeks(2);
        globalRequest = new CustomActivityEventRequest().eventId(EVENT_KEY2).timestamp(globalTimestamp);
        usersApi.createCustomActivityEvent(globalRequest).execute();
        
        studyScopedTimestamp = studyScopedTimestamp.minusWeeks(2);
        studyScopedRequest = new StudyActivityEventRequest().eventId(EVENT_KEY2).timestamp(studyScopedTimestamp);
        usersApi.createStudyActivityEvent(STUDY_ID_1, studyScopedRequest).execute();

        // user can see these events
        scopedList = usersApi.getStudyActivityEvents(STUDY_ID_1).execute().body();
        scopedEvent = findEventByKey(scopedList, "custom:"+EVENT_KEY2);
        assertEquals(studyScopedTimestamp, scopedEvent.getTimestamp());
        
        globalList= usersApi.getActivityEvents().execute().body();
        globalEvent = findEventByKey(globalList, "custom:"+EVENT_KEY2);
        assertEquals(globalTimestamp, globalEvent.getTimestamp());

        // the researcher can see these events
        globalList = researchersApi.getActivityEventsForParticipant(user.getUserId()).execute().body();
        globalEvent = findEventByKey(globalList, "custom:"+EVENT_KEY2);
        assertEquals(globalTimestamp, globalEvent.getTimestamp());
        
        scopedList = researchersApi.getStudyParticipantStudyActivityEvents(STUDY_ID_1, user.getUserId()).execute().body();
        scopedEvent = findEventByKey(scopedList, "custom:"+EVENT_KEY2);
        assertEquals(studyScopedTimestamp, scopedEvent.getTimestamp());
    }
    
    @Test
    public void testMutableStudyEvent() throws Exception {
        DateTime now = DateTime.now(DateTimeZone.UTC);
        DateTime pastTime = DateTime.now(DateTimeZone.UTC).minusHours(2);
        DateTime futureTime = DateTime.now(DateTimeZone.UTC).plusHours(2);

        // Create event #1 which is mutable
        StudyActivityEventRequest req = new StudyActivityEventRequest().eventId(EVENT_KEY1).timestamp(now);
        usersApi.createStudyActivityEvent(STUDY_ID_1, req).execute();
        
        StudyActivityEventList list = usersApi.getStudyActivityEvents(STUDY_ID_1).execute().body();
        assertEquals(now, getTimestamp(list, EVENT_KEY1));
        
        // future time updates
        req = new StudyActivityEventRequest().eventId(EVENT_KEY1).timestamp(futureTime);
        usersApi.createStudyActivityEvent(STUDY_ID_1, req).execute();
        list = usersApi.getStudyActivityEvents(STUDY_ID_1).execute().body();
        assertEquals(futureTime, getTimestamp(list, EVENT_KEY1));

        // past time updates
        req = new StudyActivityEventRequest().eventId(EVENT_KEY1).timestamp(pastTime);
        usersApi.createStudyActivityEvent(STUDY_ID_1, req).execute();
        list = usersApi.getStudyActivityEvents(STUDY_ID_1).execute().body();
        assertEquals(pastTime, getTimestamp(list, EVENT_KEY1));
        
        // This will have accumulated three events
        StudyActivityEventPagedList page = usersApi.getStudyActivityEventHistory(
                STUDY_ID_1, EVENT_KEY1, null, null).execute().body();
        assertEquals(3, page.getItems().size());
        assertEquals(Integer.valueOf(3), page.getTotal());
        assertEquals(ImmutableSet.of(now, pastTime, futureTime), page.getItems().stream()
                .map(StudyActivityEvent::getTimestamp).collect(Collectors.toSet()));
        
        // These are counted in the record total
        list = usersApi.getStudyActivityEvents(STUDY_ID_1).execute().body();
        assertEquals(Integer.valueOf(3), findEventByKey(list, "custom:"+EVENT_KEY1).getRecordCount());

        // can delete
        usersApi.deleteStudyActivityEvent(STUDY_ID_1, EVENT_KEY1).execute();
        
        list = usersApi.getStudyActivityEvents(STUDY_ID_1).execute().body();
        assertNull(getTimestamp(list, EVENT_KEY1));
        
        // Cannot retrieve events for study 2.
        try {
            usersApi.getStudyActivityEvents(STUDY_ID_2).execute().body();
            fail("Should have thrown exception");
        } catch(EntityNotFoundException e) {
            assertEquals("Account not found.", e.getMessage());
        }
    }

    @Test
    public void testImmutableStudyEvent() throws Exception {
        DateTime now = DateTime.now(DateTimeZone.UTC);
        DateTime pastTime = DateTime.now(DateTimeZone.UTC).minusHours(2);
        DateTime futureTime = DateTime.now(DateTimeZone.UTC).plusHours(2);

        // Create event #2 which is immutable.
        StudyActivityEventRequest req = new StudyActivityEventRequest().eventId(EVENT_KEY2).timestamp(now);
        usersApi.createStudyActivityEvent(STUDY_ID_1, req).execute();
        StudyActivityEventList list = usersApi.getStudyActivityEvents(STUDY_ID_1).execute().body();
        assertEquals(now, getTimestamp(list, EVENT_KEY2));

        // past time won't update
        req = new StudyActivityEventRequest().eventId(EVENT_KEY2).timestamp(pastTime);
        usersApi.createStudyActivityEvent(STUDY_ID_1, req).execute();
        list = usersApi.getStudyActivityEvents(STUDY_ID_1).execute().body();
        assertEquals(getTimestamp(list, EVENT_KEY2), now);

        // future time won't update
        req = new StudyActivityEventRequest().eventId(EVENT_KEY2).timestamp(futureTime);
        usersApi.createStudyActivityEvent(STUDY_ID_1, req).execute();
        list = usersApi.getStudyActivityEvents(STUDY_ID_1).execute().body();
        assertEquals(getTimestamp(list, EVENT_KEY2), now);

        // nor will it delete
        usersApi.deleteStudyActivityEvent(STUDY_ID_1, EVENT_KEY2).execute();

        list = usersApi.getStudyActivityEvents(STUDY_ID_1).execute().body();
        assertEquals(getTimestamp(list, EVENT_KEY2), now);
        
        // nor will it accumulate in history
        StudyActivityEventPagedList page = usersApi.getStudyActivityEventHistory(
                STUDY_ID_1, EVENT_KEY2, null, null).execute().body();
        assertEquals(1, page.getItems().size());
        assertEquals(Integer.valueOf(1), page.getTotal());
    }
    
    @Test
    public void testFutureOnlyStudyEvent() throws Exception {
        DateTime now = DateTime.now(DateTimeZone.UTC);
        DateTime pastTime = DateTime.now(DateTimeZone.UTC).minusHours(2);
        DateTime futureTime = DateTime.now(DateTimeZone.UTC).plusHours(2);

        StudyActivityEventRequest req3 = new StudyActivityEventRequest().eventId(EVENT_KEY3).timestamp(now);
        usersApi.createStudyActivityEvent(STUDY_ID_1, req3).execute();
        
        StudyActivityEventList list3 = usersApi.getStudyActivityEvents(STUDY_ID_1).execute().body();
        assertEquals(now, getTimestamp(list3, EVENT_KEY3));
        
        // future will update
        req3 = new StudyActivityEventRequest().eventId(EVENT_KEY3).timestamp(futureTime);
        usersApi.createStudyActivityEvent(STUDY_ID_1, req3).execute();
        list3 = usersApi.getStudyActivityEvents(STUDY_ID_1).execute().body();
        assertEquals(getTimestamp(list3, EVENT_KEY3), futureTime);
        
        // past will not update
        req3 = new StudyActivityEventRequest().eventId(EVENT_KEY3).timestamp(pastTime);
        usersApi.createStudyActivityEvent(STUDY_ID_1, req3).execute();
        list3 = usersApi.getStudyActivityEvents(STUDY_ID_1).execute().body();
        assertEquals(getTimestamp(list3, EVENT_KEY3), futureTime);

        // This doesn't delete the timestamp
        usersApi.deleteStudyActivityEvent(STUDY_ID_1, EVENT_KEY3).execute();
        
        list3 = usersApi.getStudyActivityEvents(STUDY_ID_1).execute().body();
        assertEquals(getTimestamp(list3, EVENT_KEY3), futureTime);
        
        // This will have accumulated two events
        StudyActivityEventPagedList page = usersApi.getStudyActivityEventHistory(
                STUDY_ID_1, EVENT_KEY3, null, null).execute().body();
        assertEquals(2, page.getItems().size());
        assertEquals(Integer.valueOf(2), page.getTotal());
        assertEquals(ImmutableSet.of(now, futureTime), page.getItems().stream()
                .map(StudyActivityEvent::getTimestamp).collect(Collectors.toSet()));
    }
    
    @SuppressWarnings("deprecation")
    @Test
    public void globalEventsDoNotCreateStudyVersions() throws Exception {
        DateTime globalTimestamp = DateTime.now(UTC).minusDays(2);
        CustomActivityEventRequest globalRequest = new CustomActivityEventRequest()
                .eventId(EVENT_KEY1).timestamp(globalTimestamp);
        researchersApi.createActivityEventForParticipant(user.getUserId(), globalRequest).execute();

        StudyActivityEventList scopedList = researchersApi.getStudyParticipantStudyActivityEvents(
                STUDY_ID_1, user.getUserId()).execute().body();
        assertNull(findEventByKey(scopedList, "custom:"+EVENT_KEY1));

        // The user has not been enrolled in this study so there should be no events.
        try {
            researchersApi.getStudyParticipantStudyActivityEvents(STUDY_ID_2, user.getUserId()).execute().body();
            fail("Should have thrown an exception.");
        } catch(EntityNotFoundException e) {
            
        }
    }
    
    private DateTime getTimestamp(StudyActivityEventList list, String eventId) {
        for (StudyActivityEvent event : list.getItems()) {
            if (event.getEventId().equals("custom:"+eventId)) {
                return event.getTimestamp();
            }
        }
        return null;
    }
    
    private ActivityEvent findEventByKey(ActivityEventList list, String key) {
        for (ActivityEvent event : list.getItems()) {
            if (event.getEventId().equals(key)) {
                return event;
            }
        }
        return null;
    }
    
    private StudyActivityEvent findEventByKey(StudyActivityEventList list, String key) {
        for (StudyActivityEvent event : list.getItems()) {
            if (event.getEventId().equals(key)) {
                return event;
            }
        }
        return null;
    }
}
