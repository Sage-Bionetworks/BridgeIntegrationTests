package org.sagebionetworks.bridge.sdk.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.sagebionetworks.bridge.rest.model.ActivityEventUpdateType.FUTURE_ONLY;
import static org.sagebionetworks.bridge.rest.model.ActivityEventUpdateType.IMMUTABLE;
import static org.sagebionetworks.bridge.rest.model.ActivityEventUpdateType.MUTABLE;
import static org.sagebionetworks.bridge.sdk.integration.InitListener.EVENT_KEY1;
import static org.sagebionetworks.bridge.sdk.integration.InitListener.EVENT_KEY2;
import static org.sagebionetworks.bridge.sdk.integration.InitListener.EVENT_KEY3;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.sagebionetworks.bridge.rest.api.ForConsentedUsersApi;
import org.sagebionetworks.bridge.rest.api.ForDevelopersApi;
import org.sagebionetworks.bridge.rest.api.ForResearchersApi;
import org.sagebionetworks.bridge.rest.model.ActivityEvent;
import org.sagebionetworks.bridge.rest.model.ActivityEventList;
import org.sagebionetworks.bridge.rest.model.App;
import org.sagebionetworks.bridge.rest.model.CustomActivityEventRequest;
import org.sagebionetworks.bridge.rest.model.Role;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;
import org.sagebionetworks.bridge.user.TestUserHelper;

public class ActivityEventTest {
    private static final String TWO_WEEKS_AFTER_KEY = "2-weeks-after";
    private static final String TWO_WEEKS_AFTER_VALUE = "enrollment:P2W";
    
    private static TestUserHelper.TestUser developer;
    private static TestUserHelper.TestUser researcher;
    private static TestUserHelper.TestUser user;
    private static ForConsentedUsersApi usersApi;
    private static ForResearchersApi researchersApi;

    @Before
    public void beforeAll() throws Exception {
        researcher = TestUserHelper.createAndSignInUser(ActivityEventTest.class, true, Role.RESEARCHER);
        researchersApi = researcher.getClient(ForResearchersApi.class);

        developer = TestUserHelper.createAndSignInUser(ActivityEventTest.class, false, Role.DEVELOPER);
        ForDevelopersApi developersApi = developer.getClient(ForDevelopersApi.class);

        App app = developersApi.getUsersApp().execute().body();
        boolean updateApp = false;

        // Add custom event keys, if not already present, with three different update behaviors.
        if (!app.getCustomEvents().keySet().contains(EVENT_KEY1) || 
                app.getCustomEvents().get(EVENT_KEY1) != MUTABLE) {
            app.getCustomEvents().put(EVENT_KEY1, MUTABLE);
            updateApp = true;
        }
        if (!app.getCustomEvents().keySet().contains(EVENT_KEY2) ||
                app.getCustomEvents().get(EVENT_KEY2) != IMMUTABLE) {
            app.getCustomEvents().put(EVENT_KEY2, IMMUTABLE);
            updateApp = true;
        }
        if (!app.getCustomEvents().keySet().contains(EVENT_KEY3) ||
                app.getCustomEvents().get(EVENT_KEY3) != FUTURE_ONLY) {
            app.getCustomEvents().put(EVENT_KEY3, FUTURE_ONLY);
            updateApp = true;
        }

        // Add automatic custom event.
        if (!app.getAutomaticCustomEvents().containsKey(TWO_WEEKS_AFTER_KEY)) {
            app.putAutomaticCustomEventsItem(TWO_WEEKS_AFTER_KEY, TWO_WEEKS_AFTER_VALUE);
            updateApp = true;
        }

        if (updateApp) {
            developersApi.updateUsersApp(app).execute();
        }
        
        // Create user last, so the automatic custom events are created
        user = TestUserHelper.createAndSignInUser(ActivityEventTest.class, true);
        usersApi = user.getClient(ForConsentedUsersApi.class);
    }

    @After
    public void deleteDeveloper() throws Exception {
        if (developer != null) {
            developer.signOutAndDeleteUser();
        }
    }

    @After
    public void deleteResearcher() throws Exception {
        if (researcher != null) {
            researcher.signOutAndDeleteUser();
        }
    }

    @After
    public void deleteUser() throws Exception {
        if (user != null) {
            user.signOutAndDeleteUser();
        }
    }

    @Test
    public void canCreateCreatedOnAndStudyStartDate() throws IOException {
        // Get activity events and convert to map for ease of use
        List<ActivityEvent> activityEventList = usersApi.getActivityEvents().execute().body().getItems();
        Map<String, ActivityEvent> activityEventMap = activityEventList.stream().collect(
                Collectors.toMap(ActivityEvent::getEventId, e -> e));
        
        // Verify enrollment events exist
        ActivityEvent enrollmentEvent = activityEventMap.get("enrollment");
        assertNotNull(enrollmentEvent);
        DateTime enrollmentTime = enrollmentEvent.getTimestamp();
        assertNotNull(enrollmentTime);
        
        StudyParticipant participant = usersApi.getUsersParticipantRecord(false).execute().body();
        
        // Verify enrollment events exist
        ActivityEvent createdOnEvent = activityEventMap.get("created_on");
        assertNotNull(createdOnEvent);
        DateTime createdOnTime = createdOnEvent.getTimestamp();
        assertEquals(createdOnTime, participant.getCreatedOn());
        
        // Verify enrollment events exist
        ActivityEvent studyStateDateEvent = activityEventMap.get("study_start_date");
        assertNotNull(studyStateDateEvent);
        DateTime studyStateDateTime = studyStateDateEvent.getTimestamp();
        assertEquals(studyStateDateTime, enrollmentTime);
    }

    @Test
    public void canCrudCustomEvent() throws IOException {
        // Setup
        ActivityEventList activityEventList = usersApi.getActivityEvents().execute().body();
        List<ActivityEvent> activityEvents = activityEventList.getItems();

        StudyParticipant participant = usersApi.getUsersParticipantRecord(false).execute().body();

        // Create custom event
        DateTime timestamp = DateTime.now(DateTimeZone.UTC);
        usersApi.createCustomActivityEvent(
                new CustomActivityEventRequest().eventId(EVENT_KEY1).timestamp(timestamp)).execute();

        // Verify created event
        activityEventList = usersApi.getActivityEvents().execute().body();
        List<ActivityEvent> updatedActivityEvents = activityEventList.getItems();
        assertNotEquals(activityEvents, updatedActivityEvents);

        String expectedEventKey = "custom:" + EVENT_KEY1;
        Optional<ActivityEvent> eventOptional = updatedActivityEvents.stream()
                .filter(e -> e.getEventId().equals(expectedEventKey))
                .findAny();

        assertTrue(eventOptional.isPresent());
        ActivityEvent event = eventOptional.get();
        assertEquals(timestamp, event.getTimestamp());

        // Verify researcher's view of created event
        activityEventList = researchersApi.getActivityEventsForParticipant(participant.getId()).execute().body();
        assertEquals(updatedActivityEvents, activityEventList.getItems());
        
        // delete the event
        usersApi.deleteCustomActivityEvent(EVENT_KEY1).execute();
        activityEventList = usersApi.getActivityEvents().execute().body();
        assertFalse(activityEventList.getItems().stream().map(ActivityEvent::getEventId)
                .collect(Collectors.toSet()).contains(expectedEventKey));
        assertFalse(activityEventList.getItems().stream().map(ActivityEvent::getEventId)
                .collect(Collectors.toSet()).contains(EVENT_KEY1));
    }

    @Test
    public void automaticCustomEvents() throws Exception {
        // Get activity events and convert to map for ease of use
        List<ActivityEvent> activityEventList = usersApi.getActivityEvents().execute().body().getItems();
        Map<String, ActivityEvent> activityEventMap = activityEventList.stream().collect(
                Collectors.toMap(ActivityEvent::getEventId, e -> e));

        // Verify enrollment events exist
        ActivityEvent enrollmentEvent = activityEventMap.get("enrollment");
        assertNotNull(enrollmentEvent);
        DateTime enrollmentTime = enrollmentEvent.getTimestamp();
        assertNotNull(enrollmentTime);

        // Verify custom event exists and that it's 2 weeks after enrollment
        ActivityEvent twoWeeksAfterEvent = activityEventMap.get("custom:" + TWO_WEEKS_AFTER_KEY);
        assertNotNull(twoWeeksAfterEvent);
        DateTime twoWeeksAfterTime = twoWeeksAfterEvent.getTimestamp();
        // This can fail when you're near the time zone change to DST. Add one hour to overshoot 
        // and compensate for the time zone change.
        Period twoWeeksAfterPeriod = new Period(enrollmentTime, twoWeeksAfterTime.plusHours(1));
        assertEquals(2, twoWeeksAfterPeriod.getWeeks());
    }
    
    @Test
    public void researcherCanSubmitCustomEvents() throws Exception {
        ForResearchersApi researchersApi = researcher.getClient(ForResearchersApi.class);
        
        // it's stored as a long since epoch, so timezone must be UTC to match
        DateTime timestamp1 = DateTime.now(DateTimeZone.UTC).minusHours(4);
        DateTime timestamp2 = DateTime.now(DateTimeZone.UTC);
        
        CustomActivityEventRequest request = new CustomActivityEventRequest();
        request.setEventId(EVENT_KEY1);
        request.setTimestamp(timestamp1);
        
        researchersApi.createActivityEventForParticipant(user.getUserId(), request).execute();
        
        ActivityEventList list = researchersApi.getActivityEventsForParticipant(user.getUserId()).execute().body();
        Optional<ActivityEvent> optional = list.getItems().stream()
                .filter((evt) -> evt.getEventId().equals("custom:"+EVENT_KEY1)).findAny();
        assertTrue(optional.isPresent());
        assertEquals(timestamp1.toString(), optional.get().getTimestamp().toString());
        
        request.setTimestamp(timestamp2);
        researchersApi.createActivityEventForParticipant(user.getUserId(), request).execute();
        
        list = researchersApi.getActivityEventsForParticipant(user.getUserId()).execute().body();
        optional = list.getItems().stream()
                .filter((evt) -> evt.getEventId().equals("custom:"+EVENT_KEY1)).findAny();
        assertTrue(optional.isPresent());
        assertEquals(timestamp2.toString(), optional.get().getTimestamp().toString());
    }
    
    @Test
    public void testMutableGlobalEvent() throws Exception {
        DateTime now = DateTime.now(DateTimeZone.UTC);
        DateTime pastTime = DateTime.now(DateTimeZone.UTC).minusHours(2);
        DateTime futureTime = DateTime.now(DateTimeZone.UTC).plusHours(2);

        // Create event #1 which is mutable
        CustomActivityEventRequest req = new CustomActivityEventRequest().eventId(EVENT_KEY1).timestamp(now);
        usersApi.createCustomActivityEvent(req).execute();
        
        ActivityEventList list = usersApi.getActivityEvents().execute().body();
        assertEquals(now, getTimestamp(list, EVENT_KEY1));
        
        // future update works
        req = new CustomActivityEventRequest().eventId(EVENT_KEY1).timestamp(futureTime);
        usersApi.createCustomActivityEvent(req).execute();
        list = usersApi.getActivityEvents().execute().body();
        assertEquals(futureTime, getTimestamp(list, EVENT_KEY1));
        
        // past update works
        req = new CustomActivityEventRequest().eventId(EVENT_KEY1).timestamp(pastTime);
        usersApi.createCustomActivityEvent(req).execute();
        list = usersApi.getActivityEvents().execute().body();
        assertEquals(pastTime, getTimestamp(list, EVENT_KEY1));
        
        // delete works
        usersApi.deleteCustomActivityEvent(EVENT_KEY1).execute();
        
        list = usersApi.getActivityEvents().execute().body();
        assertNull(getTimestamp(list, EVENT_KEY1));
    }
    
    @Test
    public void testImmutableGlobalEvent() throws Exception {
        DateTime now = DateTime.now(DateTimeZone.UTC);
        DateTime pastTime = DateTime.now(DateTimeZone.UTC).minusHours(2);
        DateTime futureTime = DateTime.now(DateTimeZone.UTC).plusHours(2);

        // Create event #2 which is immutable.
        CustomActivityEventRequest req = new CustomActivityEventRequest().eventId(EVENT_KEY2).timestamp(now);
        usersApi.createCustomActivityEvent(req).execute();
        
        ActivityEventList list = usersApi.getActivityEvents().execute().body();
        assertEquals(now, getTimestamp(list, EVENT_KEY2));

        // future will not update
        req = new CustomActivityEventRequest().eventId(EVENT_KEY2).timestamp(futureTime);
        usersApi.createCustomActivityEvent(req).execute();
        list = usersApi.getActivityEvents().execute().body();
        assertEquals(getTimestamp(list, EVENT_KEY2), now);

        // past will not update
        req = new CustomActivityEventRequest().eventId(EVENT_KEY2).timestamp(pastTime);
        usersApi.createCustomActivityEvent(req).execute();
        list = usersApi.getActivityEvents().execute().body();
        assertEquals(getTimestamp(list, EVENT_KEY2), now);

        // delete will not delete
        usersApi.deleteCustomActivityEvent(EVENT_KEY2).execute();

        list = usersApi.getActivityEvents().execute().body();
        assertEquals(getTimestamp(list, EVENT_KEY2), now);
    }
    
    @Test
    public void testFutureOnlyGlobalEvent() throws Exception {
        DateTime now = DateTime.now(DateTimeZone.UTC);
        DateTime pastTime = DateTime.now(DateTimeZone.UTC).minusHours(2);
        DateTime futureTime = DateTime.now(DateTimeZone.UTC).plusHours(2);

        CustomActivityEventRequest req3 = new CustomActivityEventRequest().eventId(EVENT_KEY3).timestamp(now);
        usersApi.createCustomActivityEvent(req3).execute();
        
        ActivityEventList list3 = usersApi.getActivityEvents().execute().body();
        assertEquals(now, getTimestamp(list3, EVENT_KEY3));
        
        // future update will work
        req3 = new CustomActivityEventRequest().eventId(EVENT_KEY3).timestamp(futureTime);
        usersApi.createCustomActivityEvent(req3).execute();
        list3 = usersApi.getActivityEvents().execute().body();
        assertEquals(getTimestamp(list3, EVENT_KEY3), futureTime);
        
        // past update will not work
        req3 = new CustomActivityEventRequest().eventId(EVENT_KEY3).timestamp(pastTime);
        usersApi.createCustomActivityEvent(req3).execute();
        list3 = usersApi.getActivityEvents().execute().body();
        assertEquals(getTimestamp(list3, EVENT_KEY3), futureTime);

        // This doesn't delete the timestamp
        usersApi.deleteCustomActivityEvent(EVENT_KEY3).execute();
        
        list3 = usersApi.getActivityEvents().execute().body();
        assertEquals(getTimestamp(list3, EVENT_KEY3), futureTime);
    }
    
    private DateTime getTimestamp(ActivityEventList list, String eventId) {
        for (ActivityEvent event : list.getItems()) {
            if (event.getEventId().equals("custom:"+eventId)) {
                return event.getTimestamp();
            }
        }
        return null;
    }

}
