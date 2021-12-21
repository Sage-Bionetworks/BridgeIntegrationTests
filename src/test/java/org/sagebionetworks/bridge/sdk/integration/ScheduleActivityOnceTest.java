package org.sagebionetworks.bridge.sdk.integration;

import static org.junit.Assert.assertEquals;
import static org.sagebionetworks.bridge.rest.model.Role.DEVELOPER;

import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.sagebionetworks.bridge.rest.api.AppsApi;
import org.sagebionetworks.bridge.rest.api.ForAdminsApi;
import org.sagebionetworks.bridge.rest.api.ForConsentedUsersApi;
import org.sagebionetworks.bridge.rest.api.ForSuperadminsApi;
import org.sagebionetworks.bridge.rest.api.SchedulesV1Api;
import org.sagebionetworks.bridge.rest.model.Activity;
import org.sagebionetworks.bridge.rest.model.ActivityType;
import org.sagebionetworks.bridge.rest.model.App;
import org.sagebionetworks.bridge.rest.model.Criteria;
import org.sagebionetworks.bridge.rest.model.CriteriaScheduleStrategy;
import org.sagebionetworks.bridge.rest.model.GuidVersionHolder;
import org.sagebionetworks.bridge.rest.model.Schedule;
import org.sagebionetworks.bridge.rest.model.ScheduleCriteria;
import org.sagebionetworks.bridge.rest.model.SchedulePlan;
import org.sagebionetworks.bridge.rest.model.SchedulePlanList;
import org.sagebionetworks.bridge.rest.model.ScheduleType;
import org.sagebionetworks.bridge.rest.model.ScheduledActivity;
import org.sagebionetworks.bridge.rest.model.ScheduledActivityList;
import org.sagebionetworks.bridge.rest.model.SignUp;
import org.sagebionetworks.bridge.rest.model.TaskReference;
import org.sagebionetworks.bridge.rest.model.VersionHolder;
import org.sagebionetworks.bridge.user.TestUser;
import org.sagebionetworks.bridge.user.TestUserHelper;

public class ScheduleActivityOnceTest {
    private static final String FILTERED_LABEL = "ScheduleActivityOnceTest";
    private TestUser admin;
    private TestUser developer;
    private TestUser user;
    private SchedulePlan schedulePlan;
    
    @SuppressWarnings("deprecation")
    @Before
    public void before() throws Exception {
        admin = TestUserHelper.getSignedInAdmin();
        developer = TestUserHelper.createAndSignInUser(ScheduleActivityOnceTest.class, true, DEVELOPER);
        SignUp signUp = new SignUp().dataGroups(ImmutableList.of("sdk-int-1"));
        user = new TestUserHelper.Builder(ScheduleActivityOnceTest.class).withConsentUser(true).withSignUp(signUp)
                .createAndSignInUser();
        
        SchedulePlanList list = developer.getClient(SchedulesV1Api.class).getSchedulePlans(true).execute().body();
        for (SchedulePlan plan : list.getItems()) {
            if (plan.getLabel().contains(FILTERED_LABEL)) {
                admin.getClient(ForAdminsApi.class).deleteSchedulePlan(plan.getGuid(), true).execute();    
            }
        }
    }
    
    @SuppressWarnings("deprecation")
    @After
    public void after() throws Exception {
        if (developer != null) {
            developer.signOutAndDeleteUser();
        }
        if (user != null) {
            user.signOutAndDeleteUser();
        }
        if (schedulePlan != null) {
            admin.getClient(SchedulesV1Api.class).deleteSchedulePlan(schedulePlan.getGuid(), true).execute();
        }
    }
    
    @SuppressWarnings("deprecation")
    @Test
    public void test() throws Exception {
        App app = admin.getClient(AppsApi.class).getUsersApp().execute().body();
        if (app.isExternalIdRequiredOnSignup()) {
            app.setExternalIdRequiredOnSignup(false);
            
            VersionHolder version = admin.getClient(ForSuperadminsApi.class).updateApp(app.getIdentifier(), app).execute().body();
            app.setVersion(version.getVersion());
        }
        Schedule schedule = new Schedule();
        schedule.setLabel("Schedule Label");
        schedule.setScheduleType(ScheduleType.ONCE);
        schedule.getActivities().add(new Activity().label("label").activityType(ActivityType.TASK)
                .task(new TaskReference().identifier("CCC")));
        schedule.getTimes().add("13:11");
        
        ScheduleCriteria schCriteria = new ScheduleCriteria();
        schCriteria.setSchedule(schedule);
        schCriteria.setCriteria(new Criteria().addAllOfGroupsItem("sdk-int-1"));
        CriteriaScheduleStrategy strategy = new CriteriaScheduleStrategy();
        strategy.addScheduleCriteriaItem(schCriteria);
        
        schedulePlan = new SchedulePlan();
        schedulePlan.setLabel(FILTERED_LABEL);
        schedulePlan.setStrategy(strategy);
        
        GuidVersionHolder keys = developer.getClient(SchedulesV1Api.class).createSchedulePlan(schedulePlan).execute().body();
        schedulePlan.setGuid(keys.getGuid());
        schedulePlan.setVersion(keys.getVersion());
        
        ForConsentedUsersApi userApi = user.getClient(ForConsentedUsersApi.class);
        ScheduledActivityList first = filterList(userApi.getScheduledActivities("-07:00", 4, null).execute().body(), keys.getGuid());
        ScheduledActivityList second = filterList(userApi.getScheduledActivities("+03:00", 4, null).execute().body(), keys.getGuid());
        assertEquals(1, first.getItems().size());
        assertEquals(1, second.getItems().size());
        
        ScheduledActivity act1 = first.getItems().get(0);
        ScheduledActivity act2 = second.getItems().get(0);
        
        // The time portion should 13:11 because that's what we set, regardless of time zone.
        assertEquals("13:11:00.000", act1.getScheduledOn().toLocalTime().toString());
        assertEquals("13:11:00.000", act2.getScheduledOn().toLocalTime().toString());
    }
    
    private ScheduledActivityList filterList(ScheduledActivityList list, String guid) throws Exception {
        List<ScheduledActivity> activities = list.getItems().stream()
                .filter((activity) -> guid.equals(activity.getSchedulePlanGuid())).collect(Collectors.toList());
        Tests.setVariableValueInObject(list, "items", activities);
        return list;
    }
}
