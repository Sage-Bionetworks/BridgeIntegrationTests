package org.sagebionetworks.bridge.sdk.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.sagebionetworks.bridge.rest.model.ScheduledActivity;
import org.sagebionetworks.bridge.rest.api.ForConsentedUsersApi;
import org.sagebionetworks.bridge.rest.api.SchedulesV1Api;
import org.sagebionetworks.bridge.rest.model.Activity;
import org.sagebionetworks.bridge.rest.model.ClientInfo;
import org.sagebionetworks.bridge.rest.model.Criteria;
import org.sagebionetworks.bridge.rest.model.CriteriaScheduleStrategy;
import org.sagebionetworks.bridge.rest.model.Role;
import org.sagebionetworks.bridge.rest.model.Schedule;
import org.sagebionetworks.bridge.rest.model.ScheduleCriteria;
import org.sagebionetworks.bridge.rest.model.SchedulePlan;
import org.sagebionetworks.bridge.rest.model.ScheduleType;
import org.sagebionetworks.bridge.rest.model.ScheduledActivityList;
import org.sagebionetworks.bridge.rest.model.SimpleScheduleStrategy;
import org.sagebionetworks.bridge.rest.model.TaskReference;
import org.sagebionetworks.bridge.user.TestUser;
import org.sagebionetworks.bridge.user.TestUserHelper;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

public class ScheduleTest {
    
    private String planGuid;
    
    private TestUser user1;
    private TestUser user2;
    private TestUser user3;
    private TestUser developer;
    
    @Before
    public void before() throws Exception {
        ClientInfo clientInfo = getClientInfo(Tests.APP_NAME, 3);
        developer = new TestUserHelper.Builder(getClass()).withClientInfo(clientInfo).withConsentUser(true)
                .withRoles(Role.DEVELOPER).createAndSignInUser();
    }
    
    @SuppressWarnings("deprecation")
    @After
    public void after() throws Exception {
        if (user1 != null) {
            user1.signOutAndDeleteUser();    
        }
        if (user2 != null) {
            user2.signOutAndDeleteUser();    
        }
        if (user3 != null) {
            user3.signOutAndDeleteUser();    
        }
        if (developer != null) {
            try {
                if (planGuid != null) {
                    TestUser admin = TestUserHelper.getSignedInAdmin();
                    SchedulesV1Api schedulesApi = admin.getClient(SchedulesV1Api.class);
                    schedulesApi.deleteSchedulePlan(planGuid, true).execute();
                }
            } finally {
                developer.signOutAndDeleteUser();    
            }
        }
    }
    
    @SuppressWarnings("deprecation")
    @Test
    public void canScheduleASequence() throws Exception {
        SchedulesV1Api schedulesApi = developer.getClient(SchedulesV1Api.class);
        
        SchedulePlan plan = Tests.getSimpleSchedulePlan();
        plan.setLabel("This is a sequenced recurring schedule");
        SimpleScheduleStrategy strategy = (SimpleScheduleStrategy)plan.getStrategy();
        
        List<Activity> acts = strategy.getSchedule().getActivities();
        String randomLabel = Tests.randomIdentifier(getClass());
        List<Activity> taggedActivities = Tests.labelActivities(acts, randomLabel);
        
        // I've made this shorter so we can verify it without paging, etc.
        Schedule schedule = new Schedule();
        schedule.setLabel("This is a squence schedule");
        schedule.setScheduleType(ScheduleType.RECURRING);
        schedule.setInterval("P1D");
        schedule.setExpires("P1D");
        schedule.setSequencePeriod("P3D");
        schedule.setTimes(Lists.newArrayList("14:00"));
        schedule.setActivities(taggedActivities);
        strategy.setSchedule(schedule);

        planGuid = schedulesApi.createSchedulePlan(plan).execute().body().getGuid();
        
        user1 = TestUserHelper.createAndSignInUser(getClass(), true);
        ForConsentedUsersApi usersApi = user1.getClient(ForConsentedUsersApi.class);
        
        String scheduledOn1 = LocalDate.now(DateTimeZone.UTC).toString() + "T14:00:00.000Z";
        String scheduledOn2 = LocalDate.now(DateTimeZone.UTC).plusDays(1).toString() + "T14:00:00.000Z";
        String scheduledOn3 = LocalDate.now(DateTimeZone.UTC).plusDays(2).toString() + "T14:00:00.000Z";
        
        ScheduledActivityList scheduledActivityList = usersApi.getScheduledActivities("+00:00", 4, null).execute().body();
        List<ScheduledActivity> list = Tests.filterActivitiesForLabel(scheduledActivityList.getItems(), randomLabel);
        
        assertEquals(3, list.size());
        assertEquals(scheduledOn1, list.get(0).getScheduledOn().toString());
        assertEquals(scheduledOn2, list.get(1).getScheduledOn().toString());
        assertEquals(scheduledOn3, list.get(2).getScheduledOn().toString());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void schedulePlanIsCorrect() throws Exception {
        SchedulesV1Api schedulesApi = developer.getClient(SchedulesV1Api.class);
        planGuid = schedulesApi.createSchedulePlan(Tests.getSimpleSchedulePlan()).execute().body().getGuid();
        
        SchedulePlan originalPlan = Tests.getSimpleSchedulePlan();
        SchedulePlan plan = schedulesApi.getSchedulePlan(planGuid).execute().body();

        // Fields that are set on the server.
        originalPlan.setGuid(plan.getGuid());
        Tests.setVariableValueInObject(originalPlan, "modifiedOn", plan.getModifiedOn());
        originalPlan.setVersion(plan.getVersion());
        
        Tests.getActivitiesFromSimpleStrategy(originalPlan).set(0, Tests.getActivityFromSimpleStrategy(plan));
        
        Tests.setVariableValueInObject(originalPlan, "type", "SchedulePlan");
        
        Schedule originalSchedule = Tests.getSimpleSchedule(originalPlan);
        Schedule updatedSchedule = Tests.getSimpleSchedule(plan);
        originalSchedule.setPersistent(updatedSchedule.isPersistent());
        Tests.setVariableValueInObject(originalSchedule, "type", "Schedule");
        
        originalPlan.setDeleted(false); // Has to be set for equality to work
        assertEquals(originalPlan, plan);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void canRetrieveSchedulesForAUser() throws Exception {
        // Make a schedule plan. Stick a probabilistically unique label on it so we can find it again later.
        // Note: We stick the label on the *schedule*, not the schedule *plan*. This is because the end user only ever
        // sees the schedule, not the schedule plan.
        String label = Tests.randomIdentifier(getClass());
        SchedulePlan schedulePlan = Tests.getSimpleSchedulePlan();
        Tests.getSimpleSchedule(schedulePlan).setLabel(label);

        SchedulesV1Api schedulesApi = developer.getClient(SchedulesV1Api.class);
        planGuid = schedulesApi.createSchedulePlan(schedulePlan).execute().body().getGuid();

        user1 = TestUserHelper.createAndSignInUser(getClass(), true);
        ForConsentedUsersApi usersApi = user1.getClient(ForConsentedUsersApi.class);

        // There may be multiple schedules from other tests. Loop through all schedules until we find the one we're
        // looking for.
        boolean foundSchedule = false;
        List<Schedule> schedules = usersApi.getSchedulesV1().execute().body().getItems();
        for (Schedule oneSchedule : schedules) {
            if (label.equals(oneSchedule.getLabel())) {
                foundSchedule = true;
            }
        }
        assertTrue(foundSchedule);
    }
    
    @SuppressWarnings("deprecation")
    @Test
    public void persistentSchedulePlanMarkedPersistent() throws Exception {
        SchedulePlan plan = Tests.getPersistentSchedulePlan();
        SchedulesV1Api schedulesApi = developer.getClient(SchedulesV1Api.class);
        
        planGuid = schedulesApi.createSchedulePlan(plan).execute().body().getGuid();

        plan = schedulesApi.getSchedulePlan(planGuid).execute().body();
        Schedule schedule = Tests.getSimpleSchedule(plan);
        
        assertEquals(true, schedule.isPersistent());
    }
    
    @SuppressWarnings("deprecation")
    @Test
    public void simpleSchedulePlanNotMarkedPersistent() throws Exception {
        SchedulePlan plan = Tests.getSimpleSchedulePlan();
        SchedulesV1Api schedulesApi = developer.getClient(SchedulesV1Api.class);

        planGuid = schedulesApi.createSchedulePlan(plan).execute().body().getGuid();

        plan = schedulesApi.getSchedulePlan(planGuid).execute().body();
        Schedule schedule = Tests.getSimpleSchedule(plan);
        
        assertEquals(false, schedule.isPersistent());
    }
    
    @SuppressWarnings("deprecation")
    @Test
    public void criteriaBasedScheduleIsFilteredForUser() throws Exception {
        SchedulePlan plan = new SchedulePlan();
        plan.setLabel("Criteria plan");

        String activityLabel1 = Tests.randomIdentifier(getClass());
        Schedule schedule1 = new Schedule();
        schedule1.setLabel("Schedule 1");
        schedule1.setScheduleType(ScheduleType.ONCE);
        schedule1.setActivities(taskActivity(activityLabel1, "task:AAA"));

        String activityLabel2 = Tests.randomIdentifier(getClass());
        Schedule schedule2 = new Schedule();
        schedule2.setLabel("Schedule 2");
        schedule2.setScheduleType(ScheduleType.ONCE);
        schedule2.setActivities(taskActivity(activityLabel2, "task:BBB"));
        
        Criteria criteria1 = new Criteria();
        criteria1.setMinAppVersions(ImmutableMap.of("Android", 0));
        criteria1.setMaxAppVersions(ImmutableMap.of("Android", 10));
        
        ScheduleCriteria scheduleCriteria1 = new ScheduleCriteria();
        scheduleCriteria1.setCriteria(criteria1);
        scheduleCriteria1.setSchedule(schedule1);
        
        Criteria criteria2 = new Criteria();
        criteria2.setMinAppVersions(ImmutableMap.of("Android", 11));
        
        ScheduleCriteria scheduleCriteria2 = new ScheduleCriteria();
        scheduleCriteria2.setCriteria(criteria2);
        scheduleCriteria2.setSchedule(schedule2);
        
        CriteriaScheduleStrategy strategy = new CriteriaScheduleStrategy();
        strategy.setScheduleCriteria(Lists.newArrayList(scheduleCriteria1, scheduleCriteria2));
        plan.setStrategy(strategy);
        
        SchedulesV1Api schedulesApi = developer.getClient(SchedulesV1Api.class);
        planGuid = schedulesApi.createSchedulePlan(plan).execute().body().getGuid();
        
        // Manipulate the User-Agent string and see scheduled activity change accordingly
        user1 = new TestUserHelper.Builder(getClass()).withClientInfo(getClientInfoWithVersion("Android", 2))
                .withConsentUser(true).createAndSignInUser();
        activitiesShouldContainTask(user1,activityLabel1);
        
        user2 = new TestUserHelper.Builder(getClass()).withClientInfo(getClientInfoWithVersion("Android", 12))
                .withConsentUser(true).createAndSignInUser();
        activitiesShouldContainTask(user2, activityLabel2);

        // In this final test no matching occurs, but this simply means that the first schedule will match and be 
        // returned (not that all of the schedules in the plan will be returned, that's not how a plan works).
        user3 = new TestUserHelper.Builder(getClass()).withClientInfo(getClientInfoWithVersion("iPhone OS", 12))
                .withConsentUser(true).createAndSignInUser();
        activitiesShouldContainTask(user3, activityLabel1);
    }
    
    private ClientInfo getClientInfo(String appName, Integer appVersion) {
        ClientInfo info = new ClientInfo();
        info.setAppName(appName);
        info.setAppVersion(appVersion);
        info.setDeviceName("Integration Tests");
        return info;
    }
    
    private List<Activity> taskActivity(String label, String taskIdentifier) {
        TaskReference ref = new TaskReference();
        ref.setIdentifier(taskIdentifier);
        
        Activity activity = new Activity();
        activity.setLabel(label);
        activity.setTask(ref);
        return Lists.newArrayList(activity);
    }

    @SuppressWarnings("deprecation")
    private void activitiesShouldContainTask(TestUser user, String activityLabel) throws Exception {
        // user1 = TestUserHelper.createAndSignInUser(ScheduleTest.class, true);
        ForConsentedUsersApi usersApi = user.getClient(ForConsentedUsersApi.class);
        ScheduledActivityList activities = usersApi.getScheduledActivities("+00:00", 1, null).execute().body();

        // There may be other tasks, but there should only be one task with this label. Loop through tasks and count
        // the number of tasks with this label.
        int numMatchingActivities = 0;
        for (ScheduledActivity oneActivity : activities.getItems()) {
            if (activityLabel.equals(oneActivity.getActivity().getLabel())) {
                numMatchingActivities++;
            }
        }
        assertEquals(1, numMatchingActivities);
    }
    
    private ClientInfo getClientInfoWithVersion(String osName, Integer version) {
        ClientInfo info = new ClientInfo();
        info.setAppName("app");
        info.setAppVersion(version);
        info.setOsName(osName);
        info.setDeviceName("Integrate Tests");
        info.setOsVersion("2.0.0");
        return info;
    }    
}
