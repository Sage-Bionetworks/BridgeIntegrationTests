package org.sagebionetworks.bridge.sdk.integration;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.sagebionetworks.bridge.rest.model.Role.STUDY_DESIGNER;
import static org.sagebionetworks.bridge.sdk.integration.Tests.STUDY_ID_1;

import java.io.IOException;

import org.apache.http.HttpResponse;
import org.apache.http.client.fluent.Request;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sagebionetworks.bridge.rest.api.ForStudyDesignersApi;
import org.sagebionetworks.bridge.rest.api.SchedulesV2Api;
import org.sagebionetworks.bridge.rest.api.StudiesApi;
import org.sagebionetworks.bridge.rest.model.Schedule2;
import org.sagebionetworks.bridge.rest.model.Study;
import org.sagebionetworks.bridge.user.TestUser;
import org.sagebionetworks.bridge.user.TestUserHelper;

import com.google.common.net.HttpHeaders;

import retrofit2.Response;

public class EtagsTest {

    private static final String PARTICIPANT_SELF_TIMELINE_URL = "/v5/studies/"+STUDY_ID_1+"/participants/self/timeline";
    private static final String PARTICIPANT_TIMELINE_URL = "/v5/studies/"+STUDY_ID_1+"/participants/%s/timeline";
    private static final String TIMELINE_URL = "/v5/studies/"+STUDY_ID_1+"/timeline";
    private static final String SCHEDULE_URL = "/v5/studies/"+STUDY_ID_1+"/schedule";
    
    TestUser studyDesigner;
    TestUser user;
    Schedule2 schedule;
    String hostUrl;
    
    @Before
    public void before() throws Exception {
        user = TestUserHelper.createAndSignInUser(getClass(), true);
        studyDesigner = TestUserHelper.createAndSignInUser(getClass(), false, STUDY_DESIGNER);
        hostUrl = studyDesigner.getClientManager().getHostUrl();
        
        // If there's a schedule associated to study 1, we need to delete it.
        TestUser admin = TestUserHelper.getSignedInAdmin();
        StudiesApi studiesApi = admin.getClient(StudiesApi.class);
        Study study = studiesApi.getStudy(STUDY_ID_1).execute().body();
        if (study.getScheduleGuid() != null) {
            admin.getClient(SchedulesV2Api.class).deleteSchedule(study.getScheduleGuid()).execute();
        }
    }
    
    @After
    public void after() throws Exception {
        TestUser admin = TestUserHelper.getSignedInAdmin();
        SchedulesV2Api adminSchedulesApi = admin.getClient(SchedulesV2Api.class);
        
        if (schedule != null && schedule.getGuid() != null) {
            adminSchedulesApi.deleteSchedule(schedule.getGuid()).execute();    
        }
        if (user != null) {
            user.signOutAndDeleteUser();
        }
        if (studyDesigner != null) {
            studyDesigner.signOutAndDeleteUser();
        }
    }

    @Test
    public void crudWorks() throws Exception {
        // create a schedule
        SchedulesV2Api schedulesApi = studyDesigner.getClient(SchedulesV2Api.class);
        schedule = new Schedule2();
        schedule.setName("Test Schedule [EtagsTest]");
        schedule.setDuration("P10W");
        
        schedule = schedulesApi.saveScheduleForStudy(STUDY_ID_1, schedule).execute().body();
        
        ForStudyDesignersApi designApi = studyDesigner.getClient(ForStudyDesignersApi.class);
        
        // First request is a 200, but it has an etag
        Response<Schedule2> res1 = designApi.getScheduleForStudy(STUDY_ID_1).execute();
        assertEquals(200, res1.code());
        String etag = res1.headers().get(HttpHeaders.ETAG);
        assertNotNull(etag);
        
        // Request it again through any API, you get a 304.
        assertStatus(studyDesigner, SCHEDULE_URL, etag, 304);
        assertStatus(studyDesigner, TIMELINE_URL, etag, 304);
        assertStatus(studyDesigner, format(PARTICIPANT_TIMELINE_URL, user.getUserId()), etag, 304);
        assertStatus(user, PARTICIPANT_SELF_TIMELINE_URL, etag, 304);
        
        // Change the schedule, the cache is busted
        schedule.setDuration("P11D");
        schedule = schedulesApi.saveScheduleForStudy(STUDY_ID_1, schedule).execute().body();
        
        // Now you get the content and a 200
        assertStatus(studyDesigner, SCHEDULE_URL, etag, 200);
        assertStatus(studyDesigner, TIMELINE_URL, etag, 200);
        assertStatus(studyDesigner, format(PARTICIPANT_TIMELINE_URL, user.getUserId()), etag, 200);
        assertStatus(user, PARTICIPANT_SELF_TIMELINE_URL, etag, 200);
        
        // Get this new etag
        Response<Schedule2> res2 = designApi.getScheduleForStudy(STUDY_ID_1).execute();
        etag = res2.headers().get(HttpHeaders.ETAG);
        
        // Deleting the schedule clears the cache too (which was just set) so we see 404 despite 
        // sending the last etag
        TestUser admin = TestUserHelper.getSignedInAdmin();
        admin.getClient(SchedulesV2Api.class).deleteSchedule(schedule.getGuid()).execute();
        schedule = null;

        assertStatus(studyDesigner, SCHEDULE_URL, etag, 404);
        assertStatus(studyDesigner, TIMELINE_URL, etag, 404);
        assertStatus(studyDesigner, format(PARTICIPANT_TIMELINE_URL, user.getUserId()), etag, 404);
        assertStatus(user, PARTICIPANT_SELF_TIMELINE_URL, etag, 404);
    }
    
    private void assertStatus(TestUser caller, String url, String etag, int statusCode) throws IOException { 
        HttpResponse response = Request.Get(hostUrl + url)
            .setHeader("Bridge-Session", caller.getSession().getSessionToken())
            .setHeader(HttpHeaders.IF_NONE_MATCH, etag)
            .execute().returnResponse();
        assertEquals(statusCode, response.getStatusLine().getStatusCode());        
    }
}
