package org.sagebionetworks.bridge.sdk.integration;

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
        
        if (schedule != null) {
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
        schedule.setName("Test Schedule [Schedule2Test]");
        schedule.setDuration("P10W");
        schedule = schedulesApi.saveScheduleForStudy(STUDY_ID_1, schedule).execute().body();
        
        ForStudyDesignersApi designApi = studyDesigner.getClient(ForStudyDesignersApi.class);
        
        Response<Schedule2> res1 = designApi.getScheduleForStudy(STUDY_ID_1).execute();
        assertEquals(200, res1.code());
        String etag = res1.headers().get("ETag");
        assertNotNull(etag);
        
        assertStatus("/v5/studies/"+STUDY_ID_1+"/schedule", etag, 304);
        assertStatus("/v5/studies/"+STUDY_ID_1+"/timeline", etag, 304);
        assertStatus("/v5/studies/"+STUDY_ID_1+"/participants/"+user.getUserId()+"/timeline", etag, 304);
        assertUserStatus("/v5/studies/"+STUDY_ID_1+"/participants/self/timeline", etag, 304);
        
        // Change the schedule, the cache is busted
        schedule.setDuration("P11D");
        schedule = schedulesApi.saveScheduleForStudy(STUDY_ID_1, schedule).execute().body();
        
        assertStatus("/v5/studies/"+STUDY_ID_1+"/schedule", etag, 200);
        assertStatus("/v5/studies/"+STUDY_ID_1+"/timeline", etag, 200);
        assertStatus("/v5/studies/"+STUDY_ID_1+"/participants/"+user.getUserId()+"/timeline", etag, 200);
        assertUserStatus("/v5/studies/"+STUDY_ID_1+"/participants/self/timeline", etag, 200);
        
        Response<Schedule2> res2 = designApi.getScheduleForStudy(STUDY_ID_1).execute();
        etag = res2.headers().get("ETag");
        
        // Deleting the schedule clears the cache too (which was just set) so we see 404 despite sending the
        // etag.
        TestUser admin = TestUserHelper.getSignedInAdmin();
        admin.getClient(SchedulesV2Api.class).deleteSchedule(schedule.getGuid()).execute();
        schedule = null;

        assertStatus("/v5/studies/"+STUDY_ID_1+"/schedule", etag, 404);
        assertStatus("/v5/studies/"+STUDY_ID_1+"/timeline", etag, 404);
        assertStatus("/v5/studies/"+STUDY_ID_1+"/participants/"+user.getUserId()+"/timeline", etag, 404);
        assertUserStatus("/v5/studies/"+STUDY_ID_1+"/participants/self/timeline", etag, 404);
    }
    
    private void assertStatus(String url, String etag, int statusCode) throws IOException { 
        HttpResponse response = Request.Get(hostUrl + url)
            .setHeader("Bridge-Session", studyDesigner.getSession().getSessionToken())
            .setHeader(HttpHeaders.IF_NONE_MATCH, etag)
            .execute().returnResponse();
        assertEquals(statusCode, response.getStatusLine().getStatusCode());        
    }

    private void assertUserStatus(String url, String etag, int statusCode) throws IOException { 
        HttpResponse response = Request.Get(hostUrl + url)
            .setHeader("Bridge-Session", user.getSession().getSessionToken())
            .setHeader(HttpHeaders.IF_NONE_MATCH, etag)
            .execute().returnResponse();
        assertEquals(statusCode, response.getStatusLine().getStatusCode());        
    }
}
