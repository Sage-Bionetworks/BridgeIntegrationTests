package org.sagebionetworks.bridge.sdk.integration;

import static org.junit.Assert.assertEquals;

import java.util.List;

import com.google.common.collect.ImmutableList;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.sagebionetworks.bridge.rest.api.ForSuperadminsApi;
import org.sagebionetworks.bridge.rest.api.ParticipantsApi;
import org.sagebionetworks.bridge.rest.model.ClientInfo;
import org.sagebionetworks.bridge.rest.model.RequestInfo;
import org.sagebionetworks.bridge.user.TestUser;
import org.sagebionetworks.bridge.user.TestUserHelper;

public class RequestInfoTest {
    private static final DateTime ACTIVITIES_ACCESSED_ON = DateTime.parse("2022-09-04T07:58:56.102-04:00");
    private static final List<String> LANGUAGES = ImmutableList.of("en","fr");
    private static final DateTime SIGNED_IN_ON = DateTime.parse("2022-09-03T02:17:42.629-04:00");
    private static final DateTime TIMELINE_ACCESSED_ON = DateTime.parse("2022-09-18T14:18:03.313-04:00");
    private static final String TIME_ZONE_STRING = "-04:00";
    private static final String USER_AGENT = "RequestInfoTest/1 (BridgeIntegrationTests; Java/1.8.0) BridgeJavaSDK/25";
    private static final String USER_AGENT_DUPLICATED =
            "RequestInfoTest/2 (BridgeIntegrationTests; Java/1.8.0) BridgeJavaSDK/25,RequestInfoTest/2 (BridgeIntegrationTests; Java/1.8.0) BridgeJavaSDK/25";
    private static final DateTime UPLOADED_ON = DateTime.parse("2022-09-20T17:26:17.181-04:00");

    private static ParticipantsApi participantsApi;
    private static ForSuperadminsApi superadminApi;

    private TestUser user;
    private String userId;

    @BeforeClass
    public static void beforeClass() {
        TestUser admin = TestUserHelper.getSignedInAdmin();
        participantsApi = admin.getClient(ParticipantsApi.class);
        superadminApi = admin.getClient(ForSuperadminsApi.class);
    }

    @Before
    public void before() throws Exception {
        user = TestUserHelper.createAndSignInUser(RequestInfoTest.class, true);
        userId = user.getUserId();
    }

    @After
    public void deleteUser() throws Exception {
        if (user != null) {
            user.signOutAndDeleteUser();
        }
    }

    @Test
    public void test() throws Exception {
        // Read request info from Bridge. Save a few values so that we can verify them later.
        RequestInfo requestInfo = participantsApi.getParticipantRequestInfo(userId).execute().body();
        List<String> dataGroups = requestInfo.getUserDataGroups();
        List<String> studyIds = requestInfo.getUserStudyIds();

        // Write some values to Request Info and save it back to Bridge Server.
        requestInfo.setActivitiesAccessedOn(ACTIVITIES_ACCESSED_ON);
        requestInfo.setLanguages(LANGUAGES);
        requestInfo.setSignedInOn(SIGNED_IN_ON);
        requestInfo.setTimelineAccessedOn(TIMELINE_ACCESSED_ON);
        requestInfo.setTimeZone(TIME_ZONE_STRING);
        requestInfo.setUploadedOn(UPLOADED_ON);
        requestInfo.setUserAgent(USER_AGENT);
        superadminApi.updateParticipantRequestInfo(userId, requestInfo).execute();

        // Get the request info back and validate the values.
        requestInfo = participantsApi.getParticipantRequestInfo(userId).execute().body();
        assertEquals(ACTIVITIES_ACCESSED_ON, requestInfo.getActivitiesAccessedOn());
        assertEquals(LANGUAGES, requestInfo.getLanguages());
        assertEquals(SIGNED_IN_ON, requestInfo.getSignedInOn());
        assertEquals(TIMELINE_ACCESSED_ON, requestInfo.getTimelineAccessedOn());
        assertEquals(TIME_ZONE_STRING, requestInfo.getTimeZone());
        assertEquals(UPLOADED_ON, requestInfo.getUploadedOn());
        assertEquals(USER_AGENT, requestInfo.getUserAgent());
        assertEquals(dataGroups, requestInfo.getUserDataGroups());
        assertEquals(userId, requestInfo.getUserId());
        assertEquals(studyIds, requestInfo.getUserStudyIds());

        // Server automatically parses ClientInfo.
        ClientInfo clientInfo = requestInfo.getClientInfo();
        assertEquals("RequestInfoTest", clientInfo.getAppName());
        assertEquals(1, clientInfo.getAppVersion().intValue());
        assertEquals("BridgeIntegrationTests", clientInfo.getDeviceName());
        assertEquals("Java", clientInfo.getOsName());
        assertEquals("1.8.0", clientInfo.getOsVersion());
        assertEquals("BridgeJavaSDK", clientInfo.getSdkName());
        assertEquals(25, clientInfo.getSdkVersion().intValue());

        // BRIDGE-3349 Write a duplicated User-Agent string and verify that this parses correctly. Note that this
        // User-Agent string is also slightly different from the one before.
        requestInfo.setUserAgent(USER_AGENT_DUPLICATED);
        superadminApi.updateParticipantRequestInfo(userId, requestInfo).execute();

        // Get the request info back and validate changed values.
        requestInfo = participantsApi.getParticipantRequestInfo(userId).execute().body();
        assertEquals(USER_AGENT_DUPLICATED, requestInfo.getUserAgent());

        clientInfo = requestInfo.getClientInfo();
        assertEquals("RequestInfoTest", clientInfo.getAppName());
        assertEquals(2, clientInfo.getAppVersion().intValue());
        assertEquals("BridgeIntegrationTests", clientInfo.getDeviceName());
        assertEquals("Java", clientInfo.getOsName());
        assertEquals("1.8.0", clientInfo.getOsVersion());
        assertEquals("BridgeJavaSDK", clientInfo.getSdkName());
        assertEquals(25, clientInfo.getSdkVersion().intValue());
    }
}
