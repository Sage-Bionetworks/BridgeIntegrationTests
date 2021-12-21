package org.sagebionetworks.bridge.sdk.integration;

import static org.junit.Assert.*;

import org.junit.Test;
import org.sagebionetworks.bridge.user.TestUser;
import org.sagebionetworks.bridge.user.TestUserHelper;

public class BootstrapAdminTest {
    
    @Test
    public void test() throws Exception {
        TestUser admin = TestUserHelper.getSignedInAdmin();
        
        try {
            admin.signOut();
            fail("Should have thrown an exception");
        } catch(UnsupportedOperationException e) {
        }
        try {
            admin.signOutAndDeleteUser();
            fail("Should have thrown an exception");
        } catch(UnsupportedOperationException e) {
        }
        try {
            admin.signInAgain();
            fail("Should have thrown an exception");
        } catch(UnsupportedOperationException e) {
        }
    }

}
