# BridgeIntegrationTests

Integration tests for the BridgeServer2 server. Integration tests require a bootstrap account on the server to execute. Outside of production, the full suite of tests runs using  `SUPERADMIN` account. On production, for security reasons, a subset of these tests execute using an `ADMIN` account. (The `ADMIN` account can’t access all apps, or create worker accounts that can access all apps.)

The production tests are marked with the `@Category(IntegrationSmokeTest.class)` class annotation.

## Setting up your environment to run integration tests

The integration tests use the Bridge Java REST SDK, and both the SDK and the tests need some properties to be set in order to run (in the following property files):

**~/bridge-sdk.properties**

    log.level = warn
    env = local
    languages = en

**~/bridge-sdk-test.properties**

    dev.name = <dev name>

    admin.email = <email address>
    admin.password = <password>

    synapse.test.user = <user name>
    synapse.test.user.id = <numerical account ID>
    synapse.test.user.password = <password>
    synapse.test.user.api.key = <api key>

**admin.email** and **admin.password:** Create an account with the `SUPERADMIN` role outside of production and the `ADMIN` role in production. This account should be created in the 'api' and 'shared' apps, with the same email and password, and the same synapse user ID of 0000 (four zeroes) in both apps (this allows an `ADMIN` account to to switch between these two studies). These are test apps, so accidents in these apps would not impact other production apps.

The **synapse.test.*** keys are available in LastPass.

It should be possible to start the server and run `mvn clean test` at this point.
