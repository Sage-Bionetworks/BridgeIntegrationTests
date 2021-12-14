# BridgeIntegrationTests

Integration tests for the BridgeServer2 server. Integration tests require bootstrap accounts on the server to execute. All tests run outside of production using a `SUPERADMIN` account. In production, for security reasons, a subset of these tests execute using an `ADMIN` account (marked with the `@Category(IntegrationSmokeTest.class)` class annotation). The `ADMIN` account can’t access all apps, or create worker accounts that can access all apps.

## Setting up your environment to run integration tests

The integration tests use the Bridge Java REST SDK, and both the SDK and the tests need some properties to be set in order to run (in the following property files):

**~/bridge-sdk.properties**

    log.level = warn
    env = local
    languages = en

**~/bridge-sdk-test.properties**

    dev.name = <dev name>

    synapse.test.user = <user name>
    synapse.test.user.id = <numerical account ID>
    synapse.test.user.password = <password>
    synapse.test.user.api.key = <api key>

Create two bootstrap accounts in each environment, one in the 'api' app and one in the 'shared' app. Each account should have the `synapseUserId` ID set in the `synapse.test.user.id` property. In production these accounts should have the `ADMIN` role and in other environments, these accounts should have the `SUPERADMIN` role.

The **synapse.test.*** keys for environments other than `local` are available in LastPass.

It should be possible at this point to start the server and run the tests (`mvn clean test`).
