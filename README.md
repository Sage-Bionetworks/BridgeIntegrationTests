# BridgeIntegrationTests

Integration tests for the BridgeServer2 server. 

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

The **synapse.test.*** keys for environments other than `local` are available in LastPass.

## Server configuration

On startup, the `BridgeServer2` server will create the following apps and accounts on start-up if they do not currently exist on the server in your environment:

**In all environments** the bootstrapper creates three apps with the IDs of `api`, `api-2`, and `shared`. The API apps are used solely for testing, while the `shared` app is used to test shared assessment functionality outside of production. In production, `shared` is only used to host our shared assessment library.

It also reads the `admin.email` and `admin.synapse.user.id` properties in the `~/BridgeServer2.conf` file. The `admin.synapse.user.id` property should have the same number as the `synapse.test.user.id` property described above.

**In local, development, and staging environments:** the bootstrapper creates three admin accounts, one in each of the `api`,  `api-2`, and `shared` apps. The accounts will have the role of `SUPERADMIN`.

**In the production environment:** the bootstrapper creates three admin accounts, one in each of the `api`,  `api-2`, and `shared` apps. The accounts will have the role of `ADMIN` (**NOT** `SUPERADMIN`).

Once the Bridge server has started, it should be possible to run the test suite with `mvn clean test`.
