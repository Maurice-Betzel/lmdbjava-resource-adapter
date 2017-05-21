## LMDb JCA Resource Adapter

A JCA 1.7 standard resource provider for [lmdbjava](https://github.com/lmdbjava/lmdbjava).

This resource adapter plugs into any JavaEE implementation and provides connectivity, security and transactions
between lmdbjava, the JavaEE implementation, and the consuming application. It is implemented as a passive library,
only supporting outbound communication. Therefore all interactions are executed in the context of the application thread.

Before running the tests, set the config-property named filePath in the file "ironjacamar.xml" to a folder where the
database files are allowed to be created.

Wildfly testing only works on Windows OS because auf vm crash with jffi so binaries on linux on RA test deploy.

### License

This project is licensed under the
[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html)

This project uses IronJacamar for testing purposes, which is licenced under:

[Eclipse Public License v1.0 (EPL 1.0)](http://www.eclipse.org/legal/epl-v10.html "EPL v1.0")

This project depends on LMDB, which is licensed under:

[The OpenLDAP Public License](http://www.openldap.org/software/release/license.html)
