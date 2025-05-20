# v1.4.0-pre2

* Kafka reader concurrency has been temporarily disabled.

* The folio-analytics tag has been updated to v1.7.15.

# v1.4.0-pre1

* JSON transformation has been extended to include objects and arrays,
  and it is now disabled by default in new installations.  The `CREATE
  DATA MAPPING` command can be used to enable and configure
  transformation of specific JSON paths.  The `LIST data_mappings`
  command shows configured mappings.

* New commands `GRANT` and `REVOKE` improve support for managing user
  privileges.

* A new command `CREATE SCHEMA` supports creating a user schema for an
  existing user.

* New commands `REGISTER USER` and `DEREGISTER USER` support adding an
  existing database user to a Metadb instance.

* A new function `mdbusers()` lists registered users.

* A new command `DROP USER` supports removing users.

* The `METADB_FOLIO` environment variable can now be defined when
  building Metadb to override the default folio reference, for
  example:

  ```
  METADB_FOLIO="refs/tags/v1.7.7" ./build
  ```

* The command `CREATE DATA MAPPING` no longer requires restarting the
  server before it takes effect.

* The compilation script `build.sh` has been renamed to `build`.

* Go 1.24 is now required to build Metadb.

* Various performance improvements and bug fixes.
