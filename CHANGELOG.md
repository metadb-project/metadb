# v1.4.0

* JSON transformation has been extended to support objects and arrays.
  New commands `create data mapping` and `drop data mapping` can be
  used to define or disable transformation of objects and arrays at
  specific JSON paths.  A new command `list data_mappings` shows
  configured mappings.

* JSON transformation is now disabled by default in new installations.

* New commands `grant` and `revoke` improve support for managing user
  privileges.

* New commands `alter system` and `list config` support defining and
  viewing server configuration parameters.

* New configuration parameters `checkpoint_segment_size` and
  `max_poll_interval` can be used to configure the operation of the
  stream processor.

* A new command `purge data` supports removing tables.

* A new command `drop user` supports removing users.

* A new command `create schema` supports creating a user schema for an
  existing user.

* New commands `register user` and `deregister user` support adding an
  existing database user to a Metadb instance.

* A new function `mdbusers()` lists registered users.

* A new data source option `map_public_schema` supports changing the
  schema name of tables that originate in the `public` schema.

* The command `create data origin` no longer requires restarting the
  server before it takes effect.

* The compilation script `build.sh` has been renamed to `build`.

* Go 1.25.2 is now required to build Metadb.

* Improvements in error logging.  Various performance improvements and
  bug fixes.

* The folio-analytics and folio-reshare tags are no longer defined by
  default in new installations.  The command `alter system` can be
  used to set them via the configuration parameters
  `external_sql_folio` and `external_sql_reshare`.

