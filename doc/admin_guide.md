Metadb Administrator Guide
==========================

##### Contents  
1\. [Overview](#1-overview)  
2\. [System requirements](#2-system-requirements)  
3\. [Installing Metadb](#3-installing-metadb)  
4\. [Running the server](#4-running-the-server)  
5\. [Running the client](#5-running-the-client)  
6\. [Adding a database](#6-adding-a-database)  
7\. [Adding a data source](#7-adding-a-data-source)  
8\. [Resynchronizing a data stream](#8-resynchronizing-a-data-stream)  
9\. [User permissions](#9-user-permissions)  
10\. [Command line](#9-command-line)


1\. Overview
------------

A _Metadb instance_ defines a selection of data sources, data
transforms, and analytic databases.  Metadb stores state for a single
instance in a _data directory_ on the local file system.  Multiple
instances require a separate data directory for each instance.

The software consists of a server (`metadb`) and a command-line client
(`mdb`).


2\. System requirements
-----------------------

* Architecture:  x86-64 (AMD64)
* Operating system:  Ubuntu Linux 20.04 LTS
* Database systems supported:
  * [PostgreSQL](https://www.postgresql.org/) 13.4 or later
* Required to build from source:
  * [Go](https://golang.org/) 1.17 or later
  * [GCC C compiler](https://gcc.gnu.org/) 9.3.0 or later


3\. Installing Metadb
---------------------

### Branches

There are two primary types of branches:

* The main branch (`main`).  This is a development branch where new 
  features are first merged.  It is relatively unstable.  Note that it 
  is also the default view when browsing the repository in GitHub.

* Release branches (`release-*`).  These are releases made from 
  `main`.  They are managed as stable branches; i.e. they may receive 
  bug fixes but generally no new features.  Most users should pull 
  from a recent release branch.

### Building the software

First set the `GOPATH` environment variable to specify a path that can
serve as the build workspace for Go, e.g.:

```bash
export GOPATH=$HOME/go
```

Then:

```bash
./build.sh
```

The `build.sh` script creates a `bin/` subdirectory and builds the
`metadb` and `mdb` executables there:

```bash
./bin/metadb help
```
```
Metadb server

Usage:  metadb <command> <arguments>

Commands:
  start                       - Start server
  stop                        - Shutdown server
  init                        - Initialize new Metadb instance
  upgrade                     - Upgrade a Metadb instance to the current version
  reset                       - Reset database for new snapshot
  clean                       - Remove data from previous reset
  version                     - Print metadb version
  completion                  - Generate command-line completion

Use "metadb help <command>" for more information about a command.
```

```bash
./bin/mdb help
```
```
Metadb client

Usage:  mdb <command> <arguments>

Commands:
  config                      - Configure or show server settings
  user                        - Configure or show database user permissions
  enable                      - Enable database or source connectors
  disable                     - Disable database or source connectors
  version                     - Print mdb version
  completion                  - Generate command-line completion

Use "mdb help <command>" for more information about a command.
```


4\. Running the server
----------------------

### Metadb user account

It is recommended to create a Linux user account that will run the
`metadb` server.  This guide assumes that a user `metadb` has been
created for this purpose and that the commands are run in the user's
home directory.

### Creating a data directory

Metadb stores an instance's state in a data directory, which is
created using `metadb` with the `init` command.  The data directory is
required to be on a local file system; it may not be on a network file
system.  For these examples we will create the data directory as
simply `data`:

```bash
metadb init -D data
```
```
metadb: initializing new instance in data
```

If the directory already exists, `metadb` will exit with an error.

Creating the data directory is generally done only once, when Metadb
is first installed.

### Starting the server

To start the server:

```bash
nohup metadb start -D data -l metadb.log &
```

The server log is by default written to standard error.  The `-l` or 
`--log` option specifies a log file.  The `--csvlog` option writes a 
log in CSV format.

The server listens by default on the loopback address.  This allows 
the command-line client, `mdb`, to connect to the server when running 
locally.

Although not yet fully implemented, the server is planned to support
two ports:

* The "admin port" defaults to 8440.  This provides administrative 
  services, e.g. server configuration.

* The "client port" defaults to 8441.  It is currently unused but is
  planned to support user services.

The `--listen` option allows listening on a specified address.  When
`--listen` is used, the `--cert` and `--key` options also must be
included to provide a server certificate (including the CA's
certificate and intermediates) and matching private key.  As an
alternative, `--notls` may be used in both server and client to
disable TLS entirely; however, this is insecure and for testing
purposes only.

**At present it is recommended that the client and server run on the
same host, until support for authentication is added.**

The `--debug` option enables detailed logging.

### Stopping the server

To stop the server:

```bash
metadb stop -D data
```

It is recommended to stop the server before making a backup of the
data directory.

### Upgrading to a new version

When installing a new version of Metadb, the instance should be
"upgraded" before starting the new server:

a. Stop the old version of the server.

b. Make a backup of the data directory and database(s).

c. Use the `upgrade` command in the new version of Metadb to perform 
   the upgrade, e.g.:

```bash
metadb upgrade -D /usr/local/metadb/data
```

In automated deployments, the `--force` option can be used to disable
prompting.

The upgrade process may take some time, depending on the version.

d. Start the new version of the server.


5\. Running the client
----------------------

By default the `mdb` client tries to connect to the server on the
loopback address.  To specify a different address, the `-h` or
`--host` option may be used, which also will enable TLS unless
`--notls` is included.

The `-v` option enables verbose output.


6\. Adding a database
---------------------

To configure a connector for an analytical database that will receive
data from Metadb, for example:

```bash
mdb config db.main.type postgresql
mdb config db.main.host dbserver
mdb config db.main.port 5432
mdb config db.main.dbname metadb
mdb config db.main.adminuser metadbadmin
mdb config db.main.adminpassword @admincred.txt
mdb config db.main.superuser admin
mdb config --pwprompt db.main.superpassword
mdb config db.main.sslmode require
mdb enable db.main
```

The connector in this example is named `main`.  Database connector
names are prefixed with `db.`.

Note that the `@` in `@admincred.txt` means that the password will be
read from the file `admincred.txt`.


7\. Adding a data source
------------------------

A source connector defines a Kafka source that Metadb will read data
from, for example:

```bash
mdb config src.example.brokers kafka:29092
mdb config src.example.topics '^metadb_example[.].*'
mdb config src.example.group metadb_example
mdb config src.example.schemapassfilter 'example_.+'
mdb config src.example.schemaprefix 'example_'
mdb config src.example.dbs main
mdb enable src.example
```

Here the connector is named `example`.  Source connector names are
prefixed with `src.`.

When Metadb stores data via the database connector, it will tag
records with the source connector name.  It is a good idea to choose a
short name that meaningfully identifies the source.


8\. Resynchronizing a data stream
---------------------------------

If a Kafka data stream fails and cannot be resumed, it may be
necessary to re-stream data to Metadb.  For example, a source database
may become unsynchronized with the analytic database, requiring a new
snapshot of the source database to be streamed.  Metadb can accept
re-streamed data in order to resynchronize with the source, using this
procedure:

a. Disable the source connector, for example:

```bash
mdb disable src.example
```

b. Update the source connector's `topics` and `group` configuration
   settings for the new data stream, or temporarily delete them or set
   them to empty strings.

```bash
mdb config src.example.topics ''
mdb config src.example.group ''
```

c. Stop the Metadb server.

d. "Reset" the analytic database to mark current data as old.  This
   may take some time to run.

```bash
metadb reset -D /usr/local/metadb/data --origin 's1,s2,s3' db.main
```

Note that `--origin` should include all origins associated with the
source, or empty string ( `''` ) if there are no origins.

e. Start the Metadb server, configure and enable the source connector
   for the new stream, and begin streaming the data.

f. Once the new data have finished streaming, stop the Metadb server,
   and "clean" the analytic database to remove old data.

```bash
metadb clean -D /usr/local/metadb/data --origin 's1,s2,s3' db.main
```

The `--origin` should be the same as used for the reset.

Note that the metadb server currently does not give any indication
that it has finished re-streaming, except that running it with
`--debug` will typically show updates slowing down.  The precise
timing when "metadb clean" is run is not critical, but it is
preferable to run it late rather than early.  (Having the server
report that initial streaming or re-streaming has finished is a
planned feature.)

g. Start the server.

Until a failed stream is re-streamed by following the process above,
the analytic database may continue to be unsynchronized with the
source.


9\. User permissions
--------------------

The `user` command enables database users to access the tables managed
by Metadb.  For example to grant read-only permissions to a database
user `ada`:

```bash
mdb user ada
```
```
mdb: user: updated "ada"
```

A variation of this command also creates the database user and creates
a schema for the user, in addition to granting the permissions:

```bash
mdb user -c jim
```
```
Password for "jim":
mdb: user: created "jim"
```

It is recommended that short user names be used, ideally three
letters.

To list all users that have access:

```bash
mdb user -l
```
```
ada
jim
```

To revoke access permissions:

```bash
mdb user -d jim
```
```
mdb: user: deleted permissions for "jim"
```

Support for table-specific permissions is planned for a future
version.


10\. Command line
-----------------

### Color

To enable color when writing to the standard error stream, set the
environment variable `METADB_COLOR` to:

*  `auto`: Color is automatically enabled for terminal output but
   otherwise disabled, such as when the output is piped to another
   process.

* `always`: Color is always enabled.

* In all other cases, color is disabled.

### Command-line completion

Comand-line completion scripts are available via the `completion`
command, e.g. for bash:

```bash
source < (mdb completion bash)
```

