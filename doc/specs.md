Miscellaneous Specifications
============================


Data type conversions
---------------------

When the data type of a source table column changes or is not
supported by Metadb, the receiving table is automatically adjusted to
be able to accept the data.  If no similar data type is supported, the
column is converted to varchar.  This table summarizes the type
conversions that are performed:

| Data type conversions        | To numeric | To uuid  | To jsonb | To varchar |
| ---------------------------- |:----------:|:--------:|:--------:|:----------:|
| From boolean                 |            |          |          |     ✅     |
| From smallint                |     ✅     |          |          |     ✅     |
| From integer                 |     ✅     |          |          |     ✅     |
| From bigint                  |     ✅     |          |          |     ✅     |
| From real                    |     ✅     |          |          |     ✅     |
| From double precision        |     ✅     |          |          |     ✅     |
| From numeric                 |            |          |          |     ✅     |
| From date                    |            |          |          |     ✅     |
| From time                    |            |          |          |     ✅     |
| From time with timezone      |            |          |          |     ✅     |
| From timestamp               |            |          |          |     ✅     |
| From timestamp with timezone |            |          |          |     ✅     |
| From uuid                    |            |          |          |     ✅     |
| From json                    |            |          |    ✅    |     ✅     |
| From jsonb                   |            |          |          |     ✅     |
| From varchar                 |            |    ✅    |          |            |

