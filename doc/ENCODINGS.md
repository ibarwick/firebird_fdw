PostgreSQL and Firebird character set encoding compatibility
============================================================

Character set mappings
----------------------

The following table provides an overview of available PostgreSQL server
character set encodings and the matching Firebird ones.

Encodings marked with `-` in the Firebird column are not available in Firebird.

| PostgreSQL    | Firebird  | Notes
|---------------|-----------|--------------------------------------------
| EUC_CN        | -         | Extended UNIX Code-CN
| EUC_JP        | EUCJ_0208 | Compatibility likely but not tested
| EUC_JIS_2004  | -         | Extended UNIX Code-JP, JIS X 0213
| EUC_KR        | -         | Extended UNIX Code-KR
| EUC_TW        | -         | Extended UNIX Code-TW
| ISO_8859_5    | ISO8859_5 |
| ISO_8859_6    | ISO8859_6 |
| ISO_8859_7    | ISO8859_7 |
| ISO_8859_8    | ISO8859_8 |
| KOI8R         | KOI8R     |
| KOI8U         | KOI8U     |
| LATIN1        | LATIN1    |
| LATIN2        | LATIN2    |
| LATIN3        | LATIN3    |
| LATIN4        | LATIN4    |
| LATIN5        | LATIN5    |
| LATIN6        | -         | ISO 8859-10 / ECMA 144 "Nordic"
| LATIN7        | LATIN7    |
| LATIN8        | -         | ISO 8859-14 "Celtic"
| LATIN9        | -         | ISO 8859-15 "LATIN1 with Euro and accents"
| LATIN10       | -         | ISO 8859-16 "Romanian"
| MULE_INTERNAL | -         | "Multilingual Emacs"
| SQL_ASCII     | NONE      |
| UTF8          | UTF8      |
| WIN866        | DOS866    |
| WIN874        | -         | Windows CP874 "Thai"
| WIN1250       | WIN1250   |
| WIN1251       | WIN1251   |
| WIN1252       | WIN1252   |
| WIN1253       | WIN1253   |
| WIN1254       | WIN1254   |
| WIN1255       | WIN1255   |
| WIN1256       | WIN1256   |
| WIN1257       | WIN1257   |
| WIN1258       | WIN1258   |

See also:

- https://www.postgresql.org/docs/current/multibyte.html#MULTIBYTE-CHARSET-SUPPORTED
- https://firebirdsql.org/file/documentation/html/en/refdocs/fblangref40/firebird-40-language-reference.html#fblangref40-appx07-charsets
- https://firebirdsql.org/refdocs/langrefupd25-charsets.html
- https://firebirdsql.org/en/firebird-1-5-character-sets-collations/

Databases with the Firebird "NONE" character set
------------------------------------------------

The `NONE` character set is pretty much the equivalent of PostgreSQL's
`SQL_ASCII`, i.e. a pseudo-character set/encoding which enables the user to
store much any data they care to input without any kind of validation.  This
means that it's perfectly possible to insert a mix of data in (for example)
`ISO-8859-1` and `UTF8` encoding.

This does however mean that Firebird can't know what encoding the data is
supposed to be in, so it can't convert the data to whatever encoding the client
is requesting. Conseqeuently, when `firebird_fdw` connects to a Firebird
database configured with the `NONE` character set, the PostgreSQL database's
server encoding has no meaning, and the raw data will be transmitted to
PostgreSQL.

If the data happens to be in the same encoding as the PostgreSQL databases's server
encoding, this is normally not an issue. However, if the data is in a different
encoding, it will need to be treated as a stream of `bytea` values which need to
be explictly converted using e.g. PostgreSQL's `convert_from()` function, e.g.:

    SELECT convert_from(some_column_name, 'LATIN1')
      FROM firebird_table

where `firebird_table` is a foreign table in a PostgreSQL database with `UTF8`
server encoding which references a table in a Firebird database with `NONE`
pseudo-encoding containing data in `LATIN1` encoding.

