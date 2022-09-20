# Recommendations for character data compatibility between Firebird and PostgreSQL

## Compability between character data *collations* and *character sets* in Firebird and *encodings* (or character setes) in PostgreSQL

You can search by *collation* to corresponding *character set* and then to PostgreSQL's *encoding* if exist.
There is many Firebird's character sets, haven't corresponding encoding in PostgreSQL, but **any Firebird's character set can be converted to UTF8** encoding in PostgreSQL.

|FB character set|pg encoding|fbID|fb Collation|Description|fb Aliases|fb maxBytes/Char|FbVersion+|JavaCharSet|
|----------------|-----------|----|------------|-----------|----------|-----------------|----------|-----------|
|**NONE**|**SQL_ASCII**|0|**NONE** |**Codepage-neutral. Uppercasing limited to ASCII codes 97-122**||1 ||ASCII|
|**UTF8**|**UTF8**|3|**UNICODE**|**8-bit Unicode Transformation Format**|SQL_TEXT, UTF-8|3 |2.0|UTF-8|
|OCTETS|SQL_ASCII|1|OCTETS ||BINARY|1 |1.0||
|ASCII|SQL_ASCII|2|ASCII ||ASCII7, USASCII|1 |1.0|ASCII|
|SJIS_0208|SJIS|5|SJIS_0208 ||SJIS|2 |1.0||
|EUCJ_0208||6|EUCJ_0208 |EUC Japanese: JIS X 0201, 0208, 0212, EUC encoding, Japanese|EUCJ|2 |1.0|EUC_JP|
|DOS737||9|DOS737 |MS-DOS: Greek|DOS_737|1 |1.5|Cp737|
|DOS437||10|DOS437 |MS-DOS: United States, Australia, New Zeland, South Africa|DOS_437|1 |1.0|Cp437|
|||10|DB_DEU437 ||||||
|||10|DB_ESP437 ||||||
|||10|DB_FIN437 ||||||
|||10|DB_FRA437 ||||||
|||10|DB_ITA437 ||||||
|||10|DB_NLD437 ||||||
|||10|DB_SVE437 ||||||
|||10|DB_UK437 ||||||
|||10|DB_US437 ||||||
|||10|PDOX_ASCII ||||||
|||10|PDOX_INTL ||||||
|||10|PDOX_SWEDFIN ||||||
|DOS850||11|DOS850 |MS-DOS: Latin-1|DOS_850|1 |1.0|Cp850|
|||11|DB_DEU850 ||||||
|||11|DB_ESP850 ||||||
|||11|DB_FRA850 ||||||
|||11|DB_FRC850 ||||||
|||11|DB_ITA850 ||||||
|||11|DB_NLD850 ||||||
|||11|DB_PTB850 ||||||
|||11|DB_SVE850 ||||||
|||11|DB_UK850 ||||||
|||11|DB_US850 ||||||
|DOS865||12|DOS865 |MS-DOS: Nordic|DOS_865|1 |1.0|Cp865|
|||12|DB_DAN865 ||||||
|||12|DB_NOR865 ||||||
|||12|PDOX_NORDAN4 ||||||
|DOS860||13|DOS860 |MS-DOS: Portuguese|DOS_860|1 |1.0|Cp860|
|||13|DB_PTG860 ||||||
|DOS863||14|DOS863 |MS-DOS: Canadian French|DOS_863|1 |1.0|Cp863|
|||14|DB_FRC863 ||||||
|DOS775||15|DOS775 |MS-DOS: Baltic|DOS_775|1 |1.5|Cp775|
|DOS858||16|DOS858 |IBM: Latin-1 + €|DOS_858|1 |1.5|Cp858|
|DOS862||17|DOS862 |IBM: Hebrew|DOS_862|1 |1.5|Cp862|
|DOS864||18|DOS864 |IBM: Arabic|DOS_864|1 |1.5|Cp864|
|NEXT||19|NEXT |||1 |||
|||19|NXT_DEU ||||||
|||19|NXT_ESP ||||||
|||19|NXT_FRA ||||||
|||19|NXT_ITA ||||||
|||19|NXT_US ||||||
|ISO8859_1|LATIN1|21|ISO8859_1 |Latin 1  * ISO 8859-1, Latin alphabet No. 1|ANSI, ISO88591, LATIN1|1 |1.0|ISO-8859-1|
|||21|DA_DA ||||||
|||21|DE_DE ||||||
|||21|DU_NL ||||||
|||21|EN_UK ||||||
|||21|EN_US ||||||
|||21|ES_ES ||||||
|||21|FI_FI ||||||
|||21|FR_CA ||||||
|||21|FR_FR ||||||
|||21|IS_IS ||||||
|||21|IT_IT ||||||
|||21|NO_NO ||||||
|||21|PT_PT ||||||
|||21|SV_SV ||||||
|ISO8859_2|LATIN2|22|ISO8859_2 |Latin 2 —  Central European (Croatian, Czech, Hungarian, Polish, Romanian, Serbian, Slovakian, Slovenian)  * ISO 8859-2|ISO-8859-2, ISO88592, LATIN2|1 |1.0|ISO-8859-2|
|||22|CS_CZ ||||||
|||22|ISO_HUN ||||||
|ISO8859_3|LATIN3|23|ISO8859_3 |Latin3 — Southern European (Maltese, Esperanto)  * ISO 8859-3|ISO-8859-3, ISO88593, LATIN3|1 |1.5|ISO-8859-3|
|ISO8859_4|LATIN4|34|ISO8859_4 |Latin 4 — Northern European (Estonian, Latvian, Lithuanian, Greenlandic, Lappish)  * ISO 8859-4|ISO-8859-4, ISO88594, LATIN4|1 |1.5|ISO-8859-4|
|ISO8859_5|ISO_8859_5|35|ISO8859_5 |Cyrillic (Russian)  * ISO 8859-5|ISO-8859-5, ISO88595|1 |1.5|ISO-8859-5|
|ISO8859_6|ISO_8859_6|36|ISO8859_6 |Arabic  * ISO 8859-6|ISO-8859-6, ISO88596|1 |1.5|ISO-8859-6|
|ISO8859_7|ISO_8859_7|37|ISO8859_7 |Greek  * ISO 8859-7|ISO-8859-7, ISO88597|1 |1.5|ISO-8859-7|
|ISO8859_8|ISO_8859_8|38|ISO8859_8 |Hebrew  * ISO 8859-8|ISO-8859-8, ISO88598|1 |1.5|ISO-8859-8|
|ISO8859_9|LATIN5|39|ISO8859_9 |Latin 5  * ISO 8859-9|ISO-8859-9, ISO88599, LATIN5|1 |1.5|ISO-8859-9|
|ISO8859_13|LATIN7|40|ISO8859_13 |Latin 7 — Baltic Rim  * ISO 8859-13|ISO-8859-13, ISO885913, LATIN7|1 |1.5|ISO-8859-13|
|KSC_5601|UHC|44|KSC_5601 |Korean (Unified Hangeul) |DOS_949, KSC5601, WIN_949|2 |1.0|MS949|
|||44|KSC_DICTIONARY ||||||
|DOS852||45|DOS852 |Latin II  * MS-DOS: Latin-2|DOS_852|1 |1.0|Cp852|
|||45|DB_CSY ||||||
|||45|DB_PLK ||||||
|||45|DB_SLO ||||||
|||45|PDOX_CSY ||||||
|||45|PDOX_HUN ||||||
|||45|PDOX_PLK ||||||
|||45|PDOX_SLO ||||||
|DOS857||46|DOS857 |IBM: Turkish|DOS_857|1 |1.0|Cp857|
|||46|DB_TRK ||||||
|DOS861||47|DOS861 |MS-DOS: Icelandic|DOS_861|1 |1.0|Cp861|
|||47|PDOX_ISL ||||||
|DOS866||48|DOS866 |IBM: Cyrillic|DOS_866|1 |1.5|Cp866|
|DOS869||49|DOS869 |IBM: Modern Greek|DOS_869|1 |1.5|Cp869|
|CYRL|WIN1251|50|CYRL |||1 |1.0||
|||50|DB_RUS ||||||
|||50|PDOX_CYRL ||||||
|WIN1250|WIN1250|51|WIN1250 |ANSI — Central European|WIN_1250|1 |1.0|Cp1250|
|||51|PXW_CSY ||||||
|||51|PXW_HUN ||||||
|||51|PXW_HUNDC ||||||
|||51|PXW_PLK ||||||
|||51|PXW_SLOV ||||||
|WIN1251|WIN1251|52|WIN1251 |ANSI — Cyrillic|WIN_1251|1 |1.0|Cp1251|
|||52|PXW_CYRL ||||||
|||52|WIN1251_UA ||||||
|WIN1252|WIN1252|53|WIN1252 |ANSI — Latin I|WIN_1252|1 |1.0|Cp1252|
|||53|PXW_INTL ||||||
|||53|PXW_INTL850 ||||||
|||53|PXW_NORDAN4 ||||||
|||53|PXW_SPAN ||||||
|||53|PXW_SWEDFIN ||||||
|WIN1253|WIN1253|54|WIN1253 |ANSI Greek |WIN_1253|1 |1.0|Cp1253|
|||54|PXW_GREEK ||||||
|WIN1254|WIN1254|55|WIN1254 |ANSI Turkish|WIN_1254|1 |1.0|Cp1254|
|||55|PXW_TURK ||||||
|BIG_5|BIG5|56|BIG_5 |Traditional Chinese|BIG5, DOS_950, WIN_950|2 |1.0|Big5|
|GB_2312|GBK|57|GB_2312 |GB2312, EUC encoding, Simplified Chinese|DOS_936, GB2312, WIN_936|2 |1.0|EUC_CN|
|WIN1255|WIN1255|58|WIN1255 |ANSI Hebrew|WIN_1255|1 |1.5|Cp1255|
|WIN1256|WIN1256|59|WIN1256 |ANSI Arabic|WIN_1256|1 |1.5|Cp1256|
|WIN1257|WIN1257|60|WIN1257 |ANSI Baltic|WIN_1257|1 |1.5|Cp1257|
|WIN1258|WIN1258|61|||WIN_1258||2.0||
|GB18030|GB18030||||||2.5||
|GBK|GBK||||||2.1||
|KOI8R|KOI8R|||KOI ru|||2.0||
|KOI8U|KOI8U|||KOI ua|||2.0||
|CP943C|||CP943C_UNICODE||||2.1||
|TIS620|||TIS620_UNICODE||||2.1||

Sources for Firebird
* https://www.firebirdsql.org/en/firebird-1-5-character-sets-collations/
* https://firebirdsql.org/refdocs/langrefupd25-charsets.html

Source for PostgreSQL
* https://www.postgresql.org/docs/current/multibyte.html#MULTIBYTE-CHARSET-SUPPORTED
