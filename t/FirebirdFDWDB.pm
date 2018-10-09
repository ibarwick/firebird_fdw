package FirebirdFDWDB;

# Initialise a PostgreSQL database with the Firebird FDW

sub new {
    my $class = shift;
    my $pg_fdw_node = shift;

    my $self = {};

    bless ($self, $class);

    $self->{pg_fdw_node} = $pg_fdw_node;

    $self->{dbname} = 'fdw_test';

    $self->{pg_fdw_node}->safe_psql(
        'postgres',
        sprintf(
            q|CREATE DATABASE %s|,
            $self->{dbname},
        ),
    );

    $self->safe_psql(
        <<EO_SQL,
CREATE EXTENSION firebird_fdw;
CREATE FOREIGN DATA WRAPPER firebird
  HANDLER firebird_fdw_handler
  VALIDATOR firebird_fdw_validator;
EO_SQL
    );

    $self->safe_psql(
        sprintf(
            <<EO_SQL,
CREATE SERVER fb_test
  FOREIGN DATA WRAPPER firebird
  OPTIONS (
    address 'localhost',
    database '%s'
 );
EO_SQL
            $self->{pg_fdw_node}->{firebird_dbname},
        )
    );


    $self->safe_psql(
        sprintf(
            <<EO_SQL,
CREATE USER MAPPING FOR CURRENT_USER SERVER fb_test
  OPTIONS(
    username '%s',
    password '%s'
  );
EO_SQL
            $ENV{'ISC_USER'},
            $ENV{'ISC_PASSWORD'},
        )
    );

    $self->{table_name} = 'tbl_';

    foreach my $i (0..7) {
        $self->{table_name} .= chr(int(26*rand) + 97);
    }


    return $self;
}


sub safe_psql {
    my $self = shift;
    my $sql = shift;

    $self->{pg_fdw_node}->safe_psql(
        $self->{dbname},
        $sql,
    );
}


sub version {
    my $self = shift;

    my $sql = <<EO_SQL;
  SELECT setting
    FROM pg_catalog.pg_settings
   WHERE name = 'server_version_num'
EO_SQL

    return $self->safe_psql($sql);
}


sub init_table {
    my $self = shift;

    my $tbl_query = $self->{pg_fdw_node}->{dbh}->prepare(
        sprintf(
            <<EO_SQL,
CREATE TABLE %s (
  LANG_ID                         CHAR(2) NOT NULL PRIMARY KEY,
  NAME_ENGLISH                    VARCHAR(64) NOT NULL,
  NAME_NATIVE                     VARCHAR(64) NOT NULL
)
EO_SQL
            $self->{table_name},
        ),
    );

    $tbl_query->execute();
    $tbl_query->finish();

    $self->safe_psql(
        sprintf(
            <<EO_SQL,
CREATE FOREIGN TABLE %s (
  lang_id                         CHAR(2) NOT NULL,
  name_english                    VARCHAR(64) NOT NULL,
  name_native                     VARCHAR(64) NOT NULL
)
  SERVER fb_test
  OPTIONS (table_name '%s')
EO_SQL
            $self->{table_name},
            $self->{table_name},
        ),
    );

    return $self->{table_name};
}


1;
