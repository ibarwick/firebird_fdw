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


sub dbname {
    $self->{dbname};
}

sub psql {
    my $self = shift;
    my $sql = shift;

    $self->{pg_fdw_node}->psql(
        $self->{dbname},
        $sql,
    );
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
    my %table_options = @_;

	$table_options{firebird_only} //= 0;

    # Create Firebird table

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

    return $self->{table_name} if $table_options{firebird_only} == 1;

    # Create PostgreSQL foreign table

    my @options = ();

    push @options, sprintf(
        q|table_name '%s'|,
        $self->{table_name},
    );

    if (defined($table_options{updatable})) {
        push @options, sprintf(
            q|updatable '%s'|,
            $table_options{updatable},
        );
    }

    if (defined($table_options{estimated_row_count})) {
        push @options, sprintf(
            q|estimated_row_count '%s'|,
            $table_options{estimated_row_count},
        );
    }

    my $sql = sprintf(
        <<EO_SQL,
CREATE FOREIGN TABLE %s (
  lang_id                         CHAR(2) NOT NULL,
  name_english                    VARCHAR(64) NOT NULL,
  name_native                     VARCHAR(64) NOT NULL
)
  SERVER fb_test
  OPTIONS (%s)
EO_SQL
        $self->{table_name},
        join(",\n", @options),
    );

    $self->safe_psql($sql);

    return $self->{table_name};
}


sub init_data_table {
    my $self = shift;
    my %params = @_;

    $params{firebird_only} //= 0;

    my $table_name = sprintf(
        q|%s_data|,
        $self->{table_name},
    );

	my $fb_tables = {
		2 => sprintf(
            <<EO_SQL,
CREATE TABLE %s (
  id        INT NOT NULL PRIMARY KEY,
  blob_type BLOB SUB_TYPE TEXT DEFAULT NULL
)
EO_SQL
            $table_name,
        ),
		3 => sprintf(
            <<EO_SQL,
CREATE TABLE %s (
  id        INT NOT NULL PRIMARY KEY,
  blob_type BLOB SUB_TYPE TEXT DEFAULT NULL,
  bool_type BOOLEAN DEFAULT NULL
)
EO_SQL
            $table_name,
        ),
	};

	my $fb_major_version = $self->{pg_fdw_node}->{firebird_major_version};

    my $tbl_query = $self->{pg_fdw_node}->{dbh}->prepare(
        $fb_tables->{$fb_major_version},
    );

    $tbl_query->execute();
    $tbl_query->finish();

    return $table_name if $params{firebird_only} == 1;

    # Create PostgreSQL foreign table

	my $pg_column_defs = {
		2 => sprintf(
            <<EO_SQL,
  id        INT NOT NULL,
  blob_type TEXT DEFAULT NULL
EO_SQL
		),
		3 => sprintf(
            <<EO_SQL,
  id        INT NOT NULL,
  blob_type TEXT DEFAULT NULL,
  bool_type BOOLEAN DEFAULT NULL
EO_SQL
		),
	};


    $self->safe_psql(
        sprintf(
            <<EO_SQL,
CREATE FOREIGN TABLE %s (
%s
)
  SERVER fb_test
  OPTIONS (table_name '%s')
EO_SQL
            $table_name,
			$pg_column_defs->{$fb_major_version},
            $table_name,
        ),
    );

    return $table_name;
}


sub drop_foreign_server {
    my $self = shift;

    my $drop_foreign_server = q|DROP SERVER IF EXISTS fb_test CASCADE|;

    $self->safe_psql( $drop_foreign_server );
}

1;
