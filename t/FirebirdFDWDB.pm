package FirebirdFDWDB;

# Initialise a PostgreSQL database with the Firebird FDW

sub new {
    my $class = shift;
    my $pg_fdw_node = shift;

    my $self = {};

    bless ($self, $class);

    $self->{pg_fdw_node} = $pg_fdw_node;

    $self->{dbname} = 'fdw_test';
	$self->{server_name} = 'fb_test';

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

	# Server is created with all options explicitly set so we can
	# easily execute "ALTER SERVER ... OPTION (SET foo 'bar)".
    $self->safe_psql(
        sprintf(
            <<EO_SQL,
CREATE SERVER %s
  FOREIGN DATA WRAPPER firebird
  OPTIONS (
    address 'localhost',
    database '%s',
    port '3050',
    updatable 'true',
    disable_pushdowns 'false'
 );
EO_SQL
			$self->{server_name},
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

    return $self;
}


sub _make_table_name {
	my $self = shift;

	my $table_name = 'tbl_';

    foreach my $i (0..7) {
        $table_name .= chr(int(26*rand) + 97);
    }

	return $table_name;
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


sub alter_server_option {
    my $self = shift;
    my $option = shift;
	my $value = shift;

	$self->safe_psql(
        sprintf(
            <<EO_SQL,
    ALTER SERVER %s
       OPTIONS (SET %s '%s')
EO_SQL
			$self->{server_name},
			$option,
			$value,
		),
	);
}


sub drop_server_option {
    my $self = shift;
    my $option = shift;

	$self->safe_psql(
        sprintf(
            <<EO_SQL,
    ALTER SERVER %s
       OPTIONS (DROP %s)
EO_SQL
			$self->{server_name},
			$option,
		),
	);
}

sub init_table {
    my $self = shift;
    my %table_options = @_;

	$table_options{firebird_only} //= 0;

	my $table_name = $self->_make_table_name();

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
            $table_name,
        ),
    );

    $tbl_query->execute();
    $tbl_query->finish();

    return $table_name if $table_options{firebird_only} == 1;

    # Create PostgreSQL foreign table

    my @options = ();

    push @options, sprintf(
        q|table_name '%s'|,
        $table_name,
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
  SERVER %s
  OPTIONS (%s)
EO_SQL
        $table_name,
		$self->{server_name},
        join(",\n", @options),
    );

    $self->safe_psql($sql);

    return $table_name;
}


sub init_data_table {
    my $self = shift;
    my %params = @_;

    $params{firebird_only} //= 0;

    my $table_name = sprintf(
        q|%s_data|,
		$self->_make_table_name(),
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
  SERVER %s
  OPTIONS (table_name '%s')
EO_SQL
            $table_name,
			$pg_column_defs->{$fb_major_version},
			$self->{server_name},
            $table_name,
        ),
    );

    return $table_name;
}

sub drop_foreign_table {
    my $self = shift;
    my $table = shift;

    my $drop_foreign_table = sprintf(
        q|DROP FOREIGN TABLE IF EXISTS %s CASCADE|,
        $table,
    );

    $self->safe_psql( $drop_foreign_table );
}


sub drop_foreign_server {
    my $self = shift;

    my $drop_foreign_server = q|DROP SERVER IF EXISTS fb_test CASCADE|;

    $self->safe_psql( $drop_foreign_server );
}

1;
