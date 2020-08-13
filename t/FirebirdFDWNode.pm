package FirebirdFDWNode;

# This class extends PostgresNode with firebird_fdw-specific methods.
#
# It has two main purposes:
#  1) to initialise a PostgreSQL node with the firebird_fdw extension
#     configured
#  2) to provide a connection to a running Firebird instance.
#
# The Firebird database must be specified with the standard Firebird
# environment variables `ISC_DATABASE`, `ISC_USER` and `ISC_PASSWORD`,
# otherwise each test will exit with an error.

use strict;
use warnings;

use v5.10.0;

use PostgresNode;


use Exporter 'import';
use vars qw(@EXPORT @EXPORT_OK);

use Carp;
use DBI;

$SIG{__DIE__} = \&Carp::confess;

@EXPORT = qw(
	get_new_fdw_node
);

sub new {
    my $class = shift;
    my $self = {};

    bless($self, $class);

	$self->{postgres_node} = get_new_node('pg_node');

    # Set up Firebird connection
    # --------------------------
    #
    # (do this first to catch any problems connecting to Firebird)

    croak "please set environment variables ISC_DATABASE, ISC_USER and ISC_PASSWORD"
        unless defined($ENV{'ISC_DATABASE'});

	$self->{firebird_dbname} = $ENV{'ISC_DATABASE'};

	$self->{firebird_dbh} = DBI->connect(
		"dbi:Firebird:host=localhost;dbname=".$self->{firebird_dbname},
		undef,
		undef,
		{
			PrintError => 1,
			RaiseError => 1,
			AutoCommit => 1
		}
	);

	$self->{firebird_major_version} = $self->get_firebird_major_version();



    # Set up FDW on PostgreSQL
    # ------------------------

    $self->{postgres_node}->init();
    $self->{postgres_node}->start();

    $self->{dbname} = 'fdw_test';
	$self->{server_name} = 'fb_test';

    $self->{postgres_node}->safe_psql(
        'postgres',
        sprintf(
            q|CREATE DATABASE %s|,
            $self->{dbname},
        ),
    );

    $self->safe_psql(
        <<EO_SQL,
CREATE EXTENSION firebird_fdw;
EO_SQL
    );

	# Server is created with all options explicitly set so we can
	# easily execute "ALTER SERVER ... OPTION (SET foo 'bar')".
    $self->safe_psql(
        sprintf(
            <<EO_SQL,
CREATE SERVER %s
  FOREIGN DATA WRAPPER firebird_fdw
  OPTIONS (
    address 'localhost',
    database '%s',
    port '3050',
    updatable 'true',
    disable_pushdowns 'false'
 );
EO_SQL
			$self->{server_name},
            $self->{firebird_dbname},
        )
    );


    $self->safe_psql(
        sprintf(
            <<EO_SQL,
CREATE USER MAPPING
  FOR CURRENT_USER
  SERVER %s
  OPTIONS(
    username '%s',
    password '%s'
  );
EO_SQL
            $self->{server_name},
            $ENV{'ISC_USER'},
            $ENV{'ISC_PASSWORD'},
        )
    );

	return $self;
}

#-----------------------------------------------------------------------
#
# General methods
#
#-----------------------------------------------------------------------

sub server_name {
    shift->{server_name};
}


sub _make_table_name {
	my $self = shift;
    my %options = @_;

    $options{uc_prefix} //= 0;

	my $table_name = 'tbl_';

    if ($options{uc_prefix}) {
        $table_name = uc($table_name);
    }

    foreach my $i (0..7) {
        $table_name .= chr(int(26*rand) + 97);
    }

	return $table_name;
}


sub init_table {
    my $self = shift;
    my %table_options = @_;

	$table_options{firebird_only} //= 0;

	my $table_name = $self->_make_table_name();

    # Create Firebird table

    my $tbl_query = $self->firebird_conn()->prepare(
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


# Tables for testing data type handling

sub init_data_type_table {
    my $self = shift;
    my %params = @_;

    $params{firebird_only} //= 0;

    my $table_name = sprintf(
        q|%s_data_type|,
		$self->_make_table_name(),
    );

	my $fb_tables = {
        # Firebird 2.x
		2 => sprintf(
            <<EO_SQL,
CREATE TABLE %s (
  id        INT NOT NULL PRIMARY KEY,
  blob_type BLOB SUB_TYPE TEXT DEFAULT NULL
)
EO_SQL
            $table_name,
        ),
        # Firebird 3.x
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

	my $fb_major_version = $self->{firebird_major_version};

    my $tbl_query = $self->firebird_conn()->prepare(
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



#-----------------------------------------------------------------------
#
# PostgreSQL methods
#
#-----------------------------------------------------------------------


sub dbname {
    shift->{dbname};
}

sub psql {
    my $self = shift;
    my $sql = shift;

    return $self->{postgres_node}->psql(
        $self->{dbname},
        $sql,
    );
}

sub safe_psql {
    my $self = shift;
    my $sql = shift;

    return $self->{postgres_node}->safe_psql(
        $self->{dbname},
        $sql,
    );
}


sub pg_version {
    my $self = shift;

    my $sql = <<EO_SQL;
  SELECT setting
    FROM pg_catalog.pg_settings
   WHERE name = 'server_version_num'
EO_SQL

    return $self->safe_psql($sql);
}



sub add_server_option {
    my $self = shift;
    my $option = shift;
	my $value = shift;

	$self->safe_psql(
        sprintf(
            <<EO_SQL,
    ALTER SERVER %s
       OPTIONS (ADD %s '%s')
EO_SQL
			$self->{server_name},
			$option,
			$value,
		),
	);
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

    my $drop_foreign_server = sprintf(
        q|DROP SERVER IF EXISTS %s CASCADE|,
        $self->{server_name},
    );

    $self->safe_psql( $drop_foreign_server );
}

#-----------------------------------------------------------------------
#
# Firebird methods
#
#-----------------------------------------------------------------------

sub firebird_conn {
    shift->{firebird_dbh};
}

sub firebird_reconnect {
    my $self = shift;

    $self->firebird_conn()->disconnect();

	$self->{firebird_dbh} = DBI->connect(
		'dbi:Firebird:host=localhost;dbname='.$self->{firebird_dbname},
		undef,
		undef,
		{
			PrintError => 1,
			RaiseError => 1,
			AutoCommit => 1
		}
	);
}


sub get_firebird_major_version {
	my $self = shift;

	my $version = q|SELECT CAST(rdb$get_context('SYSTEM', 'ENGINE_VERSION') AS VARCHAR(10)) FROM rdb$database|;

	my $version_q = $self->firebird_conn()->prepare( $version );

	$version_q->execute();

	my $res = $version_q->fetchrow_array();
	$version_q->finish();

	# We're expecting something like "3.0.3"
	if ($res =~ m|(\d)|) {
		return $1;
	}

	return undef;
}

sub firebird_format_results {
    my $self = shift;
    my $query = shift;

    my @outp;

    while (my @row = $query->fetchrow_array()) {
        push @outp, join('|', @row);
    }

    return join("\n", @outp);
}


sub firebird_execute_sql {
    my $self = shift;
    my $sql = shift;

    my $q = $self->firebird_conn()->prepare( $sql);

    $q->execute();

    $q->finish();
}


sub firebird_drop_table {
    my $self = shift;
    my $table_name = shift;
    my $quote_identifier = @_ ? shift : 0;

    $self->firebird_reconnect();

    if ($quote_identifier) {
        $table_name = qq|"$table_name"|;
    }
    my $drop_table = sprintf(
        q|DROP TABLE %s|,
        $table_name,
    );

    my $tbl_query = $self->firebird_conn()->prepare( $drop_table );

    $tbl_query->execute();
    $tbl_query->finish();
}

1;
