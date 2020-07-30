package FirebirdFDWNode;

# This class extends PostgresNode with firebird_fdw-specific methods.
#
# It has two main purposes:
#  1) to initialise a PostgreSQL node with the firebird_fdw extension
#     configured
#  2) to provide a connection to a running Firebird instance.
#
# The Firebird database must be specified with the standard Firebird
# environment variables `ISC_DATABASE`, `ISC_USER` and `ISC_PASSWORD`.
#
# XXX behaviour undefined if not set

use strict;
use warnings;

use base 'PostgresNode';
use v5.10.0;

use PostgresNode;

use Exporter 'import';
use vars qw(@EXPORT @EXPORT_OK);

use Carp 'verbose';

use DBI;

$SIG{__DIE__} = \&Carp::confess;

@EXPORT = qw(
	get_new_fdw_node
);

sub get_new_fdw_node {
	my $name = shift;

	my $class = 'FirebirdFDWNode';

	my $self = $class->SUPER::get_new_node($name);

	$self->{firebird_dbname} = $ENV{'ISC_DATABASE'};

	$self->{dbh} = DBI->connect(
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

	return $self;
}


sub firebird_reconnect {
    my $self = shift;

    $self->{dbh}->disconnect();

	$self->{dbh} = DBI->connect(
		"dbi:Firebird:host=localhost;dbname=".$self->{firebird_dbname},
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

	my $version_q = $self->{dbh}->prepare( $version );

	$version_q->execute();

	my $res = $version_q->fetchrow_array();
	$version_q->finish();

	# We're expecting something like "3.0.3"
	if ($res =~ m|(\d)|) {
		return $1;
	}

	return undef;
}

sub drop_table {
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

    my $tbl_query = $self->{dbh}->prepare( $drop_table );

    $tbl_query->execute();
    $tbl_query->finish();
}

1;
