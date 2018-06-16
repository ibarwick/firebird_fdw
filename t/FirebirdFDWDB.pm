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
1;
