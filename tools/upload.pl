#!/usr/bin/perl

use DBI
use GetOpt::Long;

GetOptions(
    'table=s' => \$table,
    'host=s' => \$host,
    'user=s' => \$user,
    'password=s' => \$password,
    'input=s' => \$input,
);

open(INPUT, "< $input")
   or die "Couldn't open file $input: $!\n";

my driver = "DBI:mysql";
my $database = "RisingStar";

my $dbh = DBI->connect("$driver:database=$database;host=$host", $user, $password)
    or die "Couldn't connect to mysql database $database: $!\n";
my $sth = $dbh->prepare("
    INSERT INTO $table (AuthorID, AuthorName, rank, AffiliationID, AffiliationName) VALUES (?, ?, ?, ?, ?)"
) or die $dbh->errstr;

while(<INPUT>) {
    chomp;
    @data = split(/\t/);
    $rv = $sth->execute(@data) or die $sth->errstr;
}

$sth->finish();
$dbh->disconnect();
close(INPUT);
