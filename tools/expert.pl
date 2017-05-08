#!/usr/bin/perl

use DBI;

my $dbh = DBI->connect("DBI:mysql:database=RisingStar;host=localhost", "rs", "rs")
	or die "Couldn't connect to mysql database $database: $!\n";

my @table = ("AIExpert", "ArchitectureExpert", "CGExpert", "DatabaseExpert", "HCIExpert", "NetworkExpert", "PLExpert", "SecurityExpert", "TheoryExpert");
foreach $tab (@table) {
    my $sth = $dbh->prepare("SELECT AuthorID FROM $tab ORDER BY CocaRank DESC LIMIT 100") or die $dbh->errstr;
	$sth->execute() or die $sth->errstr;

	open(OUTPUT, "> ../data/$tab.txt")
		or die "Couldn't open $tab for writing: $!\n";
    while (@expert = $sth->fetchrow_array()) {
        print OUTPUT "$expert[0]\n";
    }
	close(OUTPUT);
    $sth->finish();
}

$dbh->disconnect();
