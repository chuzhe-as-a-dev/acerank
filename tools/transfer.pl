#!/usr/bin/perl
use DBI;
use Getopt::Long;

my $host, $user, $password, $mag, $input, $output;

GetOptions(
    'host=s' => \$host,
    'user=s' => \$user,
    'password=s' => \$password,
    'database=s' => \$mag,
    'input=s' => \$input,
    'output=s' => \$output,
);

my $driver="DBI:mysql";
my $mag = "mag-new-160205";

# CONNECT DATABASE
my $db_mag = DBI->connect("$driver:database=$mag;host=$host", $user, $password)
    or die "Couldn't connect to mysql database $mag: $!\n";
my $some_author = $db_mag->prepare("SELECT AuthorName FROM Authors WHERE AuthorID = ?");
my $some_affiliation = $db_mag->prepare("SELECT AffiliationName FROM Affiliations WHERE AffiliationID = ?");

# OPEN FILE
open(INPUT, "< $input")
    or die "Couldn't open $input for reading: $!\n";
open(OUTPUT, "> $output")
    or die "Couldn't open $output for writing: $!\n";

while (<INPUT>) {
    chomp;
    ($AuthorID, $rank, $AffiliationID) = split(/\t/);
    $some_author->execute($AuthorID) or die $db_mag->errstr;
    @sth = $some_author->fetchrow_array();
    $AuthorName = $sth[0];
    if ($AffiliationID !~ /None/) {
        $some_affiliation->execute($AffiliationID) or die $db_mag->errstr;
        @sth = $some_affiliation->fetchrow_array();
        $AffiliationName = $sth[0];
    } else {
        $AffiliationName = $AffiliationID;
    }
    print OUTPUT "$AuthorID\t$AuthorName\t$rank\t$AffiliationID\t$AffiliationName\n";
}

close(INPUT);
close(OUTPUT);

# FINISH AND DISCONNECT
$some_author->finish();
$some_affiliation->finish();
$db_mag->disconnect();
