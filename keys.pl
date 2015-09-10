#! /usr/local/bin/perl -w

#
# テーブルのキーを一覧する。
#
push(@INC,'/DataBase/perl');
require 'database.pl';	# データベース操作

use GDBM_File;
use Getopt::Long;
$^W = 1;

sub usage
{
    my($handle) = select(STDERR);
    print <<EOT;
Usage: $0 [options] <database dir>/<table name> {--column <fetch column name>} ...
  options:
    --column	: 表示する項目(カラム)名を指定する。(default:全てのカラム)
EOT
    select($handle);
    return "\n";
}

my @Columns = ();
my %Optctl = (
    'column' => \@Columns,
);

GetOptions(\%Optctl,'column=s@') || die(usage);

my ($DataBase,$TabName) = (shift =~ m'^(.+)/([^/]+)/?$');
my $DB = new database($DataBase);
my @columns = $DB->columns($TabName);
my %indexes = $DB->indexes($TabName);
$indexes{$DB->primary($TabName)} = 'Primary';

if (@Columns) {
    my $notFound = 0;
    foreach my $column (@Columns) {
	unless (grep {$_ eq $column} (@columns)) {
	    printf STDERR "$0: 指定された項目はありません。[%s]\n",$column;
	    $notFound ++;
	}
    }
    exit(1) if ($notFound);
} else {
    @Columns = @columns;
}

my ($KeyName,$KeyData) = @ARGV;
my @keys = $DB->keys($TabName,$KeyName,$KeyData);

if ($KeyData || (defined $KeyName and $KeyName eq $DB->primary($TabName))) {
    foreach my $key (@keys) {
	my %datum;
	if ($DB->fetch($TabName,$key,\%datum)) {
	    print "$key:",join(',',(map {exists($datum{$_})?$datum{$_}:'';} @Columns)),"\n";
	}
    }
} else {
    foreach my $key (@keys) {
	print "$key\n";
    }
}
exit 0;

__END__
# End of file.
