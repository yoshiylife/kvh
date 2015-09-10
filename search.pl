#! /usr/local/bin/perl -w

#
# テーブル内容の表示
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
Usage: $0 [options] <database dir>/<table name> {--column <fetch column name>} <column name> <Perl regular expressions>
  options:
    --column	: 表示する項目(カラム)名を指定する。(default:全てのカラム)
    --keyname	: 検索用索引の項目(カラム)名
    --keydata	: 検索用索引の項目(カラム)値
EOT
    select($handle);
    return "\n";
}

my @Columns = ();
my $KeyName = undef;
my $KeyData = undef;
my %Optctl = (
    'column' => \@Columns,
    'keyname' => \$KeyName,
    'keydata' => \$KeyData,
);

GetOptions(\%Optctl,'column=s@','keyname=s','keydata=s') || die(usage);

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

unless (grep {$_ eq $ARGV[0]} (@columns)) {
    printf STDERR "$0: 指定された項目はありません。[%s]\n",$ARGV[0];
    die(usage);
}

my @CallData = @ARGV;
$KeyName = $CallData[0] unless ($KeyName);

sub callback
{
    my ($KeyNAME,$KeyDATA,$DATA) = @_;
    my ($col,$pat) = @CallData;

    if ($DATA) {
	(exists($DATA->{$col}) && $DATA->{$col} =~ /$pat/);
    } else {
	($KeyDATA =~ /$pat/);
    }
}

my @keys = $DB->search($TabName,\&callback,$KeyName,$KeyData);

if ($KeyData || $KeyName eq $DB->primary($TabName)) {
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
