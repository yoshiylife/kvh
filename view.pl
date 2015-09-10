#! /usr/local/bin/perl -w

#
# テーブルの内容表示
#
# $Id: view.pl,v 1.2 2004/11/08 04:51:26 yoshiy Exp $
push(@INC,'/DataBase/perl');
require 'database.pl';	# データベース操作

use GDBM_File;
use Getopt::Long;
$^W = 1;

sub usage
{
    my($handle) = select(STDERR);
    print <<EOT;
Usage: $0 [options] <database dir>/<table name> {--primary|<key column name>} <key column value> ...
  options:
    --nl		: 改行(0x0a)を可視化する。
    --column <項目名>	: 表示する項目(カラム)名を指定する。(default:全てのカラム)
    --primary		: 検索にプライマリキーを使用する。
EOT
    select($handle);
    return "\n";
}

my @Columns = ();
my $primary = 0;
my $Opt_nl = 0;
my %Optctl = (
    'nl' => \$Opt_nl,
    'column' => \@Columns,
    'primary' => \$primary,
);

GetOptions(\%Optctl,'column=s@','primary!','nl!') || die(usage);

my ($DataBase,$TabName) = (shift =~ m'^(.+)/([^/]+)/?$');
my $DB = new database($DataBase);
my @columns = $DB->columns($TabName);
my %indexes = $DB->indexes($TabName);
$indexes{$DB->primary($TabName)} = 'Primary';

if (@ARGV == 0) {
    printf "%-7s %s\n",'-索引-','-項目名-';
    foreach (@columns) {
	printf "%-7s %s\n",(defined $indexes{$_})?$indexes{$_}:'',$_;
    }
    exit(0);
}

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

my $KeyName = undef;
my @KeyData = ();
if ($primary) {
    die(usage) unless (1 <= @ARGV);
    push(@KeyData,@ARGV);
} else {
    die(usage) unless (2 <= @ARGV);
    $KeyName = shift;
    unless (exists $indexes{$KeyName}) {
	printf STDERR "$0: 検索用索引がない項目です。[%s]\n",$KeyName;
	exit(1);
    }
    foreach (@ARGV) {
	push(@KeyData,lc);
    }
}

my @keys = ();
my %data = map {$_ => []} (@Columns);
my $total = 0;
if ($KeyName) {
    my $n;
    foreach (@KeyData) {
	$n = $DB->select($TabName,$KeyName,($_ eq 'undef')?undef:$_,\%data,$total);
	for (my $i = 0; $i < $n; $i ++) {
	    $keys[$total+$i] = $_;
	}
	$total += $n;
    }
} else {
    my %datum = ();
    foreach (@KeyData) {
	if ($DB->fetch($TabName,$_,\%datum)) {
	    $keys[$total] = $_;
	    foreach (@Columns) {
		if (exists $datum{$_}) {
		    $data{$_}->[$total] = $datum{$_};
		} else {
		    $data{$_}->[$total] = undef;
		}
	    }
	    $total ++;
	}
    }
}

print "$DataBase/$TabName: $total selected.\n";
#print "(",$DB->count($TabName,$KeyName),")\n";
my $max = 0;
foreach (@Columns) {
    $max = length if ($max < length);
}

for (my $i = 0; $i < $total; $i ++) {
    printf "\n%06d[%s]\n",$i+1,$keys[$i];
    foreach (@Columns) {
	my $val = $data{$_}->[$i];
	my ($lc,$rc) = ('[',']');
	if (defined $val) {
	    if ($Opt_nl) {
		$val =~ s/\n/'\n'/eg;
	    } else {
		if ($val =~ /\n/) {
		    $lc = "\n".'-'x$max."\n";
		    $rc = "\n".'-'x$max;
		}
	    }
	} else {
	    $val = 'undef';
	}
	printf "%s%s$lc%s$rc\n",$_,'.'x($max-length),$val;
    }
}
exit 0;

__END__
# End of file.
