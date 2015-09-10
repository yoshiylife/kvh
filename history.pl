#! /usr/local/bin/perl -w

#
# 履歴内容の表示
#
#
# $Id: history.pl,v 1.2 2004/11/30 07:26:18 yoshiy Exp $
push(@INC,'/DataBase/perl');
require 'database.pl';	# データベース操作

use GDBM_File;
use Getopt::Long;
$^W = 1;

sub usage
{
    my($handle) = select(STDERR);
    print <<EOT;
Usage: $0 [options] <database dir>/<table name> <primary key value>
  options: None
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

my ($DataDir,$TabName) = (shift =~ m'^(.+)/([^/]+)/?$');
my $DB = new database($DataDir);
my @columns = $DB->columns($TabName);
my %indexes = $DB->indexes($TabName);
$indexes{$DB->primary($TabName)} = 'Primary';

my %data;
my @hist;
my %hist;
if ($DB->fetch($TabName,$ARGV[0],\%data)) {
    my $next = $data{'更新日時'};
    %data = ();
    while ($DB->history($TabName,$ARGV[0],\%data,\$next)) {
	push(@hist,$data{'更新日時'});
	$hist{$data{'更新日時'}} = {%data};
	%data = ();
    }
    foreach my $hist (reverse(@hist)) {
	%data = %{$hist{$hist}};
	printf "-----[%s]\n",$data{'更新日時'};
	while (my ($col,$val) = each %data) {
	    my $ind = $indexes{$col} || '';
	    $val =~ s/[\r\n]/ /g;
	    printf "%-7s %s[%s]\n",$ind,$col,$val;
	}
    }
}
exit 0;

__END__
# End of file.
