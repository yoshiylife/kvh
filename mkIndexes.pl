#! /usr/local/bin/perl -w

#
# table作成
#
require 'database.pl';	# データベース操作

use Carp;
use GDBM_File;
use Cwd;
use Rcs(Verbose);
use File::Copy;
use HTML::Lint;
use HTML::Parser;
use HTML::TableExtract;
use HTML::TreeBuilder;
use HTML::PrettyPrinter;
use HTML::Entities qw(decode_entities encode_entities %entity2char);
$entity2char{nbsp} = ' ';
use File::MMagic;
use Getopt::Long;

$^W = 1;

my $OptDebug = 1;
my %Optctl =
(
    'debug' => \$OptDebug,
);

GetOptions(\%Optctl, 'debug!');

#
# 定数
#
my $DataBase = $ARGV[0] || "DataBase";		# データ／ＤＢ格納ディレクトリ

my %DataFile =
(
    AuthUser	=> "$DataBase/AuthUser.gdbm",
    AuthWord	=> "$DataBase/AuthWord.gdbm",
    AuthPath	=> "$DataBase/AuthPath.gdbm",
)
;
my %Indexes = (					# 先頭読みがなとローマ字対応表
    'あ' => 'aa', 'か' => 'ka', 'さ' => 'sa', 'た' => 'ta', 'な' => 'na',
    'は' => 'ha', 'ま' => 'ma', 'や' => 'ya', 'ら' => 'ra', 'わ' => 'wa',
    '？' => 'xx',
);



my %Columns =
(
    a_list	=> ['項目名１','項目名２','項目名３'],
    u_list	=>  ['項目名１','項目名２','項目名３'],
    l_list	=> ['ユーザＩＤ','姓','名','受付番号'],
    http_auth	=> ['AuthType','AuthName','AuthUserFile','AuthGroupFile','Require','Username','Password','Directory'],
)
;


#
# 初期設定
#
select(STDERR) ; $| = 1; select(STDOUT);
Rcs->quiet(0);
Rcs->bindir('/usr/local/bin');
my $Magic = new File::MMagic; # use internal magic file

#
# 実行時設定
#
my $DB;

# type1テーブル作成
sub _create_table_type1(*)
{
    my ($DB) = @_;
    
    my $tabName = 'type1Table';

    # データ項目(カラム)の名称と位置の対応付け(順序付き)
    my @DataCols = qw(
	担当部署
	削除フラグ
	登録日時
	登録者
	更新日時
	更新者
    );

    $DB->create($tabName,\@DataCols);
}


# type2テーブル作成
sub _create_table_type2Support(*)
{
    my ($DB) = @_;
    
    my $tabName = 'type2Table';

    # データ項目(カラム)の名称と位置の対応付け(順序付き)
    my @DataCols = qw(
	ROWID
	削除フラグ
	登録日時
	登録者
	更新日時
	更新者
    );

    # 索引を作成するデータ項目(先頭がプライマリ、２番目以降が検索用索引)
    my @DataKeys = qw(
	ROWID
	受付番号
    );

    $DB->create($tabName,\@DataCols,\@DataKeys);
}


#
# 新規テーブル作成
#
sub create(*$@)
{
    my ($DB,@TabNAME) = @_;
    my $baseDir = $DB->baseDir;
    foreach my $tabName (@TabNAME) {
	my $func = '_create_'.$tabName;
	unlink(<$baseDir/$tabName/*>);
	&$func($DB);
	# 簡単な確認
	print STDERR "$baseDir/$tabName\n";
	my $primary = $DB->primary($tabName);
	my @columns = $DB->columns($tabName);
	my %indexes = $DB->indexes($tabName);
	foreach my $i (0..$#columns) {
	    my $col = $columns[$i];
	    printf STDERR "  %02d %s",$i,$col;
	    print STDERR ":Primary" if ($col eq $primary);
	    print STDERR ":$indexes{$col}" if (defined $indexes{$col});
	    print STDERR "\n";
	}
    }
}


#
# http基本認証情報(ユーザＩＤ／パスワード／ディレクトリ)の抽出＆ＤＢ作成
#
sub parse_http_auth($*@)
{
    my ($textFile,$table,@column) = @_;
    my %authFile = %$textFile;

    my %Word = (); # 初期登録時のユーザＩＤ／パスワード(平文)
    my %Pass = (); # http基本認証のユーザＩＤ／パスワード(crypt)
    my %User = (); # Apache設定で認証設定されたパス(顧客プロファイル)
    my %Path = (); # %User の逆

    my $count;

    # 初期登録時のユーザＩＤ／パスワードを抽出する。
    open(WORD,"< $authFile{word}") or die "Can't open($authFile{word}): $!";
    while (<WORD>) {
	chomp;
	next unless (/\w+:\w+/);
	my ($user,$word) = split(/:/);
	die "Dup user $user" if (defined $Word{$user});
	$Word{$user} = $word;
    }
    close(WORD) or die "Can't close($authFile{word}): $!";

    # http基本認証のユーザＩＤ／パスワード(crypt後)を抽出する。
    open(PASS,"< $authFile{pass}") or die "Can't open($authFile{pass}): $!";
    while (<PASS>) {
	chomp;
	my ($user,$pass) = split(/:/);
	die "Dup user $user" if (defined $Pass{$user});
	$Pass{$user} = $pass;
    }
    close(PASS) or die "Can't close($authFile{pass}): $!";

    # Apache設定ファイルからhttp基本認証の設定を抽出する。
    %$table = map {$_ => []} @column;
    $table->{total} = 0;
    open(CONF,"< $authFile{conf}") or die "Can't open($authFile{conf}): $!";
    while (<CONF>) {
	chomp;
	next if (/^#/ || /^$/);
	# ディレクティブ Directory 設定を切り出す。
	if (/^\s*<Directory\s+("[^"]+"|'[^']+'|[^\s]+)\s*>/i) {
	    my $path = $1;
	    next if ($path =~ m!$Apache/cgi-bin/!);
	    print STDERR "想定外ディレクトリ $path\n" and next unless ($path =~ s!^["']\Q$Apache/htdocs.443/\E(.+)/?["']$!$1!);
	    print STDERR "テスト用設定のスキップ $path\n" and next if ($path =~ /^(999999|fss)/); # テスト用を無視する。
	    my @lines = ();
	    while (<CONF>) {
		next if (/^#/ || /^$/);
		last if (m!^\s*</Directory>!i);
		push(@lines,$_);
	    }
	    # auth設定を抽出する。(複数ある場合は、最後の指定を採用)
	    my %data = map {$_ => ''} @column; # 各項目の最後に操作したデータの保持にも使用。
	    foreach (@lines) {
		$data{AuthType} = $' if (/^\s*AuthType\s+/i);
		$data{AuthName} = $' if (/^\s*AuthName\s+/i);
		$data{AuthUserFile} = $' if (/^\s*AuthUserFile\s+/i);
		$data{AuthGroupFile} = $' if (/^\s*AuthGroupFile\s+/i);
		$data{Require} = $' if (/^\s*require\s+/i); # 現在(2004/04/08)、１行のみの指定
	    }
	    print STDERR "require設定無しのスキップ: $path", next unless ($data{Require});
	    #print "$path $data{AuthType} $data{AuthName} $data{Require}\n";
	    #print "$path\n  $data{Require}\n";
	    if ($data{AuthType} =~ /^basic/i && $data{AuthName} =~ /Profile/i && $data{Require} =~ /group\s+(.*?)\s+/i) {
		my $user = $1;
		if (defined $Pass{$user}) {
		    die "crypt前パスワード無し: $user" unless ($Word{$user});
		    my $pass = crypt($Word{$user},$Pass{$user});
		    die "パスワード変更あり $user: '$pass' <-> '$Pass{$user}'\n " unless ($pass eq $Pass{$user});
		} else {
		    print STDERR "パスワード未設定 $user: $path\n";
		}
		die "認証設定が重複 $user: '$path' <-> '$Path{$user}'\n " if (exists $Path{$user});
		$Path{$user} = $path;
		die "認証設定が重複 $path: '$user' <-> '$User{$path}'\n " if (exists $User{$path});
		$User{$path} = $user;

		$data{Directory} = $path;
		$data{Username} = $User{$path};
		$data{Password} = $Word{$user};

		# テーブルへ追加
		foreach my $colNam (@column) {
		    $table->{$colNam}->[$table->{total}] = $data{$colNam};
		}
		$table->{total} ++;
	    }
	}
    }
    close(CONF) or die "Can't close($authFile{conf}): $!";

    print STDERR "AuthUser DB: ";
    my %AuthUser;
    tie(%AuthUser,'GDBM_File',$DataFile{AuthUser},GDBM_WRCREAT,0644) or die "Can't tie($DataFile{AuthUser}): $!";
    print STDERR "Rset...";
    %AuthUser = ();
    print STDERR "Make...";
    $count = 0;
    while (my ($path,$user) = each %User) {
	$AuthUser{$path} = $user;
	$count ++;
    }
    untie(%AuthUser) or die "Can't untie($DataFile{AuthUser}): $!";
    print STDERR "$count record(s) Verify...";
    $count = 0;
    tie(%AuthUser,'GDBM_File',$DataFile{AuthUser},GDBM_READER,0) or die "Can't tie($DataFile{AuthUser}): $!";
    while (my ($path,$user) = each %AuthUser) {
	if ($user eq $User{$path}) {
	    $count ++;
	} else {
	    die "DB Error $path: '$user' <-> '$User{$path}'";
	}
    }
    untie(%AuthUser) or die "Can't untie($DataFile{AuthUser}): $!";
    print STDERR "$count record(s) Done.\n";

    print STDERR "AuthPath DB: ";
    my %AuthPath;
    tie(%AuthPath,'GDBM_File',$DataFile{AuthPath},GDBM_WRCREAT,0644) or die "Can't tie($DataFile{AuthPath}): $!";
    print STDERR "Rset...";
    %AuthPath = ();
    print STDERR "Make...";
    $count = 0;
    while (my ($user,$path) = each %Path) {
	$AuthPath{$user} = $path;
	$count ++;
    }
    untie(%AuthPath) or die "Can't untie($DataFile{AuthPath}): $!";
    print STDERR "$count record(s) Verify...";
    $count = 0;
    tie(%AuthPath,'GDBM_File',$DataFile{AuthPath},GDBM_READER,0) or die "Can't tie($DataFile{AuthPath}): $!";
    while (my ($user,$path) = each %AuthPath) {
	if ($path eq $Path{$user}) {
	    $count ++;
	} else {
	    die "DB Error $user: '$path' <-> '$Path{$user}'";
	}
    }
    untie(%AuthPath) or die "Can't untie($DataFile{AuthPath}): $!";
    print STDERR "$count record(s) Done.\n";

    print STDERR "AuthWord DB: ";
    my %AuthWord;
    tie(%AuthWord,'GDBM_File',$DataFile{AuthWord},GDBM_WRCREAT,0644) or die "Can't tie($DataFile{AuthWord}): $!";
    print STDERR "Rset...";
    %AuthWord = ();
    print STDERR "Make...";
    $count = 0;
    while (my ($user,$pass) = each %Word) {
	$AuthWord{$user} = $Word{$user};
	$count ++;
    }
    untie(%AuthWord) or die "Can't untie($DataFile{AuthWord}): $!";
    print STDERR "$count record(s) Verify...";
    $count = 0;
    tie(%AuthWord,'GDBM_File',$DataFile{AuthWord},GDBM_READER,0) or die "Can't tie($DataFile{AuthWord}): $!";
    while (my ($user,$word) = each %AuthWord) {
	if ($word eq $Word{$user}) {
	    $count ++;
	} else {
	    die "DB Error $user: '$word' <-> '$Word{$user}'";
	}
    }
    untie(%AuthWord) or die "Can't untie($DataFile{AuthWord}): $!";
    print STDERR "$count record(s) Done.\n";

    print STDERR "In $authFile{conf}\nTotal:$table->{total}\n";

    return $table->{total};
}


sub keywords
{
    my $val = shift;
    $_ = nkf("-EeXZ1",$val);
    s/<br>/ /gi;
    s/([\r\n\f\t()\[\]:;,]| |　)+/ /gi;
    s/^ +| +$//g;
    return $_;
}


sub output($$;$$$;$$$$)
{
    my ($buff,$fmt,$str,$flag) = @_;
    if (defined $str) {
	if (defined($flag) && $flag) {
	    $str = '' unless($str);
	} else {
	    $str = '(空欄)' unless($str);
	}
	$str = sprintf($fmt,$str);
    } else {
	$str = $fmt;
    }
    $str =~ s/  /　/g;
    #print nkf('-j',$str);
    $$buff .= $str;
    #return nkf('-e','-Z1',$str);
}

# 未知のテーブルを出力する。
sub output_unknown($$@)
{
    my ($buff,$title,@rows) = @_;

    #output($buff,"--unknown--\n");
    foreach my $cols (@rows) {
	foreach (@$cols) {
	    output($buff,'%-12s ',"$_") unless ($_ eq $title);
	}
	output($buff,"\n");
    }
}

# タイトル行を除くテーブルを出力する。
sub output_type1($$@)
{
    my ($buff,$title,@rows) = @_;

    #output($buff,"--type1--\n");
    foreach my $cols (@rows) {
	foreach (@$cols) {
	    output($buff,'%-12s ',$_) unless ($_ eq $title);
	}
	output($buff,"\n");
    }
}





#
# メイン処理
#


$DB = new database($DataBase,substr($0,rindex($0,'/')+1));
create($DB,'Table1','Table2','Table3');

foreach my $i (0..$DataList{http_auth}->{total}-1) {
    my $dir = $DataList{http_auth}->{'Directory'}->[$i];
    my @rowid = $DB->keys('Indexes');
    foreach my $rowid (@rowid) {
	my %data = ();
	if ($DB->fetch('Indexes',$rowid,\%data)) {
	    if ($data{'Data'} =~ /^$dir/) {
		%data = ();
		$data{'AuthDirectory'} = $ProfList{http_auth}->{'Directory'}->[$i];
		$data{'AuthUsername'} = $ProfList{http_auth}->{'Username'}->[$i];
		$data{'AuthPassword'} = $ProfList{http_auth}->{'Password'}->[$i];
		$DB->modify('Indexes',$rowid,\%data);
		#print "MODIFY $path\n";
	    }
	} else {
	    die "NOT FOUND: $dir\n";
	}
    }
}

exit 0;
# End of file.
