
#
# $Id: database.pl,v 1.19 2004/12/09 08:02:23 yoshiy Exp $
#

package database;

use GDBM_File;
use POSIX qw(:errno_h strftime);
use Carp;
use Class::Struct;
use strict;

use vars qw(*PAE *RETRY);

*PAE = \"\r\f"; # 定数：データ区切り
*RETRY = {	# 定数：再試行の
    count => 60,	# 回数
    interval => 10,	# 間隔(秒)
};

sub _join(@)
{
    my @pieces = @_;
    foreach (@pieces) {
	s/\r/\r\n/;
    }
    return join($PAE,@pieces);
}

sub _split($)
{
    my ($string) = @_;
    my @pieces = split($PAE,$string,-1);
    foreach (@pieces) {
	s/\r\n/\r/;
    }
    return @pieces;
}

sub new($;$$)
{
    my $that = shift;
    my $class = ref($that) || $that;
    my $self =
    {
	BaseDir => shift || '',	# 各テーブル用ディレクトリが配置されているディレクトリ
	Ident => shift || '',	# 登録者／更新者
	TabList => {},		# テーブル名 => {名称 => データファイル名,...})
    };

    # テーブル名のリスト作成
    my $dir = $self->{BaseDir};
    opendir(DIR,$dir) or confess "【エラー】opendir($dir): $!";
    foreach my $tabName (grep { /^[^.]/ && -d "$dir/$_" } readdir(DIR)) {
	$self->{TabList}->{$tabName} = {DataDir => "$dir/$tabName"};
    }
    closedir(DIR) or warn "【警告】closedir($dir): $!";

    bless($self,$class);
    return $self;
}

sub baseDir
{
    my $self = shift;
    return $self->{BaseDir};
}

sub exists($$)
{
    my $self = shift;
    my ($TabNAME,$KeyDATA) = @_;
    confess "【エラー】存在しないテーブル名: $TabNAME\n " unless (exists $self->{TabList}->{$TabNAME});
    $self->select($TabNAME,undef,$KeyDATA,undef);
}

sub fetch($$$)
{
    my $self = shift;
    my ($TabNAME,$PKEY,$DATA) = @_;
    confess "【エラー】存在しないテーブル名: $TabNAME\n " unless (exists $self->{TabList}->{$TabNAME});
    operate($self->{TabList}->{$TabNAME},undef,$PKEY,$DATA,0,'internal');
}

sub modify($$$;$$$$)
{
    my $self = shift;
    my ($TabNAME,$PKEY,$DATA,$IDENT) = @_;
    my $ident = $IDENT || $self->{Ident};
    confess "【エラー】存在しないテーブル名: $TabNAME\n " unless (exists $self->{TabList}->{$TabNAME});
    confess "【エラー】登録者／更新者の指定がない: $TabNAME\n " unless ($ident);
    operate($self->{TabList}->{$TabNAME},undef,$PKEY,$DATA,1,$ident);
}

sub insert($$$;$$$$)
{
    my $self = shift;
    my ($TabNAME,$PKEY,$DATA,$IDENT) = @_;
    my $ident = $IDENT || $self->{Ident};
    confess "【エラー】存在しないテーブル名: $TabNAME\n " unless (exists $self->{TabList}->{$TabNAME});
    confess "【エラー】登録者／更新者の指定がない: $TabNAME\n " unless ($ident);
    operate($self->{TabList}->{$TabNAME},undef,$PKEY,$DATA,2,$ident);
}

sub update($$$;$$$$)
{
    my $self = shift;
    my ($TabNAME,$PKEY,$DATA,$IDENT) = @_;
    my $ident = $IDENT || $self->{Ident};
    confess "【エラー】存在しないテーブル名: $TabNAME\n " unless (exists $self->{TabList}->{$TabNAME});
    confess "【エラー】登録者／更新者の指定がない: $TabNAME\n " unless ($ident);
    operate($self->{TabList}->{$TabNAME},undef,$PKEY,$DATA,3,$ident);
}

sub obtain($$$$)
{
    my $self = shift;
    my ($TabNAME,$KeyNAME,$KeyDATA,$DATA) = @_;
    confess "【エラー】存在しないテーブル名: $TabNAME\n " unless (exists $self->{TabList}->{$TabNAME});
    operate($self->{TabList}->{$TabNAME},$KeyNAME,$KeyDATA,$DATA,0,'internal');
}

sub expan($$$$)
{
    my $self = shift;
    my ($TabNAME,$KeyNAME,$KeyDATA,$DATA) = @_;
    my $expan = {};
    confess "【エラー】存在しないテーブル名: $TabNAME\n " unless (exists $self->{TabList}->{$TabNAME});
    if (operate($self->{TabList}->{$TabNAME},$KeyNAME,$DATA->{$KeyDATA},$expan,0,'internal')) {
	while (my ($col,$val) = each %$expan) {
	    $DATA->{$col} = $val unless (exists $DATA->{$col});
	}
	return 1;
    }
    return 0;
}

sub indexes($)
{
    my $self = shift;
    my ($TabNAME) = @_;
    confess "【エラー】存在しないテーブル名: $TabNAME\n " unless (exists $self->{TabList}->{$TabNAME});
    my $TabDATA = $self->{TabList}->{$TabNAME};
    schema($TabDATA);
    my %ColPos = %{$TabDATA->{ColPos}};
    my @Indexes = @{$TabDATA->{Indexes}};
    my %indexes = ();
    while (my ($col,$pos) = each %ColPos) {
	$indexes{$col} = $Indexes[$pos] if (defined $Indexes[$pos]);
    }
    #$indexes{$TabDATA->{Primary}} = 'RowData';
    return %indexes;
}

sub primary($)
{
    my $self = shift;
    my ($TabNAME) = @_;
    confess "【エラー】存在しないテーブル名: $TabNAME\n " unless (exists $self->{TabList}->{$TabNAME});
    my $TabDATA = $self->{TabList}->{$TabNAME};
    schema($TabDATA);
    return $TabDATA->{Primary};
}

sub columns($)
{
    my $self = shift;
    my ($TabNAME) = @_;
    confess "【エラー】存在しないテーブル名: $TabNAME\n " unless (exists $self->{TabList}->{$TabNAME});
    my $TabDATA = $self->{TabList}->{$TabNAME};
    schema($TabDATA);
    my @Columns = @{$TabDATA->{Columns}};
    return @Columns;
}

sub tables()
{
    my $self = shift;
    my @tables = keys(%{$self->{TabList}});
    return @tables;
}

sub schema(*)
{
    my ($TabDATA) = @_;
    my $scmFile = "$TabDATA->{DataDir}/Schema.gdbm";
    my $retry;
    my %scmData;

    # テーブル構造データの読込
    my %ScmData;
    $retry = $RETRY{count};
    while ($retry) {
	tie(%ScmData,'GDBM_File',$scmFile,GDBM_READER,0660) and last;
	warn "【注意】再試行残りあと $retry 回 ($RETRY{interval}秒間隔)\n tie($scmFile): $!\n ";
	confess "【エラー】$scmFile: $!\n " unless ($! == EACCES || $! == EAGAIN);
	$retry --;
	sleep($RETRY{interval});
    }
    confess "【エラー】tie($scmFile): $!\n " unless ($retry);
    %scmData = %ScmData;
    untie(%ScmData) or warn "【警告】untie($scmFile): $!\n ";

    # テーブル項目名 => データ配列中の位置
    my @cols = _split($scmData{Columns});
    my $cols = {};
    foreach my $pos (0..$#cols) {
	$cols->{$cols[$pos]} = $pos;
    }
    $TabDATA->{Columns} = [@cols];
    $TabDATA->{ColPos} = $cols;
    $TabDATA->{Primary} = $scmData{Primary};

    # テーブル項目名 => 検索用索引名
    my $indexes = [];
    while (my ($key,$col) = each %scmData) {
	$indexes->[$cols->{$col}] = $key if ($key =~ /^Key/);
    }
    $TabDATA->{Indexes} = $indexes;

    return 1;
}

sub create($$;$$$;$$$$;$$$$$)
{
    my $self = shift;
    my ($TabNAME,$DataCols,$DataKeys,$DATA,$IDENT) = @_;
    my $ident = $IDENT || $self->{Ident};
    my @dataCols = ();
    my @dataKeys = ();
    my %scmData;

    @dataCols = @$DataCols;
    @dataKeys = (defined $DataKeys) ? @$DataKeys : ($dataCols[0]);

    #carp "【警告】既存のテーブル名: $TabNAME\n " unless (exists $self->{TabList}->{$TabNAME});
    confess "【エラー】登録者／更新者の指定がない: $TabNAME\n " unless ($ident);

    my $dataDir = "$self->{BaseDir}/$TabNAME";

    # テーブル用ディレクトリの作成
    mkdir($dataDir,0770) or confess "【エラー】mkdir($dataDir): $!\n " unless (-e $dataDir);

    # データ項目の位置を算定する。(テーブル項目名 => データ配列中の位置)
    my %colPos = ();
    foreach my $pos (0..$#dataCols) {
	confess "【エラー】項目名が重複している: $dataCols[$pos]\n " if (exists $colPos{$dataCols[$pos]});
	$colPos{$dataCols[$pos]} = $pos;
    }

    # 必須項目のチェック
    foreach my $col ('登録日時','登録者','更新日時','更新者') {
	confess "【エラー】必須項目がない: $col\n " unless (defined $colPos{$col});
    }

    # 索引項目のチェック
    my %indexes = ();
    my $primary = $dataKeys[0];
    foreach my $pos (0..$#dataKeys) {
	my $col = $dataKeys[$pos];
	confess "【エラー】索引項目がない: $col\n " unless (exists $colPos{$col});
	next if ($col eq $primary);
	$indexes{$col} = sprintf("Key%04d",$pos);
    }

    # テーブル構造定義を作成する。
    my $scmFile = "$dataDir/Schema.gdbm";
    confess "【エラー】定義ファイルが既存: $scmFile\n " if (-e $scmFile);
    tie(my %ScmData,'GDBM_File',$scmFile,GDBM_WRCREAT,0640) or confess "【エラー】定義ファイル tie($scmFile): $!\n ";
    %ScmData = ();
    $ScmData{Primary} = $primary;
    $ScmData{Columns} = _join(@dataCols);
    while (my ($col,$key) = each %indexes) {
	$ScmData{$key} = $col;
    }
    %scmData = %ScmData;
    untie(%ScmData) or confess "【エラー】定義ファイル untie($scmFile): $!\n ";

    # データファイルを初期化する。
    foreach my $name ('RowData','LogData',values(%indexes)) {
	next unless ($name);
	my $file = "$dataDir/$name.gdbm";
	tie(my %Data,'GDBM_File',$file,GDBM_WRCREAT,0660) or confess "【エラー】データファイル\n tie($file): $!\n ";
	%Data = ();
	untie(%Data) or confess "【エラー】データファイル\n untie($file): $!\n ";
    }

    $self->{TabList}->{$TabNAME} = {DataDir => $dataDir};

    $self->isetup($TabNAME,$DATA,$ident) if ($DATA);

    return %scmData;
}

sub isetup($$;$$$)
{
    my $self = shift;
    my ($TabNAME,$DATA,$IDENT) = @_;
    my $Ident = $IDENT || $self->{Ident};
    my %RowData;
    my $total = 0;

    confess "【エラー】存在しないテーブル名: $TabNAME\n " unless (exists $self->{TabList}->{$TabNAME});
    my $TabDATA = $self->{TabList}->{$TabNAME};

    # テーブル定義の読込
    schema($TabDATA);
    my %ColPos = %{$TabDATA->{ColPos}};
    my @Indexes = @{$TabDATA->{Indexes}};
    my $rowFile = $TabDATA->{DataDir}."/RowData.gdbm";
    my $logFile = $TabDATA->{DataDir}."/LogData.gdbm";

    # データファイルのオープン
    tie(%RowData,'GDBM_File',$rowFile,GDBM_WRITER,0660) or confess "【エラー】データファイル\n tie($rowFile): $!\n ";

    # 全データの削除
    %RowData = ();

    # データの一括投入
    my $now = strftime("%Y/%m/%d %H:%M:%S",localtime);
    my $pkey = $DATA->{$TabDATA->{Primary}};
    my $nkey = scalar(@$pkey);
    for (my $i = 0; $i < $nkey; $i ++) {
	my @data = ();
	while (my ($col,$pos) = each %ColPos) {
	    if (defined $DATA->{$col}->[$i]) {
		$data[$pos] = $DATA->{$col}->[$i];
	    } else {
		$data[$pos] = '';
	    }
	}
	# 呼び出し側による更新を許可しない項目の上書き
	$data[$ColPos{'登録日時'}] = $now;
	$data[$ColPos{'登録者'}] = $Ident;
	$data[$ColPos{'更新日時'}] = $now;
	$data[$ColPos{'更新者'}] = $Ident;
	$! = 0;
	if (exists $RowData{$pkey->[$i]}) {
	    my $data = _join(@data);
	    if ($RowData{$pkey->[$i]} eq $data) {
		warn "【警告】キー重複($rowFile): $pkey->[$i]\n ";
	    } else {
		confess "【エラー】キー重複($rowFile): $pkey->[$i]\n ";
	    }
	} else {
	    $RowData{$pkey->[$i]} = _join(@data);
	    $total ++;
	}
	confess "【エラー】データファイル\n tie($rowFile): $!\n " if ($!);
    }

    # 検索用索引の作成
    while (my ($col,$pos) = each %ColPos) {
	next if ($col eq $TabDATA->{Primary});
	next unless ($Indexes[$pos]);
	my $keyFile = $TabDATA->{DataDir}."/$Indexes[$pos].gdbm";
	my %KeyData;
	tie(%KeyData,'GDBM_File',$keyFile,GDBM_WRITER,0660) or confess "【エラー】検索用ファイル\n tie($keyFile): $!\n ";
	%KeyData = ();
	my $ikey = $DATA->{$col};
	for (my $i = 0; $i < $nkey; $i ++) {
	    my $key = $ikey->[$i];						# コーディング上の便宜の為
	    next unless ($key);
	    $key = lc($key);
	    my $pkey = $pkey->[$i];						# コーディング上の便宜の為
	    my @data = ();
	    @data = _split($KeyData{$key}) if (exists $KeyData{$key});
	    push(@data,$pkey) unless (grep {$pkey eq $_} @data);
	    $KeyData{$key} = _join(@data);
	}
	untie(%KeyData) or warn "【警告】検索用ファイル\n untie($keyFile): $!\n ";
    }

    untie(%RowData) or warn "【警告】データファイル\n untie($rowFile): $!\n ";

    # 更新記録の初期化
    tie(my %LogData,'GDBM_File',$logFile,GDBM_WRITER,0660) or confess "【エラー】更新記録ファイル\n tie($logFile): $!\n ";
    %LogData = ();
    untie(%LogData) or warn "【エラー】更新記録ファイル\n untie($logFile): $!\n ";

    return $total;
}

sub _rowid(\%)
{
    my ($ROWID) = @_;
    my $rowid;
    for (;;) {
	$rowid = unpack('H8',pack('nn',rand(0xffff),rand(0xffff)));
	last unless (exists $ROWID->{$rowid});
    }
    return lc($rowid);
}

# 
# ＤＢの主操作
#
sub operate(*$$$$$)
{
    my ($TabDATA,$KeyNAME,$KeyDATA,$DATA,$mode,$Ident) = @_;

    # テーブル定義の読込
    schema($TabDATA);
    my %ColPos = %{$TabDATA->{ColPos}};
    my @Indexes = @{$TabDATA->{Indexes}};
    my $rowFile = $TabDATA->{DataDir}."/RowData.gdbm";
    my $logFile = $TabDATA->{DataDir}."/LogData.gdbm";

    $KeyNAME = $TabDATA->{Primary} unless (defined $KeyNAME);
    if ($KeyNAME ne $TabDATA->{Primary}) {
	confess "【エラー】検索用索引がない: $KeyNAME\n " unless (exists($ColPos{$KeyNAME}) && defined($Indexes[$ColPos{$KeyNAME}]));
    }

    my $found = 0;

    my $retry;

    # データファイルのオープン
    my %RowData;
    $retry = $RETRY{count};
    while ($retry) {
	tie(%RowData,'GDBM_File',$rowFile,($mode == 0) ? GDBM_READER : GDBM_WRITER,0660) and last;
	warn "【注意】再試行残りあと $retry 回 ($RETRY{interval}秒間隔)\n tie($rowFile): $!\n ";
	confess "【エラー】データファイル\n tie($rowFile): $!\n " unless ($! == EACCES || $! == EAGAIN);
	$retry --;
	sleep($RETRY{interval});
    }
    confess "【エラー】データファイル\n tie($rowFile): $!\n " unless ($retry);

    # プライマリキーの計算
    my $PKEY;
    if ($KeyNAME eq $TabDATA->{Primary}) {
	$PKEY = $KeyDATA;
    } else {
	my $keyFile = $TabDATA->{DataDir}."/$Indexes[$ColPos{$KeyNAME}].gdbm";
	my %KeyData;
	$retry = $RETRY{count};
	while ($retry) {
	    tie(%KeyData,'GDBM_File',$keyFile,GDBM_READER,0660) and last;
	    warn "【注意】再試行残りあと $retry 回 ($RETRY{interval}秒間隔)\n tie($keyFile): $!\n ";
	    confess "【エラー】検索用ファイル\n tie($keyFile): $!\n " unless ($! == EACCES || $! == EAGAIN);
	    $retry --;
	    sleep($RETRY{interval});
	}
	confess "【エラー】検索用ファイル\n tie($keyFile): $!\n " unless ($retry);
	my @pkey = ();
	my $keyData = lc($KeyDATA);
	@pkey = _split($KeyData{$keyData}) if (exists $KeyData{$keyData});
	untie(%KeyData) or warn "【警告】検索用ファイル\n untie($keyFile): $!\n ";
	if (@pkey == 1) {
	    $PKEY = $pkey[0];
	} else {
	    confess "【エラー】$KeyNAME=$KeyDATA\n 検索結果が複数です。\n".join("\n",@pkey)."\n " unless (@pkey == 0);
	    $PKEY = undef;
	}
    }

    # データの読出
    my @data = ();
    $retry = $RETRY{count};
    while ($retry) {
	$! = 0;
	if (defined($PKEY) && exists($RowData{$PKEY})) {
	    # 内部データへの変換
	    @data = _split($RowData{$PKEY});
	    $found ++;
	} else {
	    # 内部データの初期化
	    while (my ($col,$pos) = each %ColPos) {
		$data[$pos] = "";
	    }
	}
	last unless ($!);
	confess "【エラー】データファイル\n tie($rowFile): $!\n " unless ($! == EACCES || $! == EAGAIN);
	warn "【注意】再試行残りあと $retry 回 ($RETRY{interval}秒間隔)\n $rowFile: $!\n ";
	$retry --;
	sleep($RETRY{interval});
    }
    confess "【エラー】データ読込\n $rowFile\n " unless ($retry);

    # ユニークキーの自動生成(ROWID)
    if ($found == 0 and ($mode == 2 || $mode == 3)) {
	if ($KeyNAME eq 'ROWID') {
	    $PKEY = (!$KeyDATA or exists($RowData{$KeyDATA})) ? _rowid(%RowData) : $KeyDATA;
	    $DATA->{'ROWID'} = $PKEY;
	} else {
	    if (exists $ColPos{'ROWID'}) {
		my $pos = $ColPos{'ROWID'};
		my $keyFile = $TabDATA->{DataDir}."/$Indexes[$pos].gdbm";
		my %KeyData;
		$retry = $RETRY{count};
		while ($retry) {
		    tie(%KeyData,'GDBM_File',$keyFile,GDBM_READER,0660) and last;
		    warn "【注意】再試行残りあと $retry 回 ($RETRY{interval}秒間隔)\n tie($keyFile): $!\n ";
		    confess "【エラー】検索用ファイル\n tie($keyFile): $!\n " unless ($! == EACCES || $! == EAGAIN);
		    $retry --;
		    sleep($RETRY{interval});
		}
		confess "【エラー】検索用ファイル\n tie($keyFile): $!\n " unless ($retry);
		$DATA->{'ROWID'} = _rowid(%KeyData) if (!$DATA->{'ROWID'} or exists($KeyData{$DATA->{'ROWID'}}));
		untie(%KeyData) or warn "【警告】検索用ファイル\n untie($keyFile): $!\n ";
	    }
	}
    }

    # 更新条件チェック
    if ($mode) {
	if ($found and exists($DATA->{'更新日時'})) {
	    #print STDERR "$data[$ColPos{'更新日時'}] <-> $DATA->{'更新日時'}\n";
	    unless ($data[$ColPos{'更新日時'}] eq $DATA->{'更新日時'}) {
		# 更新不可
		warn "【警告】更新日時が一致しないので更新できません。: $rowFile\n ";
		$PKEY = undef;
		$found = 0;
	    }
	}
    }

    if (defined($PKEY) and $mode and (($mode % 2) == $found || $mode == 3)) {
	# 呼び出し側による更新を許可しない項目の上書き
	$DATA->{'ROWID'} = $data[$ColPos{'ROWID'}] if ($found and exists($ColPos{'ROWID'}));
	# 呼び出し側による更新を許可しない項目の削除
	delete $DATA->{'登録日時'} if (exists $DATA->{'登録日時'});
	delete $DATA->{'更新日時'} if (exists $DATA->{'更新日時'});
	delete $DATA->{'登録者'} if ($data[$ColPos{'登録者'}]);
	$DATA->{'登録者'} = "$Ident" unless($data[$ColPos{'登録者'}]);
	$DATA->{'更新者'} = "$Ident";

	my $now = strftime("%Y/%m/%d %H:%M:%S",localtime);
	unless ($found) {
	    $data[$ColPos{'登録日時'}] = $now; # 最初の更新記録のキー
	    $data[$ColPos{'更新日時'}] = $now; # 最後の更新記録のキー
	}

	# 更新データの作成
	my @renew = ();
	push(@renew,"更新日時\n$data[$ColPos{'更新日時'}]"); # 更新日時は常に更新扱い
	while (my ($col,$pos) = each %ColPos) {
	    if (exists $DATA->{$col}) {
		# ログ用データ
		my $old = $data[$pos];
		# undef値を許可 -> データのクリア
		if (defined $DATA->{$col}) {
		    push(@renew,"$col\n$DATA->{$col}") unless ($data[$pos] eq $DATA->{$col});
		    $data[$pos] = $DATA->{$col};
		} else {
		    push(@renew,"$col\n");
		    $data[$pos] = '';
		}
		my $new = $data[$pos];

		# 検索用索引の更新
		if (defined $Indexes[$pos]) {
		    unless ($old && $new && lc($old) eq lc($new)) {
			my @keyData = ();
			my $keyFile = $TabDATA->{DataDir}."/$Indexes[$pos].gdbm";
			$retry = $RETRY{count};
			my %KeyData;
			while ($retry) {
			    tie(%KeyData,'GDBM_File',$keyFile,GDBM_WRITER,0660) and last;
			    warn "【注意】再試行残りあと $retry 回 ($RETRY{interval}秒間隔)\n tie($keyFile): $!\n ";
			    confess "【エラー】検索用ファイル\n tie($keyFile): $!\n " unless ($! == EACCES || $! == EAGAIN);
			    $retry --;
			    sleep($RETRY{interval});
			}
			confess "【エラー】検索用ファイル\n tie($keyFile): $!\n " unless ($retry);
			if ($old) {
			    $old = lc($old);
			    # 削除
			    @keyData = _split($KeyData{$old}) if (exists $KeyData{$old});
			    @keyData = (grep {$PKEY ne $_} @keyData) if (@keyData);
			    if (@keyData) {
				$KeyData{$old} = _join(@keyData);
			    } else {
				delete $KeyData{$old};
			    }
			}
			if ($new) {
			    $new = lc($new);
			    # 追加
			    @keyData = _split($KeyData{$new}) if (exists $KeyData{$new});
			    push(@keyData,$PKEY) unless (grep {$PKEY eq $_} @keyData);
			    $KeyData{$new} = _join(@keyData);
			}
			untie(%KeyData) or warn "【警告】検索用ファイル\n untie($keyFile): $!\n ";
		    }
		}
	    }
	}

	# データ更新の失敗に備え、先に更新記録に追加('更新日時'は更新前のもの)
	my %LogData;
	$retry = $RETRY{count};
	while ($retry) {
	    tie(%LogData,'GDBM_File',$logFile,GDBM_WRITER,0660) and last;
	    warn "【注意】再試行残りあと $retry 回 ($RETRY{interval}秒間隔)\n tie($logFile): $!\n ";
	    confess "【エラー】検索用ファイル\n tie($logFile): $!\n " unless ($! == EACCES || $! == EAGAIN);
	    $retry --;
	    sleep($RETRY{interval});
	}
	confess "【エラー】検索用ファイル\n tie($logFile): $!\n " unless ($retry);
	if (exists $LogData{$now.'='.$PKEY}) {
	    sleep(1);
	    $now = strftime("%Y/%m/%d %H:%M:%S",localtime);
	}
	my $key = $now.'='.$PKEY;
	$LogData{$key} =  _join(@renew) or confess "【警告】更新記録ファイルの更新\n $logFile($key): $!\n ";
	untie(%LogData) or confess "【警告】更新記録ファイル\n untie($logFile): $!\n ";

	# データの登録＆更新
	$data[$ColPos{'更新日時'}] = $now;
	$retry = $RETRY{count};
	while ($retry) {
	    $! = 0;
	    $RowData{$PKEY} = _join(@data);
	    last unless ($!);
	    confess "【エラー】データファイル\n $rowFile: $!\n " unless ($! == EACCES || $! == EAGAIN);
	    warn "【注意】再試行残りあと $retry 回 ($RETRY{interval}秒間隔)\n $rowFile: $!\n ";
	    $retry --;
	    sleep($RETRY{interval});
	}
	confess "【エラー】データ書込\n $rowFile\n " unless ($retry);

	$found ++;
    }

    untie(%RowData) or warn "【警告】データファイル\n untie($rowFile): $!\n ";

    if ($found) {
	while (my ($col,$pos) = each %ColPos) {
	    $DATA->{$col} = $data[$pos];
	}
    }

    return $found;
}

sub select($$$$;$$$$$)
{
    my $self = shift;
    my ($TabNAME,$KeyNAME,$KeyDATA,$DATA,$POS) = @_;
    $POS = 0 unless (defined $POS);

    confess "【エラー】存在しないテーブル名: $TabNAME\n " unless (exists $self->{TabList}->{$TabNAME});

    my $TabDATA = $self->{TabList}->{$TabNAME};

    # テーブル定義の読込
    schema($TabDATA);
    my %ColPos = %{$TabDATA->{ColPos}};
    my @Indexes = @{$TabDATA->{Indexes}};
    my $rowFile = $TabDATA->{DataDir}."/RowData.gdbm";

    $KeyNAME = $TabDATA->{Primary} unless (defined $KeyNAME);
    if ($KeyNAME ne $TabDATA->{Primary}) {
	confess "【エラー】検索用索引がない: $KeyNAME\n " unless (exists($ColPos{$KeyNAME}) && defined($Indexes[$ColPos{$KeyNAME}]));
    }

    my $found = 0;

    my $retry;

    # データファイルのオープン
    my %RowData;
    $retry = $RETRY{count};
    while ($retry) {
	tie(%RowData,'GDBM_File',$rowFile,GDBM_READER,0660) and last;
	warn "【注意】再試行残りあと $retry 回 ($RETRY{interval}秒間隔)\n tie($rowFile): $!\n ";
	confess "【エラー】データファイル\n $rowFile: $!\n " unless ($! == EACCES || $! == EAGAIN);
	$retry --;
	sleep($RETRY{interval});
    }
    confess "【エラー】データファイル\n tie($rowFile): $!\n " unless ($retry);

    # 検索用索引キーから主キーへの変換(要複数対応)
    my @PKEY = ();
    if ($KeyNAME eq $TabDATA->{Primary}) {
	if (defined $KeyDATA) {
	    @PKEY = ($KeyDATA) if (exists $RowData{$KeyDATA});
	} else {
	    @PKEY = keys(%RowData);
	}
    } else {
	my $keyFile = $TabDATA->{DataDir}."/$Indexes[$ColPos{$KeyNAME}].gdbm";
	my $keyData = lc($KeyDATA);
	my %KeyData;
	$retry = $RETRY{count};
	while ($retry) {
	    tie(%KeyData,'GDBM_File',$keyFile,GDBM_READER,0660) and last;
	    warn "【注意】再試行残りあと $retry 回 ($RETRY{interval}秒間隔)\n tie($keyFile): $!\n ";
	    confess "【エラー】検索用ファイル\n tie($keyFile): $!\n " unless ($! == EACCES || $! == EAGAIN);
	    $retry --;
	    sleep($RETRY{interval});
	}
	confess "【エラー】検索用ファイル\n tie($keyFile): $!\n " unless ($retry);
	@PKEY = _split($KeyData{$keyData}) if (exists $KeyData{$keyData});
	untie(%KeyData) or warn "【警告】検索用ファイル\n untie($keyFile): $!\n ";
    }

    if (defined $DATA) {

	# データ格納先の矯正
	while (my ($col,$val) = each %$DATA) {
	    next unless (exists $ColPos{$col});
	    $val = [] if ($POS == 0 && ref($val) ne 'ARRAY');
	}

	my @data;
	foreach my $PKEY (@PKEY) {

	    # データの読出
	    @data = ();
	    $retry = $RETRY{count};
	    while ($retry) {
		$! = 0;
		if (exists $RowData{$PKEY}) {
		    $found ++;
		    @data = _split($RowData{$PKEY});
		    # データ項目の選定
		    while (my ($col,$val) = each %$DATA) {
			$val->[$POS] = $data[$ColPos{$col}] if (exists $ColPos{$col});
		    }
		    $POS ++;
		}
		last unless ($!);
		confess "【エラー】データファイル\n tie($rowFile): $!\n " unless ($! == EACCES || $! == EAGAIN);
		warn "【注意】再試行残りあと $retry 回 ($RETRY{interval}秒間隔)\n $rowFile: $!\n ";
		$retry --;
		sleep($RETRY{interval});
	    }
	    confess "【エラー】データ読込\n $rowFile\n " unless ($retry);

	}

    } else {
	$found = scalar(@PKEY);
    }

    untie(%RowData) or warn "【警告】データファイル\n untie($rowFile): $!\n ";

    return $found;
}

sub search($$;$$$;$$$$)
{
    my $self = shift;
    my ($TabNAME,$CallBACK,$KeyNAME,$KeyDATA) = @_;

    confess "【エラー】存在しないテーブル名: $TabNAME\n " unless (exists $self->{TabList}->{$TabNAME});

    my $TabDATA = $self->{TabList}->{$TabNAME};

    # テーブル定義の読込
    schema($TabDATA);
    my %ColPos = %{$TabDATA->{ColPos}};
    my @Indexes = @{$TabDATA->{Indexes}};
    my $rowFile = $TabDATA->{DataDir}."/RowData.gdbm";

    $KeyNAME = $TabDATA->{Primary} unless (defined $KeyNAME);
    if ($KeyNAME ne $TabDATA->{Primary}) {
	confess "【エラー】検索用索引がない: $KeyNAME\n " unless (exists($ColPos{$KeyNAME}) && defined($Indexes[$ColPos{$KeyNAME}]));
    }

    my @pkey = ();
    my @PKEY = ();

    my $retry;

    # データファイルのオープン
    my %RowData;
    $retry = $RETRY{count};
    while ($retry) {
	tie(%RowData,'GDBM_File',$rowFile,GDBM_READER,0660) and last;
	warn "【注意】再試行残りあと $retry 回 ($RETRY{interval}秒間隔)\n tie($rowFile): $!\n ";
	confess "【エラー】データファイル\n $rowFile: $!\n " unless ($! == EACCES || $! == EAGAIN);
	$retry --;
	sleep($RETRY{interval});
    }
    confess "【エラー】データファイル\n tie($rowFile): $!\n " unless ($retry);

    if ($KeyNAME eq $TabDATA->{Primary}) {
	if (defined $KeyDATA) {
	    @PKEY = ($KeyDATA) if (exists $RowData{$KeyDATA});
	} else {
	    if (defined $CallBACK) {
		@PKEY = keys(%RowData);
	    } else {
		@pkey = keys(%RowData);
	    }
	}
    } else {
	my $keyFile = $TabDATA->{DataDir}."/$Indexes[$ColPos{$KeyNAME}].gdbm";
	my %KeyData;
	$retry = $RETRY{count};
	while ($retry) {
	    tie(%KeyData,'GDBM_File',$keyFile,GDBM_READER,0660) and last;
	    warn "【注意】再試行残りあと $retry 回 ($RETRY{interval}秒間隔)\n tie($keyFile): $!\n ";
	    confess "【エラー】検索用ファイル\n tie($keyFile): $!\n " unless ($! == EACCES || $! == EAGAIN);
	    $retry --;
	    sleep($RETRY{interval});
	}
	confess "【エラー】検索用ファイル\n tie($keyFile): $!\n " unless ($retry);
	if (defined $KeyDATA) {
	    my $keyData = lc($KeyDATA);
	    @PKEY = _split($KeyData{$keyData}) if (exists $KeyData{$keyData});
	} else {
	    my $callback = $CallBACK ? $CallBACK : sub {1;};
	    foreach my $key (keys %KeyData) {
		push(@pkey,_split($KeyData{$key})) if (&$callback($KeyNAME,$key));
	    }
	}
	untie(%KeyData) or warn "【警告】検索用ファイル\n untie($keyFile): $!\n ";
    }

    foreach my $PKEY (@PKEY) {
	$retry = $RETRY{count};
	while ($retry) {
	    $! = 0;
	    if (exists($RowData{$PKEY})) {
		if (defined $CallBACK) {
		    my @data = _split($RowData{$PKEY});
		    my %data = ();
		    while (my ($col,$pos) = each %ColPos) {
			$data{$col} = $data[$pos];
		    }
		    push(@pkey,$PKEY) if (&$CallBACK($KeyNAME,$PKEY,\%data));
		} else {
		    push(@pkey,$PKEY);
		}
	    }
	    last unless ($!);
	    confess "【エラー】データファイル\n tie($rowFile): $!\n " unless ($! == EACCES || $! == EAGAIN);
	    warn "【注意】再試行残りあと $retry 回 ($RETRY{interval}秒間隔)\n $rowFile: $!\n ";
	    $retry --;
	    sleep($RETRY{interval});
	}
	confess "【エラー】データ読込\n $rowFile\n " unless ($retry);
    }

    untie(%RowData) or warn "【警告】データファイル\n untie($rowFile): $!\n ";

    return @pkey;
}

sub keys($;$$;$$$)
{
    my $self = shift;
    my ($TabNAME,$KeyNAME,$KeyDATA) = @_;

    return $self->search($TabNAME,undef,$KeyNAME,$KeyDATA);
}

sub count($;$$;$$$)
{
    my $self = shift;
    my ($TabNAME,$KeyNAME,$KeyDATA) = @_;

    return scalar($self->search($TabNAME,undef,$KeyNAME,$KeyDATA));
}

sub reorder($$$$)
{
    my $self = shift;
    my ($TabNAME,$KeyNAME,$KeyDATA,$DATA) = @_;

    confess "【エラー】存在しないテーブル名: $TabNAME\n " unless (exists $self->{TabList}->{$TabNAME});

    my $TabDATA = $self->{TabList}->{$TabNAME};

    # テーブル定義の読込
    schema($TabDATA);
    my %ColPos = %{$TabDATA->{ColPos}};
    my @Indexes = @{$TabDATA->{Indexes}};
    my $rowFile = $TabDATA->{DataDir}."/RowData.gdbm";

    confess "【エラー】プライマリ索引には適用できない: $KeyNAME\n " if ($KeyNAME ne $TabDATA->{Primary});
    confess "【エラー】検索用索引がない: $KeyNAME\n " unless (exists($ColPos{$KeyNAME}) && defined($Indexes[$ColPos{$KeyNAME}]));

    my $found = 0;

    my $retry;

    # データファイルのオープン
    my %RowData;
    $retry = $RETRY{count};
    while ($retry) {
	tie(%RowData,'GDBM_File',$rowFile,GDBM_READER,0660) and last;
	warn "【注意】再試行残りあと $retry 回 ($RETRY{interval}秒間隔)\n tie($rowFile): $!\n ";
	confess "【エラー】データファイル\n $rowFile: $!\n " unless ($! == EACCES || $! == EAGAIN);
	$retry --;
	sleep($RETRY{interval});
    }
    confess "【エラー】データファイル\n tie($rowFile): $!\n " unless ($retry);

    my $nDATA = scalar(@{$DATA});
    my $keyFile = $TabDATA->{DataDir}."/$Indexes[$ColPos{$KeyNAME}].gdbm";
    my $keyData = lc($KeyDATA);
    my %KeyData;
    tie(%KeyData,'GDBM_File',$keyFile,($nDATA == 0) ? GDBM_READER : GDBM_WRITER,0660) or confess "【エラー】検索用ファイル\n tie($keyFile): $!\n ";
    if (exists $KeyData{$keyData}) {
	if ($nDATA) {
	    my @pkeys = _split($KeyData{$keyData});
	    if ($nDATA == scalar(@pkeys)) {
		my %pkeys = map {$_ => 1} @pkeys;
		foreach (@{$DATA}) {
		    $nDATA -- if (delete $pkeys{$_});
		}
		if ($nDATA == 0) {
		    $KeyData{$keyData} = _join(@{$DATA});
		    $found = scalar(@pkeys);
		} else {
		    warn "【警告】キー集合が異なるので更新できない: $KeyNAME\n ";
		}
	    } else {
		warn "【警告】キーの数が異なるので更新できない: $KeyNAME\n ";
	    }
	} else {
	    @{$DATA} = _split($KeyData{$keyData});
	    $found = scalar(@{$DATA});
	}
    }
    untie(%KeyData) or warn "【警告】検索用ファイル\n untie($keyFile): $!\n ";

    untie(%RowData) or warn "【警告】データファイル\n untie($rowFile): $!\n ";

    return $found;
}

sub history($$$$)
{
    my $self = shift;
    my ($TabNAME,$KeyDATA,$DATA,$LogKEY) = @_;

    confess "【エラー】存在しないテーブル名: $TabNAME\n " unless (exists $self->{TabList}->{$TabNAME});

    return 0 unless ($LogKEY && $$LogKEY);

    my $TabDATA = $self->{TabList}->{$TabNAME};

    # テーブル定義の読込
    schema($TabDATA);
    my %ColPos = %{$TabDATA->{ColPos}};
    my @Indexes = @{$TabDATA->{Indexes}};
    my $rowFile = $TabDATA->{DataDir}."/RowData.gdbm";
    my $logFile = $TabDATA->{DataDir}."/LogData.gdbm";

    my $retry;

    # データファイルのオープン
    my %RowData;
    $retry = $RETRY{count};
    while ($retry) {
	tie(%RowData,'GDBM_File',$rowFile,GDBM_READER,0660) and last;
	warn "【注意】再試行残りあと $retry 回 ($RETRY{interval}秒間隔)\n tie($rowFile): $!\n ";
	confess "【エラー】データファイル\n tie($rowFile): $!\n " unless ($! == EACCES || $! == EAGAIN);
	$retry --;
	sleep($RETRY{interval});
    }
    confess "【エラー】データファイル\n tie($rowFile): $!\n " unless ($retry);

    my %LogData;
    $retry = $RETRY{count};
    while ($retry) {
	tie(%LogData,'GDBM_File',$logFile,GDBM_READER,0660) and last;
	warn "【注意】再試行残りあと $retry 回 ($RETRY{interval}秒間隔)\n tie($logFile): $!\n ";
	confess "【エラー】検索用ファイル\n tie($logFile): $!\n " unless ($! == EACCES || $! == EAGAIN);
	$retry --;
	sleep($RETRY{interval});
    }
    confess "【エラー】検索用ファイル\n tie($logFile): $!\n " unless ($retry);

    # ログデータの読み込み
    my @logData = ();
    my $logKey = $$LogKEY.'='.$KeyDATA;
    if (exists $LogData{$logKey}) {
	@logData = _split($LogData{$logKey});
	my ($col,$key) = split("\n",$logData[0],2);
	confess "【エラー】更新記録ファイル\n 更新日時がない\n " unless ($col eq '更新日時');
	$logData[0] = "更新日時\n$$LogKEY"; # 本来の更新日時にする。
	$$LogKEY = ($$LogKEY eq $key) ? '' : $key;
    }

    untie(%LogData) or confess "【警告】更新記録ファイル\n untie($logFile): $!\n ";

    untie(%RowData) or warn "【警告】データファイル\n untie($rowFile): $!\n ";

    if (@logData) {
	foreach my $datum (@logData) {
	    my ($col,$val) = split("\n",$datum,2);
	    $DATA->{$col} = $val;
	}
    }

    return 1;
}

1;
# End of file.
