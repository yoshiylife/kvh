<?php
//
// $Id: DataBase.php,v 1.2 2006/04/05 12:25:08 yoshiy Exp $
//
//
class DataBase
{
    const PAE = "\r\x0C";	// 定数：データ区切り
    protected $RETRY =		// 定数：再試行の
	array(
	    'count' => 20,		// 回数
	    'interval' => 3,		// 間隔(秒)
	);
    
    protected $BaseDir;		// 各テーブル用ディレクトリを配置したディレクトリの絶対パス
    protected $Ident;			// 登録者／更新者のデフォルト文字列
    protected $TabList = array();	// テーブル一覧

    function _implode($pieces)
    {
	$data = array();
	foreach ($pieces as $pos => $piece) {
	    $data[$pos] = str_replace("\r","\r\n",$piece); // PAE値に依存
	}
	return implode(self::PAE,$data);
    }

    function _explode($string)
    {
	$pieces = array();
	foreach (explode(self::PAE,$string) as $pos => $piece) {
	    $pieces[$pos] = str_replace("\r\n","\r",$piece); // PAE値に依存
	}
	return $pieces;
    }

    function __construct($baseDir,$ident='')
    {
	$this->BaseDir = $baseDir;
	$this->Ident = $ident;

	// テーブル名のリスト作成
	if ($dir = opendir($this->BaseDir)) {
	     while (($tabName = readdir($dir)) !== false) {
		 if (substr($tabName,0,1) == ".") continue;
		 $path = $this->BaseDir."/$tabName";
		 if (!(is_dir($path) && file_exists($path.'/Schema.gdbm'))) continue;
		 $this->TabList[$tabName] = array('DataDir' => $path);
	     }
	    closedir($dir);
	}

	srand(date("s"));
    }

    function __destruct()
    {
	/* Nothing to do */
    }

    function dump()
    {
	var_dump($this);
    }

    function test()
    {
	foreach ($this->TabList as $tabName => $path) {
	    $this->schema($this->TabList[$tabName]);
	    var_dump($this->TabList[$tabName]);
	}
    }

    function baseDir()
    {
	return $this->BaseDir;
    }

    function exists($TabNAME,$KeyDATA)
    {
	if (!isset($this->TabList[$TabNAME])) {$errmsg="【エラー】存在しないテーブル名: $TabNAME";error_log($errmsg,0);exit($errmsg);}
	$TabDATA = $this->TabList[$TabNAME];
	$data = '';
	return $this->select($TabNAME,null,$KeyDATA,$data);
    }

    function fetch($TabNAME,$KeyDATA,&$DATA)
    {
	if (!isset($this->TabList[$TabNAME])) {$errmsg="【エラー】存在しないテーブル名: $TabNAME";error_log($errmsg,0);exit($errmsg);}
	$TabDATA = $this->TabList[$TabNAME];
	return $this->operate($TabDATA,null,$KeyDATA,$DATA,0,'internal');
    }

    function modify($TabNAME,$PKEY,&$DATA,$IDENT ='')
    {
	$ident = $IDENT;
	if (empty($ident)) $ident = $this->Ident;
	if (!isset($this->TabList[$TabNAME])) {$errmsg="【エラー】存在しないテーブル名: $TabNAME";error_log($errmsg,0);exit($errmsg);}
	if (empty($ident)) {$errmsg="【エラー】登録者／更新者の指定がない: $TabNAME";error_log($errmsg,0);exit($errmsg);}
	$TabDATA = $this->TabList[$TabNAME];
	return $this->operate($TabDATA,null,$PKEY,$DATA,1,$ident);
    }

    function insert($TabNAME,$PKEY,&$DATA,$IDENT = '')
    {
	$ident = $IDENT;
	if (empty($ident)) $ident = $this->Ident;
	if (!isset($this->TabList[$TabNAME])) {$errmsg="【エラー】存在しないテーブル名: $TabNAME";error_log($errmsg,0);exit($errmsg);}
	if (empty($ident)) {$errmsg="【エラー】登録者／更新者の指定がない: $TabNAME";error_log($errmsg,0);exit($errmsg);}
	$TabDATA = $this->TabList[$TabNAME];
	return $this->operate($TabDATA,null,$PKEY,$DATA,2,$ident);
    }

    function update($TabNAME,$PKEY,&$DATA,$IDENT = '')
    {
	$ident = $IDENT;
	if (empty($ident)) $ident = $this->Ident;
	if (!isset($this->TabList[$TabNAME])) {$errmsg="【エラー】存在しないテーブル名: $TabNAME";error_log($errmsg,0);exit($errmsg);}
	if (empty($ident)) {$errmsg="【エラー】登録者／更新者の指定がない: $TabNAME";error_log($errmsg,0);exit($errmsg);}
	$TabDATA = $this->TabList[$TabNAME];
	return $this->operate($TabDATA,null,$PKEY,$DATA,3,$ident);
    }

    function obtain($TabNAME,$KeyNAME,$KeyDATA,&$DATA)
    {
	if (!isset($this->TabList[$TabNAME])) {$errmsg="【エラー】存在しないテーブル名: $TabNAME";error_log($errmsg,0);exit($errmsg);}
	$TabDATA = $this->TabList[$TabNAME];
	return $this->operate($TabDATA,$KeyNAME,$KeyDATA,$DATA,0,'internal');
    }

    function expan($TabNAME,$KeyNAME,$KeyDATA,&$DATA)
    {
	if (!isset($this->TabList[$TabNAME])) {$errmsg="【エラー】存在しないテーブル名: $TabNAME";error_log($errmsg,0);exit($errmsg);}
	$TabDATA = $this->TabList[$TabNAME];
	$expan = array();
	if ($this->operate($TabDATA,$KeyNAME,$DATA[$KeyDATA],$expan,0,'internal')) {
	    foreach ($expan as $col => $val) {
		if (!isset($DATA[$col])) $DATA[$col] = $val;
	    }
	    return 1;
	}
	return 0;
    }

    function indexes($TabNAME)
    {
	if (!isset($this->TabList[$TabNAME])) {$errmsg="【エラー】存在しないテーブル名: $TabNAME";error_log($errmsg,0);exit($errmsg);}

	$TabDATA = $this->TabList[$TabNAME];

	// テーブル定義の読込
	$this->schema($TabDATA);
	$Columns = $TabDATA['Columns'];
	$Indexes = $TabDATA['Indexes'];

	$indexes = array();
	foreach ($Columns as $pos => $col) {
	    if (isset($Indexes[$pos])) $indexes[$col] = $Indexes[$pos];
	}
	#$indexes[$TabDATA['Primary']] = 'RowData';

	return $indexes;
    }

    function primary($TabNAME)
    {
	if (!isset($this->TabList[$TabNAME])) {$errmsg="【エラー】存在しないテーブル名: $TabNAME";error_log($errmsg,0);exit($errmsg);}

	$TabDATA = $this->TabList[$TabNAME];

	// テーブル定義の読込
	$this->schema($TabDATA);

	return $TabDATA['Primary'];
    }

    function columns($TabNAME)
    {
	if (!isset($this->TabList[$TabNAME])) {$errmsg="【エラー】存在しないテーブル名: $TabNAME";error_log($errmsg,0);exit($errmsg);}

	$TabDATA = $this->TabList[$TabNAME];

	// テーブル定義の読込
	$this->schema($TabDATA);
	$Columns = $TabDATA['Columns'];

	return $Columns;
    }

    function tables()
    {
	$tables = array();
	foreach (array_keys($this->TabList) as $tabName) {
	    $tables[] = $tabName;
	}
	return $tables;
    }

    function schema(&$TabDATA)
    {
	$php_errormsg = 'No Error';
	$scmFile = $TabDATA['DataDir'].'/Schema.gdbm';
	$scmData = array();
	$RETRY = $this->RETRY;

	// テーブル構造データの読込
	$retry = $RETRY['count'];
	$php_errormsg = 'No Error';
	while ($retry) {
	    $ScmData = dba_open($scmFile,'r','gdbm') or user_error("【警告】dba_open($scmFile): $php_errormsg");
	    if ($ScmData) break;
	    user_error("【注意】再試行残りあと $retry 回 (${RETRY['interval']}秒間隔): dba_open($scmFile)");
	    $retry --;
	    sleep($RETRY['interval']);
	}
	if (!$retry) {$errmsg="【エラー】再試行オーバー: dba_open($scmFile)";error_log($errmsg,0);exit($errmsg);}
	$key = dba_firstkey($ScmData);
	while ($key) {
	      $scmData[$key] = dba_fetch($key,$ScmData); // or exit("【エラー】データ読込 $scmFile($key): $php_errormsg");
	      $key = dba_nextkey($ScmData); // or exit("【エラー】データ読込 $scmFile: $php_errormsg");
	}
	dba_close($ScmData);

	// テーブル項目名 => データ配列中の位置
	$TabDATA['Columns'] = $this->_explode($scmData['Columns']);
	$TabDATA['ColPos'] = array();
	foreach ($TabDATA['Columns'] as $pos => $col) {
	    $TabDATA['ColPos'][$col] = $pos;
	}
	$TabDATA['Primary'] = $scmData['Primary'];

	// テーブル項目名 => 検索用索引名
	$TabDATA['Indexes'] = array();
	reset($scmData);
	while (list($key,$col) = each($scmData)) {
	    if (preg_match('/^Key/',$key)) {
		$pos = $TabDATA['ColPos'][$col];
		$TabDATA['Indexes'][$pos] = $key;
	    }
	}

	return 1;
    }

    function create($args)
    {
	$php_errormsg = 'No Error';
	$argc = func_num_args();
	$args = func_get_args();
	if ($argc < 2) {$errmsg="【エラー】引数が足りない。";error_log($errmsg,0);exit($errmsg);}
	$TabNAME = $args[0];
	$DataCols = $args[1];
	if (2 < $argc) $DataKeys = $args[2];
	if (3 < $argc) $DATA = $args[3];
	if (4 < $argc) $ident = $args[4];

	$dataCols = $DataCols;
	$dataKeys = is_null($DataKeys) ? array($dataCols[0]) : $DataKeys;

	if (empty($ident)) $ident = $this->Ident;

	if (empty($ident)) {$errmsg="【エラー】登録者／更新者の指定がない: $TabNAME";error_log($errmsg,0);exit($errmsg);}

	$dataDir = $this->BaseDir."/$TabNAME";

	// テーブル用ディレクトリの作成
	if (!@mkdir($dataDir,0770)) {
	    if (!is_dir($dataDir)) {$errmsg="【エラー】mkdir($dataDir): $php_errormsg";error_log($errmsg,0);exit($errmsg);}
	}

	// データ項目の位置を算定する。(テーブル項目名 => データ配列中の位置)
	$colPos = array();
	foreach ($dataCols as $pos => $col) {
	    if (isset($colPos[$dataCols[$pos]])) {$errmsg="【エラー】項目名が重複している: ".$dataCols[$pos];error_log($errmsg,0);exit($errmsg);}
	    $colPos[$col] = $pos;
	}

	// 必須項目のチェック
	foreach (array('登録日時','登録者','更新日時','更新者') as $col) {
	    if (!isset($colPos[$col])) {$errmsg="【エラー】必須項目がない: $col";error_log($errmsg,0);exit($errmsg);}
	}

	// 索引項目のチェック
	$indexes = array();
	$primary = $dataKeys[0];
	foreach ($dataKeys as $pos => $col) {
	    if (!isset($colPos[$col])) {$errmsg="【エラー】索引項目がない: $col";error_log($errmsg,0);exit($errmsg);}
	    if ($col == $primary) continue;
	    $indexes[$col] = sprintf("Key%04d",$pos);
	}

	// テーブル構造定義を作成する。
	$scmFile = "$dataDir/Schema.gdbm";
	if (file_exists($scmFile)) {$errmsg="【エラー】定義ファイルが既存: $scmFile";error_log($errmsg,0);exit($errmsg);}
	if (!($ScmData = dba_open($scmFile,'n','gdbm'))) {$errmsg="【エラー】定義ファイル dba_open($scmFile): $php_errormsg";error_log($errmsg,0);exit($errmsg);}
	if (!dba_insert('Primary',$primary,$ScmData)) {$errmsg="【エラー】データ書込 $scmFile: $php_errormsg";error_log($errmsg,0);exit($errmsg);}
	if (!dba_insert('Columns',$this->_implode($dataCols),$ScmData)) {$errmsg="【エラー】データ書込 $scmFile: $php_errormsg";error_log($errmsg,0);exit($errmsg);}
	foreach ($indexes as $col => $key) {
	    if (!dba_insert($key,$col,$ScmData)) {$errmsg="【エラー】データ書込 $scmFile: $php_errormsg";error_log($errmsg,0);exit($errmsg);}
	}
	$scmData = array();
	for ($key = dba_firstkey($ScmData); $key; $key = dba_nextkey($ScmData)) {
	    if (!($scmData[$key] = dba_fetch($key,$ScmData))) {$errmsg="【エラー】データ読込 $scmFile: $php_errormsg";error_log($errmsg,0);exit($errmsg);}
	}
	dba_close($ScmData);

	// データファイルを初期化する。
	foreach (array_merge(array('RowData','LogData'),array_values($indexes)) as $name) {
	    if (empty($name)) continue;
	    $file = "$dataDir/$name.gdbm";
	    if (!($Data = dba_open($file,'n','gdbm'))) {$errmsg="【エラー】データファイル dba_open($file): $php_errormsg";error_log($errmsg,0);exit($errmsg);}
	    dba_close($Data);
	}

	$this->TabList[$TabNAME] = array('DataDir' => $dataDir);

	//if (is_array($DATA)) $this->isetup($TabNAME,$DATA,$ident);

	return $scmData;
    }

    private function _rowid($RowData)
    {
	for (;;) {
	    $rowid = sprintf("%04x%04x",rand(0,0xffff),rand(0,0xffff));
	    if (!dba_exists($rowid,$RowData)) break;
	}
	return $rowid;
    }

    protected function operate(&$TabDATA,$KeyNAME,$KeyDATA,&$DATA,$mode,$Ident)
    {
	$php_errormsg = 'No Error';
	// テーブル定義の読込
	$this->schema($TabDATA);
	$ColPos = $TabDATA['ColPos'];
	$Indexes = $TabDATA['Indexes'];
	$rowFile = $TabDATA['DataDir']."/RowData.gdbm";
	$logFile = $TabDATA['DataDir']."/LogData.gdbm";
	$RETRY = $this->RETRY;

	if (is_null($KeyNAME)) $KeyNAME = $TabDATA['Primary'];
	if ($KeyNAME != $TabDATA['Primary']) {
	    if (!isset($ColPos[$KeyNAME]) || !isset($Indexes[$ColPos[$KeyNAME]])) {$errmsg="【エラー】検索用索引がない: $KeyNAME";error_log($errmsg,0);exit($errmsg);}
	}

	if ($mode) set_time_limit(0);

	$found = 0;

	// データファイルのオープン
	$retry = $RETRY['count'];
	$php_errormsg = 'No Error';
	while ($retry) {
	    $RowData = dba_open($rowFile,($mode == 0) ? 'r' : 'w','gdbm') or user_error("【警告】データファイル dba_open($rowFile): $php_errormsg");
	    if ($RowData) break;
	    user_error("【注意】再試行残りあと $retry 回 (${RETRY['interval']}秒間隔): dba_open($rowFile)");
	    $retry --;
	    sleep($RETRY['interval']);
	}
	if (!$retry) {$errmsg="【エラー】再試行オーバー: dba_open($rowFile)";error_log($errmsg,0);exit($errmsg);}

	// プライマリキーの計算
	if ($KeyNAME == $TabDATA['Primary']) {
	    $PKEY = $KeyDATA;
	} else {
	    $keyFile = $TabDATA['DataDir']."/${Indexes[$ColPos[$KeyNAME]]}.gdbm";
	    $keyData = strtolower($KeyDATA);
	    $retry = $RETRY['count'];
	    $php_errormsg = 'No Error';
	    while ($retry) {
		$KeyData = dba_open($keyFile,'r','gdbm') or user_error("【警告】検索用ファイル dba_open($keyFile): $php_errormsg");
		if ($KeyData) break;
		user_error("【注意】再試行残りあと $retry 回 (${RETRY['interval']}秒間隔): dba_open($keyFile)");
		$retry --;
		sleep($RETRY['interval']);
	    }
	    if (!$retry) {$errmsg="【エラー】再試行オーバー: dba_open($keyFile)";error_log($errmsg,0);exit($errmsg);}
	    $pkey = array();
	    if (dba_exists($keyData,$KeyData)) {
		if (!($pkey = $this->_explode(dba_fetch($keyData,$KeyData)))) {$errmsg="【エラー】データ読込 $keyFile: $php_errormsg";error_log($errmsg,0);exit($errmsg);}
	    }
	    dba_close($KeyData);
	    if (count($pkey) == 1) {
		$PKEY = $pkey[0];
	    } else {
		if (count($pkey) != 0) {$errmsg="【エラー】$KeyNAME=$KeyDATA 検索結果が複数です。".implode(',',$pkey);error_log($errmsg,0);exit($errmsg);}
		unset($PKEY);
	    }
	}

	// データの読出
	$data = array();
	$retry = $RETRY['count'];
	$php_errormsg = 'No Error';
	while ($retry) {
	    $error = 0;
	    if (isset($PKEY) && dba_exists($PKEY,$RowData)) {
		//user_error("PKEY[$PKEY] Found");
		// 内部データへの変換
		$data = $this->_explode(dba_fetch($PKEY,$RowData)) or $error ++;
		$found ++;
	    } else {
		// 内部データの初期化
		while (list($col,$pos) = each($ColPos)) {
		    $data[$pos] = "";
		}
	    }
	    if (!$error) break;
	    user_error("【注意】再試行残りあと $retry 回 (${RETRY['interval']}秒間隔) $rowFile: $php_errormsg");
	    $retry --;
	    sleep($RETRY['interval']);
	}
	if (!$retry) {$errmsg="【エラー】データ読込: $rowFile";error_log($errmsg,0);exit($errmsg);}

	// ユニークキーの自動生成(ROWID)
	if ($found == 0 and ($mode == 2 || $mode == 3)) {
	    if ($KeyNAME == 'ROWID') {
		$PKEY = (empty($KeyDATA) or dba_exists($KeyDATA,$RowData)) ? $this->_rowid($RowData) : $KeyDATA;
		$DATA['ROWID'] = $PKEY;
	    } else {
		if (isset($ColPos['ROWID'])) {
		    $pos = $ColPos['ROWID'];
		    $keyFile = $TabDATA['DataDir']."/${Indexes[$pos]}.gdbm";
		    $retry = $RETRY['count'];
		    $php_errormsg = 'No Error';
		    while ($retry) {
			$KeyData = dba_open($keyFile,'r','gdbm') or user_error("【警告】検索用ファイル dba_open($keyFile): $php_errormsg");
			if ($KeyData) break;
			user_error("【注意】再試行残りあと $retry 回 (${RETRY['interval']}秒間隔): dba_open($keyFile)");
			$retry --;
			sleep($RETRY['interval']);
		    }
		    if (!$retry) {$errmsg="【エラー】検索用ファイル: dba_open($keyFile)";error_log($errmsg,0);exit($errmsg);}
		    if (empty($DATA['ROWID']) or dba_exists($DATA['ROWID'],$KeyData)) $DATA['ROWID'] = $this->_rowid($KeyData);
		    dba_close($KeyData);
		}
	    }
	}

	// 更新条件チェック
	if ($mode) {
	    if ($found and isset($DATA['更新日時'])) {
		if (!($data[$ColPos['更新日時']] == $DATA['更新日時'])) {
		    // 更新不可
		    user_error("【警告】更新日時が一致しないので更新できません。: $rowFile");
		    unset($PKEY);
		    $found = 0;
		}
	    }
	}

	if (isset($PKEY) and $mode and (($mode % 2) == $found || $mode == 3)) {
	    // 呼び出し側による更新を許可しない項目の上書き
	    if ($found and isset($ColPos['ROWID'])) $DATA['ROWID'] = $data[$ColPos['ROWID']];
	    // 呼び出し側による更新を許可しない項目の削除
	    if (isset($DATA['登録日時'])) unset($DATA['登録日時']);
	    if (isset($DATA['更新日時'])) unset($DATA['更新日時']);
	    if (isset($DATA['登録者'])) unset($DATA['登録者']);
	    if(empty($data[$ColPos['登録者']])) $DATA['登録者'] = $Ident;
	    $DATA['更新者'] = $Ident;

	    $now = strftime("%Y/%m/%d %H:%M:%S");
	    if (!$found) {
		$data[$ColPos['登録日時']] = $now; # 最初の更新記録のキー
		$data[$ColPos['更新日時']] = $now; # 最後の更新記録のキー
	    }

	    // 更新データの作成
	    $renew = array();
	    $renew[] = "更新日時\n".$data[$ColPos['更新日時']]; # 更新日時は常に更新扱い
	    reset($ColPos);
	    while (list($col,$pos) = each($ColPos)) {
		if (isset($DATA[$col])) {
		    # ログ用データ
		    if (!empty($DATA[$col])) {
			if ($data[$pos] != $DATA[$col]) $renew[] = "$col\n".$DATA[$col];
		    } else {
			$renew[] = "$col\n";
		    }
		    # undef値を許可 -> データのクリア
		    $old = $data[$pos];
		    $new = $DATA[$col];
		    $data[$pos] = $DATA[$col];

		    // 検索用索引の更新
		    if (isset($Indexes[$pos])) {
			if (empty($old) || empty($new) || strcmp($old,$new) != 0) {
			    $keyData = array();
			    $keyFile = $TabDATA['DataDir']."/${Indexes[$pos]}.gdbm";
			    $retry = $RETRY['count'];
			    $php_errormsg = 'No Error';
			    while ($retry) {
				$KeyData = dba_open($keyFile,'w','gdbm') or user_error("【警告】検索用ファイル dba_open($keyFile): $php_errormsg");
				if ($KeyData) break;
				user_error("【注意】再試行残りあと $retry 回 (${RETRY['interval']}秒間隔): dba_open($keyFile)");
				$retry --;
				sleep($RETRY['interval']);
			    }
			    if (!$retry) {$errmsg="【エラー】検索用ファイル: dba_open($keyFile)";error_log($errmsg,0);exit($errmsg);}
			    if (!empty($old)) {
				$old = strtolower($old);
				# 削除
				if (dba_exists($old,$KeyData)) {
				    if (!($temp = dba_fetch($old,$KeyData))) {$errmsg="【エラー】データ読込 $keyFile: $php_errormsg";error_log($errmsg,0);exit($errmsg);}
				    foreach ($this->_explode($temp) as $key) {
					if ($PKEY != $key) $keyData[] = $key;
				    }
				}
				if (count($keyData)) {
				    dba_replace($old,$this->_implode($keyData),$KeyData) or exit("【エラー】データ書込 $keyFile: $php_errormsg");
				} else {
				    dba_delete($old,$KeyData) or user_error("【警告】データ削除 $keyFile: $php_errormsg");
				}
			    }
			    if (!empty($new)) {
				$new = strtolower($new);
				# 追加
				if (dba_exists($new,$KeyData)) {
				    if (!($keyData = $this->_explode(dba_fetch($new,$KeyData)))) {$errmsg="【エラー】データ読込 $keyFile: $php_errormsg";error_log($errmsg,0);exit($errmsg);}
				}
				if (!in_array($PKEY,$keyData)) {
				    $keyData[] = $PKEY;
				    dba_replace($new,$this->_implode($keyData),$KeyData) or exit("【エラー】データ書込 $keyFile: $php_errormsg");
				}
			    }
			    dba_close($KeyData);
			}
		    }
		}
	    }

	    # データ更新の失敗に備え、先に更新記録に追加('更新日時'は更新前のもの)
	    $retry = $RETRY['count'];
	    $php_errormsg = 'No Error';
	    while ($retry) {
		$LogData = dba_open($logFile,'w','gdbm') or user_error("【警告】更新記録ファイル dba_open($logFile): $php_errormsg");
		if ($LogData) break;
		user_error("【注意】再試行残りあと $retry 回 (${RETRY['interval']}秒間隔): dba_open($logFile)");
		$retry --;
		sleep($RETRY['interval']);
	    }
	    if (!$retry) {$errmsg="【エラー】更新記録ファイル: dba_open($logFile)";error_log($errmsg,0);exit($errmsg);}
	    if (dba_exists($now.'='.$PKEY,$LogData)) {
		sleep(1);
		$now = strftime("%Y/%m/%d %H:%M:%S");
	    }
	    $key = $now.'='.$PKEY;
	    dba_insert($key,$this->_implode($renew),$LogData) or user_error("【警告】更新記録ファイルの更新 $logFile($key): $php_errormsg");
	    dba_close($LogData);

	    # データの登録＆更新
	    $data[$ColPos['更新日時']] = $now;
	    $retry = $RETRY['count'];
	    $php_errormsg = 'No Error';
	    while ($retry) {
		$error = 0;
		if (dba_exists($PKEY,$RowData)) {
		    dba_replace($PKEY,$this->_implode($data),$RowData) or $error ++;
		} else {
		    dba_insert($PKEY,$this->_implode($data),$RowData) or $error ++;
		}
		if (!$error) break;
		user_error("【注意】再試行残りあと $retry 回 (${RETRY['interval']}秒間隔) $rowFile: $php_errormsg");
		$retry --;
		sleep($RETRY['interval']);
	    }
	    if (!$retry) {$errmsg="【エラー】データ書込 $rowFile: $php_errormsg";error_log($errmsg,0);exit($errmsg);}

	    $found ++;
	}

	dba_close($RowData);

	if ($found) {
	    foreach ($ColPos as $col => $pos) {
		$DATA[$col] = $data[$pos];
	    }
	}

	return $found;
    }


    function select($TabNAME,$KeyNAME,$KeyDATA,&$DATA,$POS = 0)
    {
	$php_errormsg = 'No Error';
	//user_error("TabNAME=$TabNAME KeyDATA=$KeyDATA");
	if (!isset($this->TabList[$TabNAME])) {$errmsg="【エラー】存在しないテーブル名: $TabNAME";error_log($errmsg,0);exit($errmsg);}

	$TabDATA = $this->TabList[$TabNAME];

	// テーブル定義の読込
	$this->schema($TabDATA);
	$ColPos = $TabDATA['ColPos'];
	$Indexes = $TabDATA['Indexes'];
	$rowFile = $TabDATA['DataDir']."/RowData.gdbm";
	$RETRY = $this->RETRY;

	if (is_null($KeyNAME)) $KeyNAME = $TabDATA['Primary'];
	if ($KeyNAME != $TabDATA['Primary']) {
	    if (!isset($ColPos[$KeyNAME]) || !isset($Indexes[$ColPos[$KeyNAME]])) {$errmsg="【エラー】検索用索引がない: $KeyNAME";error_log($errmsg,0);exit($errmsg);}
	}

	$found = 0;

	// データファイルのオープン
	$retry = $RETRY['count'];
	$php_errormsg = 'No Error';
	while ($retry) {
	    $RowData = dba_open($rowFile,'r','gdbm') or user_error("【警告】データファイル dba_open($rowFile): $php_errormsg");
	    if ($RowData) break;
	    user_error("【注意】再試行残りあと $retry 回 (${RETRY['interval']}秒間隔): dba_open($rowFile)");
	    $retry --;
	    sleep($RETRY['interval']);
	}
	if (!$retry) {$errmsg="【エラー】データファイル: dba_open($rowFile)";error_log($errmsg,0);exit($errmsg);}

	// 検索用索引キーから主キーへの変換(要複数対応)
	$PKEYs = array();
	if ($KeyNAME == $TabDATA['Primary']) {
	    if (is_null($KeyDATA)) {
		for ($key = dba_firstkey($RowData); $key; $key = dba_nextkey($RowData)) {
		    $PKEYs[] = $key;
		}
	    } else {
		if (dba_exists($KeyDATA,$RowData)) $PKEYs[] = $KeyDATA;
	    }
	} else {
	    $keyFile = $TabDATA['DataDir']."/${Indexes[$ColPos[$KeyNAME]]}.gdbm";
	    $keyData = strtolower($KeyDATA);
	    $retry = $RETRY['count'];
	    $php_errormsg = 'No Error';
	    while ($retry) {
		$KeyData = dba_open($keyFile,'r','gdbm') or user_error("【エラー】検索用ファイル dba_open($keyFile): $php_errormsg");
		if ($KeyData) break;
		user_error("【注意】再試行残りあと $retry 回 (${RETRY['interval']}秒間隔): dba_open($keyFile)");
		$retry --;
		sleep($RETRY['interval']);
	    }
	    if (!$retry) {$errmsg="【エラー】データファイル: dba_open($keyFile)";error_log($errmsg,0);exit($errmsg);}
	    if (dba_exists($keyData,$KeyData)) {
		$PKEYs = $this->_explode(dba_fetch($keyData,$KeyData)) or user_error("【エラー】データ読込 $keyFile: $php_errormsg");
	    }
	    dba_close($KeyData);
	}

	if (is_array($DATA)) {

	    // データ格納先の矯正
	    foreach ($DATA as $col => $val) {
		if (empty($ColPos[$col])) continue;
		if ($POS == 0 && !is_array($val)) $DATA[$col] = array();
	    }

	    $columns = array_keys($DATA);
	    foreach ($PKEYs as $PKEY) {
		// データの読出
		$data = array();
		$retry = $RETRY['count'];
		$php_errormsg = 'No Error';
		while ($retry) {
		    $error = 0;
		    if (dba_exists($PKEY,$RowData)) {
			$found ++;
			$data = $this->_explode(dba_fetch($PKEY,$RowData)) or $error ++;
			# データ項目の選定
			foreach ($columns as $col) {
			    if (isset($ColPos[$col])) $DATA[$col][$POS] = $data[$ColPos[$col]];
			}
			$POS ++;
		    }
		    if (!$error) break;
		    user_error("【注意】再試行残りあと $retry 回 (${RETRY['interval']}秒間隔) $rowFile: $php_errormsg");
		    $retry --;
		    sleep($RETRY['interval']);
		}
		if (!$retry) {$errmsg="【エラー】データ読込: $rowFile";error_log($errmsg,0);exit($errmsg);}
	    }

	} else {
	    $found = count($PKEYs);
	}

	dba_close($RowData);
	if ($found and count($PKEYs) != $found) user_error("【エラー】データ不整合 $rowFile: $found [".implode(',',$PKEYs)."]");

	return $found;
    }

    function search($TabNAME,$CallBACK = null,$KeyNAME = null,$KeyDATA = null)
    {
	$php_errormsg = 'No Error';
	if (!isset($this->TabList[$TabNAME])) {$errmsg="【エラー】存在しないテーブル名: $TabNAME";error_log($errmsg,0);exit($errmsg);}

	$TabDATA = $this->TabList[$TabNAME];

	// テーブル定義の読込
	$this->schema($TabDATA);
	$ColPos = $TabDATA['ColPos'];
	$Indexes = $TabDATA['Indexes'];
	$rowFile = $TabDATA['DataDir']."/RowData.gdbm";
	$RETRY = $this->RETRY;

	if (is_null($KeyNAME)) $KeyNAME = $TabDATA['Primary'];
	if ($KeyNAME != $TabDATA['Primary']) {
	    if (!isset($ColPos[$KeyNAME]) || !isset($Indexes[$ColPos[$KeyNAME]])) {$errmsg="【エラー】検索用索引がない: $KeyNAME";error_log($errmsg,0);exit($errmsg);}
	}

	if (is_null($CallBACK)) $CallBACK = '';

	$pkeys = array();

	// データファイルのオープン
	$retry = $RETRY['count'];
	$php_errormsg = 'No Error';
	while ($retry) {
	    $RowData = dba_open($rowFile,'r','gdbm') or user_error("【警告】データファイル dba_open($rowFile): $php_errormsg");
	    if ($RowData) break;
	    user_error("【注意】再試行残りあと $retry 回 (${RETRY['interval']}秒間隔): dba_open($rowFile)");
	    $retry --;
	    sleep($RETRY['interval']);
	}
	if (!$retry) {$errmsg="【エラー】データファイル: dba_open($rowFile)";error_log($errmsg,0);exit($errmsg);}

	if ($KeyNAME == $TabDATA['Primary']) {
	    if (isset($KeyDATA)) {
		if (dba_exists($KeyDATA,$RowData)) $PKEYs[] = $KeyDATA;
	    } else {
		if (is_callable($CallBACK)) {
		    for ($key = dba_firstkey($RowData); $key; $key = dba_nextkey($RowData)) {
			$PKEYs[] = $key;
		    }
		} else {
		    for ($key = dba_firstkey($RowData); $key; $key = dba_nextkey($RowData)) {
			$pkeys[] = $key;
		    }
		}
	    }
	} else {
	    $keyFile = $TabDATA['DataDir']."/${Indexes[$ColPos[$KeyNAME]]}.gdbm";
	    $retry = $RETRY['count'];
	    $php_errormsg = 'No Error';
	    while ($retry) {
		$KeyData = dba_open($keyFile,'r','gdbm') or user_error("【エラー】検索用ファイル dba_open($keyFile): $php_errormsg");
		if ($KeyData) break;
		user_error("【注意】再試行残りあと $retry 回 (${RETRY['interval']}秒間隔): dba_open($keyFile)");
		$retry --;
		sleep($RETRY['interval']);
	    }
	    if (!$retry) {$errmsg="【エラー】データファイル: dba_open($keyFile)";error_log($errmsg,0);exit($errmsg);}
	    if (isset($KeyDATA)) {
		$keyData = strtolower($KeyDATA);
		if (dba_exists($keyData,$KeyData)) {
		    $PKEYs = $this->_explode(dba_fetch($keyData,$KeyData)) or user_error("【エラー】データ読込 $keyFile: $php_errormsg");
		}
	    } else {
		for ($key = dba_firstkey($KeyData); $key; $key = dba_nextkey($KeyData)) {
		    if (!is_callable($CallBACK) || call_user_func($CallBACK,$KeyNAME,$key)) {
			$pkeys = array_merge($pkeys,$this->_explode(dba_fetch($key,$KeyData))) or user_error("【エラー】データ読込 $keyFile: $php_errormsg");
		    }
		}
	    }
	    dba_close($KeyData);
	}

	if (isset($PKEYs)) {
	    foreach ($PKEYs as $PKEY) {
		// データの読出
		$data = array();
		$retry = $RETRY['count'];
		$php_errormsg = 'No Error';
		while ($retry) {
		    $error = 0;
		    if (dba_exists($PKEY,$RowData)) {
			if (is_callable($CallBACK)) {
			    $data = $this->_explode(dba_fetch($PKEY,$RowData)) or $error ++;
			    $DATA = array();
			    foreach ($ColPos as $col => $pos) {
				$DATA[$col] = $data[$pos];
			    }
			    if (call_user_func($CallBACK,$KeyNAME,$PKEY,$DATA)) $pkeys[] = $PKEY;
			} else {
			    $pkeys[] = $PKEY;
			}
		    }
		    if (!$error) break;
		    user_error("【注意】再試行残りあと $retry 回 (${RETRY['interval']}秒間隔) $rowFile: $php_errormsg");
		    $retry --;
		    sleep($RETRY['interval']);
		}
		if (!$retry) {$errmsg="【エラー】データ読込: $rowFile";error_log($errmsg,0);exit($errmsg);}
	    }
	}

	dba_close($RowData);

	return $pkeys;
    }

    function keys($TabNAME,$KeyNAME = null,$KeyDATA = null)
    {
	return $this->search($TabNAME,null,$KeyNAME,$KeyDATA);
    }

    function count($TabNAME,$KeyNAME = null,$KeyDATA = null)
    {
	return count($this->search($TabNAME,null,$KeyNAME,$KeyDATA));
    }

    function reorder($TabNAME,$KeyNAME,$KeyDATA,&$DATA)
    {
	$php_errormsg = 'No Error';
	//user_error("TabNAME=$TabNAME KeyDATA=$KeyDATA");
	if (!isset($this->TabList[$TabNAME])) {$errmsg="【エラー】存在しないテーブル名: $TabNAME";error_log($errmsg,0);exit($errmsg);}

	$TabDATA = $this->TabList[$TabNAME];

	// テーブル定義の読込
	$this->schema($TabDATA);
	$ColPos = $TabDATA['ColPos'];
	$Indexes = $TabDATA['Indexes'];
	$rowFile = $TabDATA['DataDir']."/RowData.gdbm";
	$RETRY = $this->RETRY;

	if ($KeyNAME == $TabDATA['Primary']) {$errmsg="【エラー】プライマリ索引には適用できない: $KeyNAME";error_log($errmsg,0);exit($errmsg);}
	if (!isset($ColPos[$KeyNAME]) || !isset($Indexes[$ColPos[$KeyNAME]])) {$errmsg="【エラー】検索用索引がない: $KeyNAME";error_log($errmsg,0);exit($errmsg);}

	$found = 0;

	// データファイルのオープン
	$retry = $RETRY['count'];
	$php_errormsg = 'No Error';
	while ($retry) {
	    $RowData = dba_open($rowFile,'r','gdbm') or user_error("【警告】データファイル dba_open($rowFile): $php_errormsg");
	    if ($RowData) break;
	    user_error("【注意】再試行残りあと $retry 回 (${RETRY['interval']}秒間隔): dba_open($rowFile)");
	    $retry --;
	    sleep($RETRY['interval']);
	}
	if (!$retry) {$errmsg="【エラー】データファイル: dba_open($rowFile)";error_log($errmsg,0);exit($errmsg);}

	$nDATA = count($DATA);
	$keyFile = $TabDATA['DataDir']."/${Indexes[$ColPos[$KeyNAME]]}.gdbm";
	$keyData = strtolower($KeyDATA);
	$retry = $RETRY['count'];
	$php_errormsg = 'No Error';
	while ($retry) {
	    $KeyData = dba_open($keyFile,($nDATA == 0) ? 'r' : 'w','gdbm') or user_error("【エラー】検索用ファイル dba_open($keyFile): $php_errormsg");
	    if ($KeyData) break;
	    user_error("【注意】再試行残りあと $retry 回 (${RETRY['interval']}秒間隔): dba_open($keyFile)");
	    $retry --;
	    sleep($RETRY['interval']);
	}
	if (!$retry) {$errmsg="【エラー】データファイル: dba_open($keyFile)";error_log($errmsg,0);exit($errmsg);}
	if (dba_exists($keyData,$KeyData)) {
	    if ($nDATA) {
		$pkeys = $this->_explode(dba_fetch($keyData,$KeyData)) or user_error("【エラー】データ読込 $keyFile: $php_errormsg");
		if (array_diff($pkeys,$DATA)) {
		    user_error("【エラー】キー集合が異なるので更新できない: $KeyNAME");
		} else {
		    set_time_limit(0);
		    dba_replace($keyData,$this->_implode($DATA),$KeyData);
		    $found = $nDATA;
		}
	    } else {
		$DATA = $this->_explode(dba_fetch($keyData,$KeyData)) or user_error("【エラー】データ読込 $keyFile: $php_errormsg");
		$found = count($DATA);
	    }
	}
	dba_close($KeyData);

	dba_close($RowData);

	return $found;
    }

    function history($TabNAME,$KeyDATA,&$DATA,&$LogKEY)
    {
	$php_errormsg = 'No Error';
	//user_error("TabNAME=$TabNAME KeyDATA=$KeyDATA");
	if (!isset($this->TabList[$TabNAME])) {$errmsg="【エラー】存在しないテーブル名: $TabNAME";error_log($errmsg,0);exit($errmsg);}

	if (empty($LogKEY)) return 0;

	$TabDATA = $this->TabList[$TabNAME];

	// テーブル定義の読込
	$this->schema($TabDATA);
	$ColPos = $TabDATA['ColPos'];
	$Indexes = $TabDATA['Indexes'];
	$rowFile = $TabDATA['DataDir']."/RowData.gdbm";
	$logFile = $TabDATA['DataDir']."/LogData.gdbm";
	$RETRY = $this->RETRY;

	$KeyNAME = $TabDATA['Primary'];

	// データファイルのオープン
	$retry = $RETRY['count'];
	$php_errormsg = 'No Error';
	while ($retry) {
	    $RowData = dba_open($rowFile,'r','gdbm') or user_error("【警告】データファイル dba_open($rowFile): $php_errormsg");
	    if ($RowData) break;
	    user_error("【注意】再試行残りあと $retry 回 (${RETRY['interval']}秒間隔): dba_open($rowFile)");
	    $retry --;
	    sleep($RETRY['interval']);
	}
	if (!$retry) {$errmsg="【エラー】データファイル: dba_open($rowFile)";error_log($errmsg,0);exit($errmsg);}

	$retry = $RETRY['count'];
	$php_errormsg = 'No Error';
	while ($retry) {
	    $LogData = dba_open($logFile,'r','gdbm') or user_error("【警告】更新記録ファイル dba_open($logFile): $php_errormsg");
	    if ($LogData) break;
	    user_error("【注意】再試行残りあと $retry 回 (${RETRY['interval']}秒間隔): dba_open($logFile)");
	    $retry --;
	    sleep($RETRY['interval']);
	}

	$logData = array();
	$logKey = $LogKEY.'='.$KeyDATA;
	if (dba_exists($logKey,$LogData)) {
	    $logData = $this->_explode(dba_fetch($logKey,$LogData));
	    list($col,$key) = explode("\n",$logData[0],2);
	    if ($col != '更新日時') {$errmsg="【エラー】更新記録ファイル ($logFile): 更新日時がない";error_log($errmsg,0);exit($errmsg);}
	    $logData[0] = "更新日時\n$LogKEY"; // 本来の更新日時にする。
	    $LogKEY = ($LogKEY === $key) ? '' : $key;
	}

	dba_close($LogData);

	dba_close($RowData);

	if ($n = count($logData)) {
	    foreach ($logData as $datum) {
		list($col,$val) = explode("\n",$datum,2);
		$DATA[$col] = $val;
	    }
	}

	return $n;
    }
}
?>
