<?php
    header("Expires: Sat, 1 Jan 2000 00:00:00 GMT");			// 有効期限を過去に設定する。
    header("Last-Modified: " . gmdate("D, d M Y H:i:s") . " GMT");	// 常に修正されているように見せかける。
    header("Cache-Control: no-store, no-cache, must-revalidate");	// for HTTP/1.1
    header("Pragma: no-cache");						// for HTTP/1.0
    header("Content-Type: text/html; charset=euc-jp");			// EUC漢字コードを使用する。

    // $Id: view.php,v 1.1 2004/11/08 09:51:18 yoshiy Exp yoshiy $
?>
<!-- <?php
    echo "--\x3e\n"; // HTMLとして処理されないための。

    ini_set('display_errors','on');
    ini_set('error_log','/homes/yoshiy/public_html/php/view/php_error_log');
    include('/userprof/ssc/DataBase/php/database.php');
    define('BaseDir','/userprof/ssc/DataBase/Tables');


    function Detail(&$DB,$dirPath,$tabName,$pkey = null)
    {
	$primary = $DB->primary($tabName);
	$columns = $DB->columns($tabName);
	$indexes = $DB->indexes($tabName);
	$data = array();
	$history = array();
	$args = 'tabName='.urlencode($tabName);
	if (is_null($pkey)) $found = 0;
	else {
	    $args .= '&keyData='.urlencode($pkey);
	    //user_error("Detail: fetch($tabName,$pkey)");
	    $found = $DB->fetch($tabName,$pkey,$data);
	    //user_error("Detail: fetch = $found");
	    if ($found) {
		$next = $data['更新日時'];
		while ($DB->history($tabName,$pkey,$hist = array(),$next)) $history[] = $hist;
	    }
	}
	if (!$found) return;
	echo "<table border='1'>\n";
	echo "<tr>\n";
	echo "<th bgcolor='cornsilk' align='left'>キー</th>\n";
	echo "<td align='left'><a href='".$_SERVER{'PHP_SELF'}."?$args' target='view_detail'>",is_null($pkey)?'<br>':$pkey,"</a></td>\n";
	echo "<td align='left' colspan='",count($history),"'>更新履歴</td>\n";
	echo "</tr>\n";
	$noPlain = empty($_POST['plain/text']);
	foreach ($columns as $col) {
	    echo "<tr>\n";
	    printf("<th bgcolor='cornsilk' align='left' nowrap>%s</th>\n",($col == $primary)?"<font color='red'>$col</font>":$col);
	    if ($found) {
		$value = "$data[$col]";
		if (empty($value)) $value = '<br>';
		else {
		    $nl = 0;
		    $tag = 0;
		    foreach (explode("\n",$value) as $line) {
			$nl ++;
			$notUsed = array();
			$tag += preg_match_all('/<[^>]+>/',$line,$notUsed);
		    }
		    if (isset($indexes[$col])) {
			$args = 'dirPath='.urlencode($dirPath);
			$args .= '&tabName='.urlencode($tabName);
			$args .= '&keyName='.urlencode($col);
			$args .= '&keyData='.urlencode($value);
			if ($tag < $nl) $value = nl2br($value);
			$value = "<a href='".$_SERVER{'PHP_SELF'}."?$args' target='_blank'>$value</a>";
		    } else {
			if ($noPlain) {
			    if ($tag < $nl) $value = nl2br($value);
			} else {
			    $value = nl2br(htmlspecialchars($value,ENT_QUOTES));
			}
		    }
		}
	    } else $value = '<br>';
	    echo "<td nowrap>$value</td>\n";
	    foreach ($history as $hist) {
		$val = '<br>';
		$bgcolor = '';
		if (isset($hist[$col])) {
		    if (strlen($val)) $val = htmlspecialchars(preg_replace('/[\r\n]/',' ',$hist[$col]),ENT_QUOTES);
		    $bgcolor = " bgcolor='orange'";
		}
		echo "<td nowrap$bgcolor>$val</td>\n";
	    }
	    echo "</tr>\n";
	}
	echo "</table>\n";
	echo "<hr>\n";
    }


    function Listup(&$DB,$dirPath,$tabName,&$pkeys)
    {
	$primary = $DB->primary($tabName);
	$columns = $DB->columns($tabName);
	$indexes = $DB->indexes($tabName);
	echo "<table border='1'>\n";
	echo "<tr bgcolor='cornsilk'>\n";
	echo "<th>番号</th>";
	echo "<th>キー</th>";
	foreach ($columns as $col) {
	    printf("<th>%s</th>",($col == $primary)?"<font color='red'>$col</font>":$col);
	}
	echo "</tr>\n";
	foreach ($pkeys as $i => $pkey) {
	    $data = array();
	    echo "<tr>\n";
	    echo "<td align='right'>",$i+1,"</td>";
	    $args = 'dirPath='.urlencode($dirPath);
	    $args .= '&tabName='.urlencode($tabName);
	    $args .= '&keyData='.urlencode($pkey);
	    echo "<td align='left'><a href='".$_SERVER{'PHP_SELF'}."?$args' target='view_detail'>$pkey</a></td>";
	    if ($DB->fetch($tabName,$pkey,$data)) {
		foreach ($columns as $col) {
		    $value = "$data[$col]";
		    if (empty($value)) $value = '<br>';
		    else {
			if (isset($indexes[$col])) {
			    $args = 'dirPath='.urlencode($dirPath);
			    $args .= '&tabName='.urlencode($tabName);
			    $args .= '&keyName='.urlencode($col);
			    $args .= '&keyData='.urlencode($value);
			    $value = htmlspecialchars($value,ENT_QUOTES);
			    $value = "<a href='".$_SERVER{'PHP_SELF'}."?$args' target='view_listup'>$value</a>";
			} else {
			    $value = htmlspecialchars($value,ENT_QUOTES);
			}
		    }
		    echo "<td nowrap>$value</td>\n";
		}
	    } else {
		echo "<td colspan='",count($columns),"'><br></td>\n";
	    }
	    echo "</tr>\n";
	}
	echo "</table>\n";
    }


    function query_form(&$DB,$dirPath,$tabName,$keyName,$keyData)
    {
	echo "<form name='QUERY' method='post'>\n";
	echo "<table border='1' cellpadding='2' cellspacing='2'>\n";
	echo "<tr>\n";
	echo "  <td rowspan='2'>\n";
	echo "    <input name='dirPath' value='${dirPath}' onChange='change_dirPath()'><br>\n";
	$tables = $DB->tables();
	sort($tables);
	echo "    <select name='tabName' onChange='change_tabName()'>\n";
	if (empty($tabName)) {
	echo "      <option value='' selected><tt></tt></option>\n";
	}
	foreach ($tables as $table) {
	    $selected = ($tabName == $table) ? ' selected' : '';
	    $table = htmlspecialchars($table, ENT_QUOTES);
	echo "      <option value='$table'",$selected,"><tt>$table</tt></option>\n";
	}
	echo "    </select>\n";
	echo "  </td>\n";
	echo "  <td>\n";
	if (empty($tabName)) {
	echo "    <input type='hidden' name='keyName' value=''>\n";
	echo "    <input type='hidden' name='keyData' value=''>\n";
	} else {
	    $primary = $DB->primary($tabName);
	    $indexes = $DB->indexes($tabName);
	    $columns = $DB->columns($tabName);
	    if (empty($keyName)) $keyName = $primary;
	echo "    <select name='keyName'>\n";
	    foreach (array_merge($primary,array_keys($indexes)) as $index) {
		$selected = ($keyName == $index) ? ' selected' : '';
		$index = htmlspecialchars($index, ENT_QUOTES);
	echo "      <option value='$index'",$selected,"><tt>$index</tt></option>\n";
	    }
	echo "    </select>\n";
	    $size = strlen($keyData); if ($size < 60) $size = 60;
	echo "    <input type='text' name='keyData' size='$size' value='",htmlspecialchars($keyData, ENT_QUOTES),"'>\n";
	$checked = isset($_POST['ignoreCase']) ? ' checked' : '';
	echo "    <input type='checkbox' name='ignoreCase'",$checked,">小文字\n";
	echo "  </td>\n";
	echo "  <td rowspan='2'>\n";
	echo "    <input type='submit' name='query' value='検索'>\n";
	echo "  </td>\n";
	echo "</tr>\n";
	echo "<tr>\n";
	echo "  <td>\n";
	$query_name = isset($_POST['query_name']) ? $_POST['query_name'] : '';
	$query_preg = isset($_POST['query_preg']) ? $_POST['query_preg'] : '';
	    if (empty($query_name)) $query_name = $primary;
	echo "    <select name='query_name'>\n";
	    foreach ($columns as $index) {
		$selected = ($query_name == $index) ? ' selected' : '';
		$index = htmlspecialchars($index, ENT_QUOTES);
	echo "      <option value='$index'",$selected,"><tt>$index</tt></option>\n";
	    }
	echo "    </select>\n";
	echo "    <input type='text' name='query_preg' size='60' value='$query_preg'>\n";
	echo "    perl正規表現\n";
	echo "  </td>\n";
	echo "</tr>\n";
	}
	echo "</table>\n";
	$checked = isset($_POST['plain/text']) ? ' checked' : '';
	echo "    <input type='checkbox' name='plain/text' readonly",$checked,">plain/text\n";
	echo "    <input type='hidden' name='changed' value=''>\n";
	echo "</form>\n";
    }


    function SEARCH_callback($KeyNAME,$KeyDATA,$data = null)
    {
	//user_error("callback: query_name=".$_POST['query_name']." query_preg=".$_POST['query_preg']);
	if (is_null($data)) {
	    //user_error("callback: KeyNAME=$KeyNAME KeyDATA=$KeyDATA");
	    return preg_match($_POST['query_preg'],$KeyDATA);
	} else {
	    //user_error("callback: data['".$_POST['query_name']."']=".$data[$_POST['query_name']]);
	    if (isset($data[$_POST['query_name']])) return preg_match($_POST['query_preg'],$data[$_POST['query_name']]);
	}
	return 0;
    }


    set_time_limit(0);
    foreach (array('dirPath','tabName','keyName','keyData') as $var) {
	if (isset($_GET[$var]))  {
	    $$var = $_GET[$var];
	} else {
	    if (isset($_POST[$var])) $$var = $_POST[$var];
	}
    }


    echo "\x3c!--"; // HTMLとして処理されないため。
?> -->
<html>
<head>
    <title>テーブル表示[<?=$dirPath?>]</title>
<script language="JavaScript">
<!--
function change_dirPath()
{
    document.QUERY.changed.value = 'yes';
    document.QUERY.submit();
    return true;
}

function change_tabName()
{
    document.QUERY.changed.value = 'yes';
    document.QUERY.submit();
    return true;
}
// -->
</script>
</head>
<body bgcolor="white">
<a name="top">
<h3 align="center">テーブル表示[<font color="red"><?=$dirPath?></font>]</h3>
</a>

<hr noshade>

<!-- <?php
    echo "--\x3e\n"; // HTMLとして処理されないための。

    $userid = strtolower(substr(strrchr($_SERVER{'REMOTE_USER'},0x5c),1));
    if (empty($userid)) exit("【エラー】認証が行われていません：REMOTE_USER が未設定");

    $DB = new DataBase((strpos($dirPath,'/') === 0)?$dirPath:BaseDir."/$dirPath",'tools/view.php');

    if (!in_array($tabName,$DB->tables())) $tabName = '';
    else {
	$primary = $DB->primary($tabName);
	$indexes = $DB->indexes($tabName);
	$indexes[$primary] = 'Primary';
	if (!in_array($keyName,$DB->tables($tabName))) $keyName = $primary;
    }

    if (empty($dirPath) or empty($tabName) or empty($keyName)) {
	query_form($DB,$dirPath,$tabName,$keyName,$keyData);
    } else {
	echo "テーブル<strong>[<font color='red'>",$tabName,"</font>]</strong>\n";
	echo $indexes[$keyName],"<strong>[<font color='red'>",$keyName,"</font>]</strong>\n";
	if (empty($_POST['query_preg'])) {
	    $keys = $DB->keys($tabName,$keyName,empty($keyData)?null:$keyData);
	    $total = count($keys);
	} else {
	    if (empty($keyData)) {
		if ($keyName == $primary) {
		    $keys = $DB->search($tabName,"SEARCH_callback");
		} else {
		    $_POST['query_name'] = $keyName;
		    $keys = $DB->search($tabName,"SEARCH_callback",$keyName);
		}
	    } else {
		$keys = $DB->search($tabName,"SEARCH_callback",$keyName,$keyData);
	    }
	    $total = count($keys);
	}
	echo "&nbsp;<strong>",$total,"</strong>&nbsp;selected.\n";
	query_form($DB,$dirPath,$tabName,$keyName,$keyData);
	//echo "(",$DB->count($tabName,$keyName,$keyData),")\n";
	if (isset($keys)) {
	    if ($total == 0) Detail($DB,$dirPath,$tabName);
	    else if ($total == 1 && !empty($keyData)) Detail($DB,$dirPath,$tabName,$keys[0]);
	    else {
		if (($keyName != $primary or !empty($_POST['query_preg'])) && empty($keyData)) {
		    echo "<table border='1' width='100%'>\n";
		    echo "<tr bgcolor='cornsilk'>\n";
		    echo "<th>番号</th>";
		    echo "<th>",empty($keyName)?$primary:$keyName,"</th>";
		    echo "</tr>\n";
		    foreach ($keys as $i => $key) {
			echo "<tr>\n";
			echo "<td align='right'>",$i+1,"</td>";
			$args = 'dirPath='.urlencode($dirPath);
			$args .= '&tabName='.urlencode($tabName);
			$args .= '&keyName='.urlencode($keyName);
			$args .= '&keyData='.urlencode($key);
			$key = htmlspecialchars($key,ENT_QUOTES);
			echo "<td align='left' nowrap><a href='".$_SERVER{'PHP_SELF'}."?$args' target='view_detail'>$key</a></td>";
			echo "</tr>\n";
		    }
		    echo "</table>\n";
		} else Listup($DB,$dirPath,$tabName,$keys);
	    }
	}
    }

    echo "\x3c!--"; // HTMLとして処理されないため。
?> -->

<p align="center">
<a href="#top">Page Topへ</a>
</p>

<hr noshade>
<pre>$Revision: 1.1 $ $Date: 2004/11/08 09:51:18 $</pre>
</body>
</html>
