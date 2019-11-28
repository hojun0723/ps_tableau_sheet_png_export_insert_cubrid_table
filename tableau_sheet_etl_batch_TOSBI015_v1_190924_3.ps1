#============================================================
#
# 주별 모니터링 결과 발송
# - tableau 통합문서의 데이터를 추출하여 csv파일로 저장
# - tableau 통합문서의 이미지를 추출하여 png파일로 저장
# - csv 파일 DW 적재
# - 적재 데이터 가공
# - 수신인 그룹 대상으로 모바일 PUSH 및 이미지 첨부메일 발송
#
#============================================================

For($i=0; $i -ge -6; $i--) {

    <#
    -gt :  > 
    -ge : >=
    -eq : ==
    -lt :  <
    -le :  <=
    #>

    #요일을 숫자로 표시하기 위해 -UFormat 옵션 사용
    #Get-Date (Get-Date).AddDays($i)
    $j = Get-Date (Get-Date).AddDays($i) -UFormat %u

    #지난 일요일 알아내기
    if ($j -eq 0) {
        $LastSunday = Get-Date (Get-Date).AddDays($i) -format "yyyy-MM-dd"
        break
    }
}

ECHO OFF
D:
cd "D:\Tableau Server\packages\bin.20183.19.0212.1312"
"********************************************************************************************* Tableau Server Login"
tabcmd login -s http://localhost:8000 -u administrator -p password

# CSV 통합문서의 기준월로 설정
$P_yyyyMMdd = Get-Date (Get-Date).AddMonths(-1) -format "yyyyMMdd"
$P_yyyy = $P_yyyyMMdd.Substring(0,4)
$P_MM = $P_yyyyMMdd.Substring(4,2)

Get-Date -format "yyyyMMdd"

####################################### 파일경로
$Path = "C:\tab_portal\tab_resources\files\images\"

####################################### 파일명
$File = "BI_TOSBI015"

####################################### URL
$Url1 = Get-Content D:\Tableau_Sheet_ETL_Program\003_weekly_stock\url_list1.txt
#$Url1 = "80_03_/2?iframeSizedToWindow=true&:embed=y&:showAppBanner=false&:display_count=no&:showVizHome=no"
$Url2 = Get-Content D:\Tableau_Sheet_ETL_Program\003_weekly_stock\url_list2.txt

#$P_D_DATE = Get-Date -format "yyyy-MM-dd"
#$P_D_DATE = "2019-04-21"
$P_D_DATE = $LastSunday

$Url1 = $Url1.Replace("?", "?P_D_DATE=" + $P_D_DATE + "&")
$Url2 = $Url2.Replace("?", "?P_D_DATE=" + $P_D_DATE + "&")

#$i_MMDD = '4/28'
#$i_YYYY = '2019'

$i_MM = $P_D_DATE.Substring(5,2)
$i_MM = [int]$i_MM
$i_MM = [string]$i_MM

$i_DD = $P_D_DATE.Substring(8,2)
$i_DD = [int]$i_DD
$i_DD = [string]$i_DD

$i_MMDD = $i_MM + '/' + $i_DD
$i_YYYY = $P_D_DATE.Substring(0,4)

"********************************************************************************************* Extract Processing"
Get-Date -format "yyyy-MM-dd HH:mm:ss"

# png 파일 추출
# Get-Content 사용할 경우 url list 가 1개인 경우 #$Url.Item(0) 으로 url 이 저장되지 않음

For($i=0; $i -lt 1; $i++) {

    $yyyyMMdd = Get-Date -format "yyyyMMdd"
    $yyyy = $yyyyMMdd.Substring(0,4)
    $MM = $yyyyMMdd.Substring(4,2)
    $HHmmss = Get-Date -format "HHmmss"

    $seq = $i + 1

	# 명령어 생성 (파일경로 + 파일명)
	$expresion = $Path + $File + "_" + $yyyyMMdd + "_" + $seq + ".png"

    $pngFileName = $File + "_" + $yyyyMMdd + "_" + $seq + ".png"
    #$expresion

    
	tabcmd export $Url1 --png -f $expresion
}

# csv 파일 추출
#For($i=0; $i -lt $Url2.Length; $i++) {
For($i=0; $i -lt 1; $i++) {

    $yyyyMMdd = Get-Date -format "yyyyMMdd"
    $yyyy = $yyyyMMdd.Substring(0,4)
    $MM = $yyyyMMdd.Substring(4,2)
    $HHmmss = Get-Date -format "HHmmss"

    $seq = $i + 1

	# 명령어 생성 (파일경로 + 파일명)
	$expresion = $Path + $File + "_" + $yyyyMMdd + "_" + $seq + ".csv"

	#tabcmd export $Url2.Item($i) --csv -f $expresion
    tabcmd export $Url2 --csv -f $expresion
}


"********************************************************************************************* Extract Processing Complete"
Get-Date -format "yyyy-MM-dd HH:mm:ss"

#exit

# Path 폴더의 파일 리스트를 배열로. 당일 생성된 파일만 대상으로 함
$SearchFile = $File + "_" + $yyyyMMdd + "*.csv"
$FileNames = Get-ChildItem -Path $Path -Name $SearchFile -File

#$FileNames.Item(0)
#$FileNames
#$FileNames.Length

####################################### db 접속
$DBServer = "123.123.123.123"    #"(localdb)\v11.0"
$DBName = "database"
$uid = "userid"
$pwd = "password"
$sqlConnection = New-Object System.Data.SqlClient.SqlConnection
$sqlConnection.ConnectionString = "Server=$DBServer;Database=$DBName;Integrated Security=True;User ID = $uid; Password = $pwd;"     #"Server=$DBServer;Database=$DBName;Integrated Security=True;"
$sqlConnection.Open()
$cmd = New-Object System.Data.SqlClient.SqlCommand
$cmd.connection = $sqlConnection

####################################### 적재 테이블명
$table = 'TOSBI015'


"********************************************************************************************* Load Processing"
Get-Date -format "yyyy-MM-dd HH:mm:ss"
# 파일별로 sql 테이블 INSERT
$sql = ""

# key 조건에 따라 기존 데이터 삭제
$sql = $sql + "DELETE FROM " + $table + " WHERE COL2 LIKE '%" + $i_MMDD + "%'" + "AND COL2 LIKE '%" + $i_YYYY + "%';"


#For($i=0; $i -lt $FileNames.Length; $i++) {
For($i=0; $i -lt 1; $i++) {

    $CRT_USER_ID = 'ISP'
    $DATA_CRT_DTM = Get-Date -UFormat "%Y-%m-%d %T"

    #$csv_path = $Path + $FileNames.Item($i)
    $csv_path = $Path + $FileNames

    $csv = Get-Content $csv_path -Encoding UTF8

    ####################################### 헤더가 있는 csv 인 경우 $j = 1, 없는 경우 0 으로 설정
    For($j=1; $j -lt $csv.Length; $j++) {
        
        $csv_arr = $csv.Item($j).Split(',', 5)

        $col1 = $csv_arr.Item(0)
        $col2 = $csv_arr.Item(1)
        $col3 = $csv_arr.Item(2)
        $col4 = $csv_arr.Item(3)
        $col5 = $csv_arr.Item(4)
        $col5 = $col5.Replace(",", "")
        $col5 = $col5.Replace("""", "")

        $sql = $sql + "INSERT INTO "
        $sql = $sql +  $table
        $sql = $sql + " VALUES ('$col1', '$col2', '$col3', '$col4', '$col5', '$CRT_USER_ID', '$DATA_CRT_DTM');"
    }

}

#$sql

$cmd.commandtext = $sql
$cmd.executenonquery()
#$sqlConnection.Close()

"********************************************************************************************* Load Processing Complete"
Get-Date -format "yyyy-MM-dd HH:mm:ss"

#적재데이터 PIVOT 및 레벨별 정렬기준 적용
"********************************************************************************************* Data Preprocessing"
Get-Date -format "yyyy-MM-dd HH:mm:ss"
$sql = ""

# key 조건에 따라 기존 데이터 삭제
$sql = $sql + "DELETE FROM TOSBI017 WHERE COL2 LIKE '%" + $i_MMDD + "%'" + "AND COL2 LIKE '%" + $i_YYYY + "%';"

$sql = $sql + "INSERT INTO TOSBI017 
               EXEC BI_TOSBI015 " + '''' + $P_D_DATE + ''''

#$sql

#$sqlConnection.Open()
#$cmd = New-Object System.Data.SqlClient.SqlCommand
#$cmd.connection = $sqlConnection
$cmd.commandtext = $sql
$cmd.executenonquery()
#$sqlConnection.Close()
"********************************************************************************************* Data Preprocessing Complete"
Get-Date -format "yyyy-MM-dd HH:mm:ss"

"********************************************************************************************* Send Mail/Message Processing"
Get-Date -format "yyyy-MM-dd HH:mm:ss"
# ==== 1. 수신인 테이블 생성 시작 ====
#TOSBI018_TEST
#TOSBI018_TEST2
#TOSBI018_LIVE
#TOSBI018_LIVE_NOTES    메일만 받는 경우 (push_flag: N)

$query = ""
$query = $query + "SELECT * FROM TOSBI018_TEST2;"
#$query = $query + "SELECT * FROM TOSBI017;"

# TSQL statement to be executed
$cmd.CommandText = $query
$cmd.CommandTimeout = 0

$adapter = new-object system.data.sqlclient.sqldataadapter ($query, $sqlConnection)
$table = new-object System.Data.DataTable
$adapter.Fill($table) | out-null
#$table | Out-File "D:\output.txt"
# ==== 1. 수신인 테이블 생성 종료 ====

# ==== 2. 기준일 레이블 생성 시작 ====
$query = ""
$query = $query + "SELECT TOP 1 COL2 FROM TOSBI017 WHERE COL2 LIKE '%" + $i_MMDD + "%'" + "AND COL2 LIKE '%" + $i_YYYY + "%'"
#$query = $query + "SELECT * FROM TOSBI017;"

# TSQL statement to be executed
$cmd.CommandText = $query
$cmd.CommandTimeout = 0

$adapter = new-object system.data.sqlclient.sqldataadapter ($query, $sqlConnection)
$table2 = new-object System.Data.DataTable
$adapter.Fill($table2) | out-null
#$table | Out-File "D:\output.txt"

$contents1 = $table2.Rows[0][0]
# ==== 2. 기준일 레이블 생성 종료 ====


#수신인 Loop
$CrtSQL = ""
for($i=0;$i -lt $table.Rows.Count;$i++)
{ 
    <#
    for($j=0;$j -lt $table.Columns.Count-2;$j++)
    {
        $i, $j, $table.Rows[$i][$j]

        CreateSQL $YID, $YNAME, $LV, $LV_DETAIL
    }
    #>

    $YID       = $table.Rows[$i][0]    #YID
    $YNAME     = $table.Rows[$i][1]    #YNAME
    $LV        = $table.Rows[$i][2]    #LV
    $LV_DETAIL = $table.Rows[$i][3]    #LV_DETAIL

    $YNAME

    $inform_date = Get-Date -format "yyyy-MM-dd HH:mm:ss"

    $CrtSQL_1 = $(
            '
            INSERT INTO m_inform_temp_for_dw (
            inform_no, 
            inform_system_code, 
            inform_target_company, 
            inform_target_flag, 
            inform_sender_id, 
            inform_receiver_id, 
            inform_push_title, 
            inform_push_content, 
            inform_sms_title, 
            inform_sms_content, 
            inform_mail_title, 
            inform_mail_content, 
            inform_date, 
            push_flag, 
            mail_flag, 
            outside_mail_user, 
            outside_mail_address, 
            outside_sms_user, 
            outside_sms_number) 
            VALUES (
            ''3199401'',					--inform_no
            ''DW'',						--inform_system_code
            ''ALL'',						--inform_target_company
            ''P'',						--inform_target_flag
            ''시스템아이디'',				--inform_sender_id
            --''0100217339,0100210307'',				--inform_receiver_id
            '
        )

    $CrtSQL_2_1 = $(
            "
            '▶ [시스템 알림]',		--inform_push_title
            --inform_push_content
            '주간 현황<br><br>1. 기준일자: ") + $contents1 + $("<br><br>")

    switch ($LV)
    {
        "전사"   {$CrtSQL_2_2 = $('2. 대분류 내역 [단위: EA]')
        }

        "사업부" {$CrtSQL_2_2 = $('2. ') + $LV_DETAIL + $(' 중분류 내역 [단위: EA]')
        }

        "사업장" {$CrtSQL_2_2 = $('2. ') + $LV_DETAIL + $(' 소분류 내역 [단위: EA]')
        }
    }    

    $CrtSQL_2_3 = $("<br><br><table border = &apos;1&apos; style = &apos;border-collapse:collapse; border:1px black solid&apos; width=&apos;260&apos; height=&apos;100&apos; ><tbody><tr><td width=&apos;100&apos; height=&apos;20&apos; align =&apos;center&apos; bgcolor=&apos;eaeaea&apos;);>사업장</td><td width=&apos;90&apos; height=&apos;20&apos; align=&apos;center&apos; bgcolor=&apos;eaeaea&apos;);>품번수</td><td width=&apos;90&apos; height=&apos;20&apos; align =&apos;center&apos; bgcolor=&apos;eaeaea&apos;);>금액</td></tr>")

    # ==== 3. PUSH 컨텐츠 생성 시작 ====
    $query = ""

    switch ($LV)
    {
        "전사"    {$query = $query + "SELECT A.COL3, A.COL6, A.COL7 FROM TOSBI017 A LEFT OUTER JOIN TOSBI016 B ON A.COL3 = B.LV_DETAIL WHERE COL2 LIKE '%" + $i_MMDD + "%'" + "AND COL2 LIKE '%" + $i_YYYY + "%'" + " AND B.SEQ > 30 ORDER BY B.SEQ;"}
        "사업부"  {$query = $query + "SELECT A.COL3, A.COL6, A.COL7 FROM TOSBI017 A LEFT OUTER JOIN TOSBI016 B ON A.COL3 = B.LV_DETAIL WHERE COL2 LIKE '%" + $i_MMDD + "%'" + "AND COL2 LIKE '%" + $i_YYYY + "%'" + " AND COL1 = '" + $LV_DETAIL + "' AND B.SEQ > 30 ORDER BY B.SEQ;"}
        "사업장"  {$query = $query + "SELECT A.COL3, A.COL6, A.COL7 FROM TOSBI017 A LEFT OUTER JOIN TOSBI016 B ON A.COL3 = B.LV_DETAIL WHERE COL2 LIKE '%" + $i_MMDD + "%'" + "AND COL2 LIKE '%" + $i_YYYY + "%'" + " AND COL3 = '" + $LV_DETAIL + "' AND B.SEQ > 30 ORDER BY B.SEQ;"}
    } 

    # TSQL statement to be executed
    $cmd.CommandText = $query
    $cmd.CommandTimeout = 0

    $adapter = new-object system.data.sqlclient.sqldataadapter ($query, $sqlConnection)
    $table3 = new-object System.Data.DataTable
    $adapter.Fill($table3) | out-null
    #$table | Out-File "D:\output.txt"

    # ==== 3. PUSH 컨텐츠 생성 종료 ====

    #컨텐츠 Loop
    $CrtSQL_2_4 = ""
    for($j=0;$j -lt $table3.Rows.Count;$j++)
    {
        $COL3       = $table3.Rows[$j][0]
        $COL6       = $table3.Rows[$j][1]
        $COL7       = $table3.Rows[$j][2]

        #$COL3
        #$COL6
        #$COL7

        #$CrtSQL_2_4 = $CrtSQL_2_4 + $COL3 + ";" + + $COL6 + ";" + $COL7 + ";"

        

        $CrtSQL_2_4 = $CrtSQL_2_4 + $("<tr>" + "<td width=&apos;100&apos; height=&apos;20&apos; align =&apos;center&apos;);>" + $COL3 + "</td>" + "<td width=&apos;100&apos; height=&apos;20&apos; align =&apos;right&apos;);>"+ $COL6 + "</td>" + "<td width=&apos;90&apos; height=&apos;20&apos; align =&apos;right&apos;);>"+ $COL7 + "</td>" + "</tr>")
    }

    $CrtSQL_2_5 = $("</tbody></table><br>3. 시스템에서 상세내역 확인 후 조치바랍니다.'")

    $CrtSQL_2_6 = $(', ''▶ [시스템 알림]'',		--inform_sms_title')

    #SMS 컨텐츠
    $CrtSQL_2_7 = $('
            --inform_sms_content
            ''컨텐츠 준비중입니다.'','
        )

    $CrtSQL_2_8 = $(
            '
            --inform_mail_title
            ''▶ [시스템 알림] 주간 현황'','
        )
    
    #메일 컨텐츠
    $CrtSQL_2_9 = $(
            '
            --inform_mail_content
            ''
            <p><span style=&apos;color: rgb(9, 0, 255);&apos;>※ 본 메일은 자동정보수신처리시스템에서 자동발송 되는 메일 입니다.</span></p>
            <p><br style=&apos;&apos;><span style=&apos;&apos;>==============================================</span></p>
            <p><img src=&apos;http://company.co.kr:9000/image/viewimage?imageName=') + $pngFileName + $(
            '
            &apos; title=&apos;&apos; alt=&apos;&apos; border=&apos;0&apos; style=&apos;border: 0px solid; margin: 0px;&apos;><br></p><p><span style=&apos;&apos;>==============================================</span><br style=&apos;&apos;><br style=&apos;&apos;><br style=&apos;&apos;><br style=&apos;&apos;><span style=&apos;&apos;>아래 링크된 [ 시스템 ] 에서 상세내용을 확인해 주시기 바랍니다.</span><br style=&apos;&apos;><br style=&apos;&apos;><span style=&apos; color: blue;&apos;><a href=&apos;#&apos; onclick="window.open(&apos;http://company.co.kr:9000&apos;)">Click Here ==&gt; [ 시스템 ] 으로 이동</a></span></p>
            '',
            '
        )

    $CrtSQL_3 = $(
            '
            ''Y'',		--push_flag
            ''Y'',		--mail_flag
            ''-'',		--outside_mail_user
            ''-'',		--outside_mail_address
            ''-'',		--outside_sms_user
            ''-''
            );'
        )

    $CrtSQL = $CrtSQL + $CrtSQL_1 + '''' + $YID + ''',' + $CrtSQL_2_1 + $CrtSQL_2_2 + $CrtSQL_2_3 + $CrtSQL_2_4 + $CrtSQL_2_5 + $CrtSQL_2_6 + $CrtSQL_2_7 + $CrtSQL_2_8 + $CrtSQL_2_9 + '''' + $inform_date + ''',' + $CrtSQL_3

}

#$CrtSQL
#$CrtSQL_2_3

$conn = New-Object System.Data.Odbc.OdbcConnection
$conn.ConnectionString= "DSN=cubrid_dsn;"
$Cubrid_cmd = new-object System.Data.Odbc.OdbcCommand($CrtSQL,$conn)
$conn.open()
$Cubrid_cmd.ExecuteNonQuery()
$conn.close()

$sqlConnection.Close()
"********************************************************************************************* Send Mail/Message Processing Complete"
Get-Date -format "yyyy-MM-dd HH:mm:ss"