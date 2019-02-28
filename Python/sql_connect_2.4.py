import mysql.connector
import csv
import time
import os
import sys
from rpy2.robjects import r

class DBConnection:
    def __init__(self, DB_HOST, DB_USER, DB_PASSWORD, DB_NAME):
        self.host = DB_HOST
        self.database = DB_NAME
        self.user = DB_USER
        self.password = DB_PASSWORD
        self.conn = None

    def get_conn(self):
        if self.conn is None:
            self.conn = mysql.connector.connect(
                host=self.host,
                db=self.database,
                user=self.user,
                passwd=self.password)
        return self.conn

mydb = DBConnection('localhost', 'root', '0000', 'TEMP_TABLE').get_conn()
mycursor = mydb.cursor()

print("SET GLOBAL innodb_buffer_pool_size=64G")
mycursor.execute("SET GLOBAL innodb_buffer_pool_size=68719476736")

illinput = []
illinput2 =[]
while True:
    temp = input("실험군 질병 입력(끝:0, 종료:-1) : ")
    if temp == '0':
        break
    elif temp == '-1':
        sys.exit()
    else:
        illinput.append(temp)
while True:
    temp = input("대조군 질병 입력(끝:0, 종료:-1) : ")
    if temp == '0':
        break
    elif temp == '-1':
        sys.exit()
    else:
        illinput2.append(temp)
yearterm = int(input("데이터 년도 간격 (<15, 종료:-1)#일단은 2 : "))
while True:
    if yearterm < 15:
        break
    elif yearterm == '-1':
        sys.exit()
    else:
        print("데이터 년도 간격은 15미만이어야 합니다.")
        yearterm = int(input("데이터 년도 간격 (<15, 종료:-1)#일단은 2 : "))

yearterm2 = int(input("데이터 년도 간격 횟수 (연도간격({})*N<15, 종료:-1)#일단은 3 : ".format(yearterm)))

while True:
    if yearterm*yearterm2 < 15:
        break
    elif yearterm2 == '-1':
        sys.exit()
    else:
        print("간격*횟수는 15미만이어야 합니다.")
        yearterm2 = int(input("데이터 년도 간격* 데이터 년도 반복 횟수 (<15, 종료:-1)#일단은 3 : "))

illname = '_'.join(illinput)
illname2 = '_'.join(illinput2)

step_yn = input("Stepwise Selection을 하시겠습니까?(Y/N/나중에 결정:L)")

print("CREATE EXP:{0}_CTR:{1} FOLDER DIRECTORY".format(illname, illname2))
try:
    os.makedirs("./DATA/EXP:{0}_CTR:{1}".format(illname, illname2))
except FileExistsError:
    # directory already exists
    pass


print("CREATE EXP DATA ... START")

EXP_STEP1_like = []
for i in illinput:
    EXP_STEP1_like.append('T1.MAIN_SICK LIKE "{}%"'.format(i))

EXP_STEP1_where = """ OR
""".join(EXP_STEP1_like)

EXP_STEP1 = """CREATE TABLE T120_{0} AS
(
SELECT T1.*
FROM ILL.T120 AS T1
WHERE
{1}
)
""".format(illname, EXP_STEP1_where)


print("EXP_STEP1-1.CREATE T120_{} ... START".format(illname))

mycursor.execute(EXP_STEP1)
mydb.commit()
print("EXP_STEP1-1.CREATE T120_{} ... DONE".format(illname))

print("EXP_STEP1-2.CREATE INDEX person ON T120_{} ... START".format(illname))
mycursor.execute("CREATE INDEX person ON T120_{}(PERSON_ID)".format(illname))
mydb.commit()
print("EXP_STEP1-2.CREATE INDEX person ON T120_{} ... DONE".format(illname))

EXP_STEP2 = """CREATE TABLE T120_{0}_YEAR AS
(
    SELECT
   	 T1.PERSON_ID, T1.MIN_YEAR, GROUP_CONCAT(DISTINCT(MAIN_SICK) SEPARATOR '/') AS MAIN_SICKS
    FROM
   	 (
    	SELECT PERSON_ID, MIN(YEAR(RECU_FR_DT)) AS MIN_YEAR
   	 FROM T120_{0}
   	 GROUP BY PERSON_ID
   	 ) AS T1
    RIGHT JOIN T120_{0} AS T2
   	 ON T1.PERSON_ID = T2.PERSON_ID
    WHERE T1.MIN_YEAR = YEAR(T2.RECU_FR_DT)
	GROUP BY T1.PERSON_ID
)
""".format(illname)

print("EXP_STEP2-1.CREATE T120_{}_YEAR ... START".format(illname))
mycursor.execute(EXP_STEP2)
mydb.commit()
print("EXP_STEP2-1.CREATE T120_{}_YEAR ... DONE".format(illname))

print("EXP_STEP2-2.CREATE INDEX person ON T120_{}_YEAR ... START".format(illname))
mycursor.execute("CREATE INDEX person ON T120_{}_YEAR(PERSON_ID,MIN_YEAR)".format(illname))
mydb.commit()
print("EXP_STEP2-2.CREATE INDEX person ON T120_{}_YEAR ... DONE".format(illname))

EXP_STEP3 = """CREATE TABLE T120_{0}_YEAR_JK AS (
	SELECT
   	 T2.`PERSON_ID`,
   	 T2.`MIN_YEAR`,
   	 T2.`MAIN_SICKS`,
   	 T1.`SEX`,
   	 T1.`AGE_GROUP`,
   	 T1.`SIDO`,
   	 T1.`IPSN_TYPE_CD`,
   	 T1.`CTRB_PT_TYPE_CD`,
   	 T1.`DFAB_GRD_CD`,
   	 T1.`DFAB_PTN_CD`
	FROM ILL.JK_UNIF AS T1
	LEFT JOIN T120_{0}_YEAR AS T2
	ON T1.PERSON_ID = T2.PERSON_ID
	WHERE T1.STND_Y = T2.MIN_YEAR AND DTH_YM =""
)
""".format(illname)

print("EXP_STEP3-1.CREATE T120_{}_YEAR_JK ... START".format(illname))
mycursor.execute(EXP_STEP3)
mydb.commit()
print("EXP_STEP3-1.CREATE T120_{}_YEAR_JK ... DONE".format(illname))

print("EXP_STEP3-2.CREATE INDEX person ON T120_{}_YEAR_JK ... START".format(illname))
mycursor.execute("CREATE INDEX person ON T120_{}_YEAR_JK(PERSON_ID,MIN_YEAR)".format(illname))
mydb.commit()
print("EXP_STEP3-2.CREATE INDEX person ON T120_{}_YEAR_JK ... DONE".format(illname))

EXP_STEP4 = """CREATE TABLE EXP_{0}_{1}_{2}YEAR
(SELECT
	T1.`PERSON_ID`,
	T1.`MIN_YEAR` AS `YEAR`,
	T1.`MAIN_SICKS`,
	T1.`SEX`,
	T1.`AGE_GROUP`,
	T1.`SIDO`,
	T1.`IPSN_TYPE_CD`,
	T1.`CTRB_PT_TYPE_CD`,
	T1.`DFAB_GRD_CD`,
	T1.`DFAB_PTN_CD`,
	T2.`HCHK_APOP_PMH_YN`,
	T2.`HCHK_HDISE_PMH_YN`,
	T2.`HCHK_HPRTS_PMH_YN`,
	T2.`HCHK_DIABML_PMH_YN`,
	T2.`HCHK_HPLPDM_PMH_YN`,
	T2.`HCHK_PHSS_PMH_YN`,
	T2.`HCHK_ETCDSE_PMH_YN`,
	T2.`FMLY_APOP_PATIEN_YN`,
	T2.`FMLY_HDISE_PATIEN_YN`,
	T2.`FMLY_HPRTS_PATIEN_YN`,
	T2.`FMLY_DIABML_PATIEN_YN`,
	T2.`FMLY_CANCER_PATIEN_YN`
FROM T120_{0}_YEAR_JK AS T1
INNER JOIN ILL.GJ_UNIF AS T2
ON T1.PERSON_ID = T2.PERSON_ID
WHERE T1.MIN_YEAR = (T2.HCHK_YEAR + 0) AND
	(T2.`HCHK_APOP_PMH_YN`  	IS NOT NULL AND NOT T2.`HCHK_APOP_PMH_YN`  	="") AND
	(T2.`HCHK_HDISE_PMH_YN` 	IS NOT NULL AND NOT T2.`HCHK_HDISE_PMH_YN` 	="") AND
	(T2.`HCHK_HPRTS_PMH_YN` 	IS NOT NULL AND NOT T2.`HCHK_HPRTS_PMH_YN` 	="") AND
	(T2.`HCHK_DIABML_PMH_YN`	IS NOT NULL AND NOT T2.`HCHK_DIABML_PMH_YN`	="") AND
	(T2.`HCHK_HPLPDM_PMH_YN`	IS NOT NULL AND NOT T2.`HCHK_HPLPDM_PMH_YN`	="") AND
	(T2.`HCHK_PHSS_PMH_YN`  	IS NOT NULL AND NOT T2.`HCHK_PHSS_PMH_YN`  	="") AND
	(T2.`HCHK_ETCDSE_PMH_YN`	IS NOT NULL AND NOT T2.`HCHK_ETCDSE_PMH_YN`	="") AND
	(T2.`FMLY_APOP_PATIEN_YN`   IS NOT NULL AND NOT T2.`FMLY_APOP_PATIEN_YN`   ="") AND
	(T2.`FMLY_HDISE_PATIEN_YN`  IS NOT NULL AND NOT T2.`FMLY_HDISE_PATIEN_YN`  ="") AND
	(T2.`FMLY_HPRTS_PATIEN_YN`  IS NOT NULL AND NOT T2.`FMLY_HPRTS_PATIEN_YN`  ="") AND
	(T2.`FMLY_DIABML_PATIEN_YN` IS NOT NULL AND NOT T2.`FMLY_DIABML_PATIEN_YN` ="") AND
	(T2.`FMLY_CANCER_PATIEN_YN` IS NOT NULL AND NOT T2.`FMLY_CANCER_PATIEN_YN` ="")
)
""".format(illname, yearterm, yearterm * 0)
print("EXP_STEP4-1.CREATE EXP_{0}_{1}_{2}YEAR ... START".format(illname, yearterm, yearterm * 0))
mycursor.execute(EXP_STEP4)
mydb.commit()
print("EXP_STEP4-1.CREATE EXP_{0}_{1}_{2}YEAR ... DONE".format(illname, yearterm, yearterm * 0))

print("EXP_STEP4-2.CREATE INDEX person ON EXP_{0}_{1}_{2}YEAR ... START".format(illname, yearterm, yearterm * 0))
mycursor.execute("CREATE INDEX person ON EXP_{0}_{1}_{2}YEAR(PERSON_ID,YEAR)".format(illname, yearterm, yearterm * 0))
mydb.commit()
print("EXP_STEP4-2.CREATE INDEX person ON EXP_{0}_{1}_{2}YEAR ... DONE".format(illname, yearterm, yearterm * 0))

for count in range(yearterm2):
    print("COUNT:" + str(count))
    EXP_STEP5 = """CREATE TABLE EXP_{0}_{1}_{3}YEAR
    (SELECT
        T1.*,
        T2.`HCHK_YEAR` AS `{3}Y_HCHK_YEAR`,
        T2.`YKIHO_GUBUN_CD` AS `{3}Y_YKIHO_GUBUN_CD`,
        T2.`BMI` AS `{3}Y_BMI`,
        T2.`BP` AS `{3}Y_BP`,
        T2.`BLDS` AS `{3}Y_BLDS`,
        T2.`TOT_CHOLE` AS `{3}Y_TOT_CHOLE`,
        T2.`HMG` AS `{3}Y_HMG`,
        T2.`OLIG_PROTE_CD` AS `{3}Y_OLIG_PROTE_CD`,
        T2.`SGOT_AST` AS `{3}Y_SGOT_AST`,
        T2.`SGPT_ALT` AS `{3}Y_SGPT_ALT`,
        T2.`GAMMA_GTP` AS `{3}Y_GAMMA_GTP`,
        T2.`SMK_STAT_TYPE_RSPS_CD` AS `{3}Y_SMK_STAT_TYPE_RSPS_CD`,
        T2.`SMK_TERM_RSPS_CD` AS `{3}Y_SMK_TERM_RSPS_CD`,
        T2.`DSQTY_RSPS_CD` AS `{3}Y_DSQTY_RSPS_CD`,
        T2.`DRNK_HABIT_RSPS_CD` AS `{3}Y_DRNK_HABIT_RSPS_CD`,
        T2.`TM1_DRKQTY_RSPS_CD` AS `{3}Y_TM1_DRKQTY_RSPS_CD`,
        T2.`EXERCI_FREQ_RSPS_CD` AS `{3}Y_EXERCI_FREQ_RSPS_CD`
    FROM EXP_{0}_{1}_{2}YEAR AS T1
    INNER JOIN ILL.GJ_UNIF AS T2
    ON T1.PERSON_ID = T2.PERSON_ID
    WHERE T1.`YEAR` = (T2.HCHK_YEAR + {3})    AND
        (T2.`HCHK_YEAR`         	IS NOT NULL AND NOT T2.`HCHK_YEAR`             	="") AND
        (T2.`YKIHO_GUBUN_CD`     	IS NOT NULL AND NOT T2.`YKIHO_GUBUN_CD`     	="") AND
        (T2.`BMI`             	IS NOT NULL) AND
        (T2.`BP`             	IS NOT NULL) AND
        (T2.`BLDS`                 	IS NOT NULL AND NOT T2.`BLDS`                 	="") AND
        (T2.`TOT_CHOLE`         	IS NOT NULL AND NOT T2.`TOT_CHOLE`             	="") AND
        (T2.`HMG`                 	IS NOT NULL AND NOT T2.`HMG`                 	="") AND
        (T2.`OLIG_PROTE_CD`     	IS NOT NULL AND NOT T2.`OLIG_PROTE_CD`         	="") AND
        (T2.`SGOT_AST`             	IS NOT NULL AND NOT T2.`SGOT_AST`             	="") AND
        (T2.`SGPT_ALT`             	IS NOT NULL AND NOT T2.`SGPT_ALT`             	="") AND
        (T2.`GAMMA_GTP`         	IS NOT NULL AND NOT T2.`GAMMA_GTP`             	="") AND
        (T2.`SMK_STAT_TYPE_RSPS_CD`	IS NOT NULL AND NOT T2.`SMK_STAT_TYPE_RSPS_CD`	="") AND
        (T2.`SMK_TERM_RSPS_CD`     	IS NOT NULL AND NOT T2.`SMK_TERM_RSPS_CD`     	="") AND
        (T2.`DSQTY_RSPS_CD`     	IS NOT NULL AND NOT T2.`DSQTY_RSPS_CD`         	="") AND
        (T2.`DRNK_HABIT_RSPS_CD` 	IS NOT NULL AND NOT T2.`DRNK_HABIT_RSPS_CD` 	="") AND
        (T2.`TM1_DRKQTY_RSPS_CD` 	IS NOT NULL AND NOT T2.`TM1_DRKQTY_RSPS_CD` 	="") AND
        (T2.`EXERCI_FREQ_RSPS_CD` 	IS NOT NULL AND NOT T2.`EXERCI_FREQ_RSPS_CD` 	="")
    )
    """.format(illname, yearterm, yearterm * count, yearterm * (count + 1))
    print("EXP_STEP5-1.CREATE EXP_{0}_{1}_{3}YEAR ... START".format(illname, yearterm, yearterm * count,
                                                                    yearterm * (count + 1)))
    mycursor.execute(EXP_STEP5)
    mydb.commit()
    print("EXP_STEP5-1.CREATE EXP_{0}_{1}_{3}YEAR ... DONE".format(illname, yearterm, yearterm * count,
                                                                   yearterm * (count + 1)))
    print("EXP_STEP5-2.CREATE INDEX person ON EXP_{0}_{1}_{3}YEAR ... START".format(illname, yearterm, yearterm * count,
                                                                                    yearterm * (count + 1)))
    mycursor.execute(
        "CREATE INDEX person ON EXP_{0}_{1}_{3}YEAR(PERSON_ID,YEAR)".format(illname, yearterm, yearterm * count,
                                                                            yearterm * (count + 1)))
    mydb.commit()
    print("EXP_STEP5-2.CREATE INDEX person ON EXP_{0}_{1}_{3}YEAR ... DONE".format(illname, yearterm, yearterm * count,
                                                                                   yearterm * (count + 1)))

    print("EXP_STEP5-3.UPDATE [BMI,GAMMA_GTP] EXP_{0}_{1}_{3}YEAR ... START".format(illname, yearterm, yearterm * count,
                                                                                    yearterm * (count + 1)))
    EXP_STEP5_3_1 = """
    UPDATE EXP_{0}_{1}_{3}YEAR
    SET
    `{3}Y_HMG`=CLASS_HMG(SEX,`{3}Y_HMG`)
    WHERE
    (`{3}Y_HMG` IS NOT NULL AND NOT `{3}Y_HMG`="")
    """.format(illname, yearterm, yearterm * count, yearterm * (count + 1))
    EXP_STEP5_3_2 = """
    UPDATE EXP_{0}_{1}_{3}YEAR
    SET
    `{3}Y_GAMMA_GTP`=CLASS_GTP(SEX,`{3}Y_GAMMA_GTP`)
    WHERE
    (`{3}Y_GAMMA_GTP` IS NOT NULL AND NOT `{3}Y_GAMMA_GTP`="")
    """.format(illname, yearterm, yearterm * count, yearterm * (count + 1))
    mycursor.execute(EXP_STEP5_3_1)
    mydb.commit()
    mycursor.execute(EXP_STEP5_3_2)
    mydb.commit()
    print("EXP_STEP5-3.UPDATE [BMI,GAMMA_GTP] EXP_{0}_{1}_{3}YEAR ... END".format(illname, yearterm, yearterm * count,
                                                                                    yearterm * (count + 1)))

print("CREATE EXP DATA ... DONE")

print("CREATE CTR DATA ... START")

if not illname == illname2:
    CTR_STEP99_like = []
    for i in illinput2:
        CTR_STEP99_like.append('T1.SUB_SICK LIKE "{}%"'.format(i))

    CTR_STEP99_where = """ OR
    """.join(CTR_STEP99_like)

    CTR_STEP99 = """CREATE TABLE T120_{0} AS
    (
    SELECT T1.*
    FROM ILL.T120 AS T1
    WHERE
    {1}
    )
    """.format(illname2, CTR_STEP99_where)

    print("CTR_STEP0-1.CREATE T120_{} ... START".format(illname2))
    mycursor.execute(CTR_STEP99)
    mydb.commit()
    print("CTR_STEP99-1.CREATE T120_{} ... DONE".format(illname2))

    print("CTR_STEP99-2.CREATE INDEX person ON T120_{} ... START".format(illname2))
    mycursor.execute("CREATE INDEX person ON T120_{}(PERSON_ID)".format(illname2))
    mydb.commit()
    print("CTR_STEP99-2.CREATE INDEX person ON T120_{} ... DONE".format(illname2))


CTR_STEP0_sub_like = []
for i in illinput2:
    CTR_STEP0_sub_like.append('T1.SUB_SICK LIKE "{}%"'.format(i))

CTR_STEP0_sub_where = """ OR
""".join(CTR_STEP0_sub_like)

CTR_STEP0 = """CREATE TABLE T120_SUB_{0} AS
(
SELECT T1.*
FROM ILL.T120 AS T1
WHERE
{1}
)
""".format(illname2, CTR_STEP0_sub_where)

print("CTR_STEP0-1.CREATE T120_SUB_{} ... START".format(illname2))
mycursor.execute(CTR_STEP0)
mydb.commit()
print("CTR_STEP0-1.CREATE T120_SUB_{} ... DONE".format(illname2))

print("EXP_STEP0-2.CREATE INDEX person ON T120_SUB_{} ... START".format(illname2))
mycursor.execute("CREATE INDEX person ON T120_SUB_{}(PERSON_ID)".format(illname2))
mydb.commit()
print("EXP_STEP0-2.CREATE INDEX person ON T120_SUB_{} ... DONE".format(illname2))

CTR_STEP1 = """CREATE TABLE T120_NOT_{0}_NOT_SUB_{0} AS
(
	SELECT *
	FROM ILL.T120 AS T1
	WHERE
    	(
        	T1.PERSON_ID NOT IN(
            	SELECT PERSON_ID
            	FROM T120_{0})
    	)
    	AND
    	(
        	T1.PERSON_ID NOT IN(
            	SELECT PERSON_ID
            	FROM T120_SUB_{0})
    	)
)

""".format(illname2)

print("CTR_STEP1-1.CREATE T120_NOT_{0}_NOT_SUB_{0} ... START".format(illname2))
mycursor.execute(CTR_STEP1)
mydb.commit()
print("CTR_STEP1-1.CREATE T120_NOT_{0}_NOT_SUB_{0} ... DONE".format(illname2))

print("CTR_STEP1-2.CREATE INDEX person ON T120_NOT_{0}_NOT_SUB_{0} ... START".format(illname2))
mycursor.execute("CREATE INDEX person ON T120_NOT_{0}_NOT_SUB_{0}(PERSON_ID)".format(illname2))
mydb.commit()
print("CTR_STEP1-2.CREATE INDEX person ON T120_NOT_{0}_NOT_SUB_{0} ... DONE".format(illname2))

CTR_STEP2 = """CREATE TABLE T120_NOT_{0}_NOT_SUB_{0}_YEAR AS
(
    SELECT
   	 T1.PERSON_ID, T1.MAX_YEAR, GROUP_CONCAT(DISTINCT(MAIN_SICK) SEPARATOR '/') AS MAIN_SICKS
    FROM
   	 (
    	SELECT PERSON_ID, MAX(YEAR(RECU_FR_DT)) AS MAX_YEAR
   	 FROM T120_NOT_{0}_NOT_SUB_{0}
   	 GROUP BY PERSON_ID
   	 ) AS T1
    RIGHT JOIN T120_NOT_{0}_NOT_SUB_{0} AS T2
   	 ON T1.PERSON_ID = T2.PERSON_ID
    WHERE T1.MAX_YEAR = YEAR(T2.RECU_FR_DT) AND T1.MAX_YEAR = 2013
	GROUP BY T1.PERSON_ID
)
""".format(illname2)

print("CTR_STEP2-1.CREATE T120_NOT_{0}_NOT_SUB_{0}_YEAR ... START".format(illname2))
mycursor.execute(CTR_STEP2)
mydb.commit()
print("CTR_STEP2-1.CREATE T120_NOT_{0}_NOT_SUB_{0}_YEAR ... DONE".format(illname2))

print("CTR_STEP2-2.CREATE INDEX person ON T120_NOT_{0}_NOT_SUB_{0}_YEAR ... START".format(illname2))
mycursor.execute("CREATE INDEX person ON T120_NOT_{0}_NOT_SUB_{0}_YEAR(PERSON_ID,MAX_YEAR)".format(illname2))
mydb.commit()
print("CTR_STEP2-2.CREATE INDEX person ON T120_NOT_{0}_NOT_SUB_{0}_YEAR ... DONE".format(illname2))

CTR_STEP3 = """CREATE TABLE T120_NOT_{0}_NOT_SUB_{0}_YEAR_JK AS (
	SELECT
   	 T2.`PERSON_ID`,
   	 T2.`MAX_YEAR`,
   	 T2.`MAIN_SICKS`,
   	 T1.`SEX`,
   	 T1.`AGE_GROUP`,
   	 T1.`SIDO`,
   	 T1.`IPSN_TYPE_CD`,
   	 T1.`CTRB_PT_TYPE_CD`,
   	 T1.`DFAB_GRD_CD`,
   	 T1.`DFAB_PTN_CD`
	FROM ILL.JK_UNIF AS T1
	LEFT JOIN T120_NOT_{0}_NOT_SUB_{0}_YEAR AS T2
	ON T1.PERSON_ID = T2.PERSON_ID
	WHERE T1.STND_Y = T2.MAX_YEAR AND DTH_YM =""
)
""".format(illname2)

print("CTR_STEP3-1.CREATE T120_NOT_{0}_NOT_SUB_{0}_YEAR_JK ... START".format(illname2))
mycursor.execute(CTR_STEP3)
mydb.commit()
print("CTR_STEP3-1.CREATE T120_NOT_{0}_NOT_SUB_{0}_YEAR_JK ... DONE".format(illname2))

print("CTR_STEP3-2.CREATE INDEX person ON T120_NOT_{0}_NOT_SUB_{0}_YEAR_JK ... START".format(illname2))
mycursor.execute("CREATE INDEX person ON T120_NOT_{0}_NOT_SUB_{0}_YEAR_JK(PERSON_ID,MAX_YEAR)".format(illname2))
mydb.commit()
print("CTR_STEP3-2.CREATE INDEX person ON T120_NOT_{0}_NOT_SUB_{0}_YEAR_JK ... DONE".format(illname2))

CTR_STEP4 = """CREATE TABLE CTR_{0}_{1}_{2}YEAR
(SELECT
	T1.`PERSON_ID`,
	T1.`MAX_YEAR` AS `YEAR`,
	T1.`MAIN_SICKS`,
	T1.`SEX`,
	T1.`AGE_GROUP`,
	T1.`SIDO`,
	T1.`IPSN_TYPE_CD`,
	T1.`CTRB_PT_TYPE_CD`,
	T1.`DFAB_GRD_CD`,
	T1.`DFAB_PTN_CD`,
	T2.`HCHK_APOP_PMH_YN`,
	T2.`HCHK_HDISE_PMH_YN`,
	T2.`HCHK_HPRTS_PMH_YN`,
	T2.`HCHK_DIABML_PMH_YN`,
	T2.`HCHK_HPLPDM_PMH_YN`,
	T2.`HCHK_PHSS_PMH_YN`,
	T2.`HCHK_ETCDSE_PMH_YN`,
	T2.`FMLY_APOP_PATIEN_YN`,
	T2.`FMLY_HDISE_PATIEN_YN`,
	T2.`FMLY_HPRTS_PATIEN_YN`,
	T2.`FMLY_DIABML_PATIEN_YN`,
	T2.`FMLY_CANCER_PATIEN_YN`
FROM T120_NOT_{0}_NOT_SUB_{0}_YEAR_JK AS T1
INNER JOIN ILL.GJ_UNIF AS T2
ON T1.PERSON_ID = T2.PERSON_ID
WHERE T1.MAX_YEAR = (T2.HCHK_YEAR + 0) AND
	(T2.`HCHK_APOP_PMH_YN`  	IS NOT NULL AND NOT T2.`HCHK_APOP_PMH_YN`  	="") AND
	(T2.`HCHK_HDISE_PMH_YN` 	IS NOT NULL AND NOT T2.`HCHK_HDISE_PMH_YN` 	="") AND
	(T2.`HCHK_HPRTS_PMH_YN` 	IS NOT NULL AND NOT T2.`HCHK_HPRTS_PMH_YN` 	="") AND
	(T2.`HCHK_DIABML_PMH_YN`	IS NOT NULL AND NOT T2.`HCHK_DIABML_PMH_YN`	="") AND
	(T2.`HCHK_HPLPDM_PMH_YN`	IS NOT NULL AND NOT T2.`HCHK_HPLPDM_PMH_YN`	="") AND
	(T2.`HCHK_PHSS_PMH_YN`  	IS NOT NULL AND NOT T2.`HCHK_PHSS_PMH_YN`  	="") AND
	(T2.`HCHK_ETCDSE_PMH_YN`	IS NOT NULL AND NOT T2.`HCHK_ETCDSE_PMH_YN`	="") AND
	(T2.`FMLY_APOP_PATIEN_YN`   IS NOT NULL AND NOT T2.`FMLY_APOP_PATIEN_YN`   ="") AND
	(T2.`FMLY_HDISE_PATIEN_YN`  IS NOT NULL AND NOT T2.`FMLY_HDISE_PATIEN_YN`  ="") AND
	(T2.`FMLY_HPRTS_PATIEN_YN`  IS NOT NULL AND NOT T2.`FMLY_HPRTS_PATIEN_YN`  ="") AND
	(T2.`FMLY_DIABML_PATIEN_YN` IS NOT NULL AND NOT T2.`FMLY_DIABML_PATIEN_YN` ="") AND
	(T2.`FMLY_CANCER_PATIEN_YN` IS NOT NULL AND NOT T2.`FMLY_CANCER_PATIEN_YN` ="")
)
""".format(illname2, yearterm, yearterm * 0)
print("CTR_STEP4-1.CREATE CTR_{0}_{1}_{2}YEAR ... START".format(illname2, yearterm, yearterm * 0))
mycursor.execute(CTR_STEP4)
mydb.commit()
print("CTR_STEP4-1.CREATE CTR_{0}_{1}_{2}YEAR ... DONE".format(illname2, yearterm, yearterm * 0))

print("CTR_STEP4-2.CREATE INDEX person ON CTR_{0}_{1}_{2}YEAR ... START".format(illname2, yearterm, yearterm * 0))
mycursor.execute("CREATE INDEX person ON CTR_{0}_{1}_{2}YEAR(PERSON_ID,YEAR)".format(illname2, yearterm, yearterm * 0))
mydb.commit()
print("CTR_STEP4-2.CREATE INDEX person ON CTR_{0}_{1}_{2}YEAR ... DONE".format(illname2, yearterm, yearterm * 0))

for count in range(yearterm2):
    print("COUNT:" + str(count))
    CTR_STEP5 = """CREATE TABLE CTR_{0}_{1}_{3}YEAR
    (SELECT
        T1.*,
        T2.`HCHK_YEAR` AS `{3}Y_HCHK_YEAR`,
        T2.`YKIHO_GUBUN_CD` AS `{3}Y_YKIHO_GUBUN_CD`,
        T2.`BMI` AS `{3}Y_BMI`,
        T2.`BP` AS `{3}Y_BP`,
        T2.`BLDS` AS `{3}Y_BLDS`,
        T2.`TOT_CHOLE` AS `{3}Y_TOT_CHOLE`,
        T2.`HMG` AS `{3}Y_HMG`,
        T2.`OLIG_PROTE_CD` AS `{3}Y_OLIG_PROTE_CD`,
        T2.`SGOT_AST` AS `{3}Y_SGOT_AST`,
        T2.`SGPT_ALT` AS `{3}Y_SGPT_ALT`,
        T2.`GAMMA_GTP` AS `{3}Y_GAMMA_GTP`,
        T2.`SMK_STAT_TYPE_RSPS_CD` AS `{3}Y_SMK_STAT_TYPE_RSPS_CD`,
        T2.`SMK_TERM_RSPS_CD` AS `{3}Y_SMK_TERM_RSPS_CD`,
        T2.`DSQTY_RSPS_CD` AS `{3}Y_DSQTY_RSPS_CD`,
        T2.`DRNK_HABIT_RSPS_CD` AS `{3}Y_DRNK_HABIT_RSPS_CD`,
        T2.`TM1_DRKQTY_RSPS_CD` AS `{3}Y_TM1_DRKQTY_RSPS_CD`,
        T2.`EXERCI_FREQ_RSPS_CD` AS `{3}Y_EXERCI_FREQ_RSPS_CD`
    FROM CTR_{0}_{1}_{2}YEAR AS T1
    INNER JOIN ILL.GJ_UNIF AS T2
    ON T1.PERSON_ID = T2.PERSON_ID
    WHERE T1.`YEAR` = (T2.HCHK_YEAR + {3})    AND
        (T2.`HCHK_YEAR`         	IS NOT NULL AND NOT T2.`HCHK_YEAR`             	="") AND
        (T2.`YKIHO_GUBUN_CD`     	IS NOT NULL AND NOT T2.`YKIHO_GUBUN_CD`     	="") AND
        (T2.`BMI`             	IS NOT NULL) AND
        (T2.`BP`             	IS NOT NULL) AND
        (T2.`BLDS`                 	IS NOT NULL AND NOT T2.`BLDS`                 	="") AND
        (T2.`TOT_CHOLE`         	IS NOT NULL AND NOT T2.`TOT_CHOLE`             	="") AND
        (T2.`HMG`                 	IS NOT NULL AND NOT T2.`HMG`                 	="") AND
        (T2.`OLIG_PROTE_CD`     	IS NOT NULL AND NOT T2.`OLIG_PROTE_CD`         	="") AND
        (T2.`SGOT_AST`             	IS NOT NULL AND NOT T2.`SGOT_AST`             	="") AND
        (T2.`SGPT_ALT`             	IS NOT NULL AND NOT T2.`SGPT_ALT`             	="") AND
        (T2.`GAMMA_GTP`         	IS NOT NULL AND NOT T2.`GAMMA_GTP`             	="") AND
        (T2.`SMK_STAT_TYPE_RSPS_CD`	IS NOT NULL AND NOT T2.`SMK_STAT_TYPE_RSPS_CD`	="") AND
        (T2.`SMK_TERM_RSPS_CD`     	IS NOT NULL AND NOT T2.`SMK_TERM_RSPS_CD`     	="") AND
        (T2.`DSQTY_RSPS_CD`     	IS NOT NULL AND NOT T2.`DSQTY_RSPS_CD`         	="") AND
        (T2.`DRNK_HABIT_RSPS_CD` 	IS NOT NULL AND NOT T2.`DRNK_HABIT_RSPS_CD` 	="") AND
        (T2.`TM1_DRKQTY_RSPS_CD` 	IS NOT NULL AND NOT T2.`TM1_DRKQTY_RSPS_CD` 	="") AND
        (T2.`EXERCI_FREQ_RSPS_CD` 	IS NOT NULL AND NOT T2.`EXERCI_FREQ_RSPS_CD` 	="")
    )
    """.format(illname2, yearterm, yearterm * count, yearterm * (count + 1))
    print("CTR_STEP5-1.CREATE CTR_{0}_{1}_{3}YEAR ... START".format(illname2, yearterm, yearterm * count,
                                                                    yearterm * (count + 1)))
    mycursor.execute(CTR_STEP5)
    mydb.commit()
    print("CTR_STEP5-1.CREATE CTR_{0}_{1}_{3}YEAR ... DONE".format(illname2, yearterm, yearterm * count,
                                                                   yearterm * (count + 1)))

    print("CTR_STEP5-2.CREATE INDEX person ON CTR_{0}_{1}_{3}YEAR ... START".format(illname2, yearterm, yearterm * count,
                                                                                    yearterm * (count + 1)))
    mycursor.execute(
        "CREATE INDEX person ON CTR_{0}_{1}_{3}YEAR(PERSON_ID,YEAR)".format(illname2, yearterm, yearterm * count,
                                                                            yearterm * (count + 1)))
    mydb.commit()
    print("CTR_STEP5-2.CREATE INDEX person ON CTR_{0}_{1}_{3}YEAR ... DONE".format(illname2, yearterm, yearterm * count,
                                                                                   yearterm * (count + 1)))

    print("CTR_STEP5-3.UPDATE [BMI,GAMMA_GTP] CTR_{0}_{1}_{3}YEAR ... START".format(illname2, yearterm, yearterm * count,
                                                                                    yearterm * (count + 1)))
    CTR_STEP5_3_1 = """UPDATE CTR_{0}_{1}_{3}YEAR
    SET
    `{3}Y_HMG`=CLASS_HMG(SEX,`{3}Y_HMG`)
    WHERE
    (`{3}Y_HMG` IS NOT NULL AND NOT `{3}Y_HMG`="")
    """.format(illname2, yearterm, yearterm * count, yearterm * (count + 1))

    CTR_STEP5_3_2 = """
    UPDATE CTR_{0}_{1}_{3}YEAR
    SET
    `{3}Y_GAMMA_GTP`=CLASS_GTP(SEX,`{3}Y_GAMMA_GTP`)
    WHERE
    (`{3}Y_GAMMA_GTP` IS NOT NULL AND NOT `{3}Y_GAMMA_GTP`="")
    """.format(illname2, yearterm, yearterm * count, yearterm * (count + 1))
    mycursor.execute(CTR_STEP5_3_1)
    mydb.commit()
    mycursor.execute(CTR_STEP5_3_2)
    mydb.commit()
    print("CTR_STEP5-3.UPDATE [BMI,GAMMA_GTP] CTR_{0}_{1}_{3}YEAR ... END".format(illname2, yearterm, yearterm * count,
                                                                                    yearterm * (count + 1)))

print("CREATE CTR DATA ... DONE")

print("EXP_{0}_{1}_{3}YEAR Export ... START".format(illname, yearterm, yearterm * (yearterm2-1),
                                                                                   yearterm * (yearterm2)))
mycursor.execute("SELECT * FROM EXP_{0}_{1}_{3}YEAR".format(illname, yearterm, yearterm * (yearterm2-1),
                                                                                   yearterm * (yearterm2)))
result = mycursor.fetchall()
columns = [i[0] for i in mycursor.description]
fp = open("./DATA/EXP:{0}_CTR:{4}/EXP_{0}_{1}_{3}YEAR.csv".format(illname, yearterm, yearterm * (yearterm2-1),
                                                                                   yearterm * (yearterm2), illname2), 'w')
myFile = csv.writer(fp, lineterminator='\n')
myFile.writerow(columns)
myFile.writerows(result)
fp.close()
print("EXP_{0}_{1}_{3}YEAR Export ... DONE".format(illname, yearterm, yearterm * (yearterm2-1),
                                                                                   yearterm * (yearterm2)))


print("CTR_{0}_{1}_{3}YEAR Export ... START".format(illname2, yearterm, yearterm * (yearterm2-1),
                                                                                   yearterm * (yearterm2)))
mycursor.execute("SELECT * FROM CTR_{0}_{1}_{3}YEAR".format(illname2, yearterm, yearterm * (yearterm2-1),
                                                                                   yearterm * (yearterm2)))
result = mycursor.fetchall()
columns = [i[0] for i in mycursor.description]
fp = open("./DATA/EXP:{0}_CTR:{4}/CTR_{4}_{1}_{3}YEAR.csv".format(illname, yearterm, yearterm * (yearterm2-1),
                                                                                   yearterm * (yearterm2), illname2), 'w')
myFile = csv.writer(fp, lineterminator='\n')
myFile.writerow(columns)
myFile.writerows(result)
fp.close()
print("CTR_{0}_{1}_{3}YEAR Export ... DONE".format(illname2, yearterm, yearterm * (yearterm2-1),
                                                                                   yearterm * (yearterm2)))


print("CREATE REPORT ... START")
fp = open("./DATA/EXP:{0}_CTR:{1}/Report_EXP:{0}_CTR:{1}.txt".format(illname,illname2), 'a')
fp.write("Number of elements\n")
for count in range(yearterm2):
    mycursor.execute("SELECT COUNT(*) FROM EXP_{0}_{1}_{3}YEAR".format(illname, yearterm, yearterm * (count),
                                                                                   yearterm * (count + 1)))
    table_count = mycursor.fetchall()[0][0]
    table_write = "EXP_{0}_{1}_{3}YEAR : {4}\n".format(illname, yearterm, yearterm * (count),
                                                                                   yearterm * (count + 1), table_count)
    print(table_write)
    fp.write(table_write)

fp.write("\n")
for count in range(yearterm2):
    mycursor.execute("SELECT COUNT(*) FROM CTR_{0}_{1}_{3}YEAR".format(illname2, yearterm, yearterm * (count),
                                                                                   yearterm * (count + 1)))
    table_count = mycursor.fetchall()[0][0]
    table_write = "CTR_{0}_{1}_{3}YEAR : {4}\n".format(illname2, yearterm, yearterm * (count),
                                                                                   yearterm * (count + 1), table_count)
    print(table_write)
    fp.write(table_write)

fp.close()
print("CREATE REPORT ... DONE")

print("Rpy start")
r('getwd()')
r('setwd("./DATA/EXP:{0}_CTR:{1}")'.format(illname,illname2))
r('options(scipen=100, digit = 11)')
r('csv_exp_6Y <- read.csv("EXP_{0}_{1}_{3}YEAR.csv",header=TRUE,sep=",")'.format(illname, yearterm, yearterm * (yearterm2-1),
                                                                                   yearterm * (yearterm2)))
r('csv_exp_6Y$MAIN_SICKS <- 1')
r('csv_ctr_6Y <- read.csv("CTR_{0}_{1}_{3}YEAR.csv",header=TRUE,sep=",")'.format(illname2, yearterm, yearterm * (yearterm2-1),
                                                                                   yearterm * (yearterm2)))
r('csv_ctr_6Y$MAIN_SICKS <- 0')
# 실험군 대조군 N수 반영
r('samp <- sample(1:nrow(csv_ctr_6Y),nrow(csv_exp_6Y), replace=FALSE,prob = NULL)')
r('csv_6Y<- rbind(csv_exp_6Y,csv_ctr_6Y[samp,])')
r('write.csv(csv_6Y,"INTEGRATED_{0}_{1}_{3}YEAR.csv",row.names=FALSE)'.format(illname, yearterm, yearterm * (yearterm2-1),
                                                                                   yearterm * (yearterm2)))
# 그룹핑 과정
r('csv_6Y$AGE_GROUP[csv_6Y$AGE_GROUP==1]<-2')
r('csv_6Y$AGE_GROUP[csv_6Y$AGE_GROUP==7]<-6')
r('csv_6Y<-csv_6Y[csv_6Y$SIDO<4,]')
r('csv_6Y<-csv_6Y[csv_6Y$IPSN_TYPE_CD!=3,]')
r('csv_6Y<-csv_6Y[csv_6Y$CTRB_PT_TYPE_CD!=0,]')
r('csv_6Y$DFAB_GRD_CD[csv_6Y$DFAB_GRD_CD==2]<-1')

r('write.csv(csv_6Y,"INTEGRATED_{0}_{1}_{3}YEAR_DELETED.csv",row.names=FALSE)'.format(illname, yearterm, yearterm * (yearterm2-1),
                                                                                   yearterm * (yearterm2)))
r('write.csv(csv_6Y[csv_6Y$MAIN_SICKS==1,],"EXP_{0}_{1}_{3}YEAR_DELETED.csv",row.names=FALSE)'.format(illname, yearterm, yearterm * (yearterm2-1),
                                                                                   yearterm * (yearterm2)))
r('write.csv(csv_6Y[csv_6Y$MAIN_SICKS==0,],"CTR_{0}_{1}_{3}YEAR_DELETED.csv",row.names=FALSE)'.format(illname2, yearterm, yearterm * (yearterm2-1),
                                                                                   yearterm * (yearterm2)))
print("CSV CREATED")
#wntjrw주 석주석주석주석주석
r("""level_column<-list(c(0,2)
                   ,c(0,2)
                   ,c(0,1)
                   ,c(1,2) #SEX
                   ,c(2,3,4,5,6)
                   ,c(1,2,3)
                   ,c(1,2)
                   ,c(1,2,3,4)
                   ,c(0,1)
                   ,c(0,1,2,3,4,5,6,7,8)
                   ,c(0,1)
                   ,c(0,1)
                   ,c(0,1)
                   ,c(0,1)
                   ,c(0,1)
                   ,c(0,1)
                   ,c(0,1)
                   ,c(0,1)
                   ,c(0,1)
                   ,c(0,1)
                   ,c(0,1)
                   ,c(0,1)
                   ,c(0,2) #2YEAR
                   ,c(1,2,3,5,7,9)
                   ,c(0,1,2,9)
                   ,c(0,1,2,9)
                   ,c(0,1,2)
                   ,c(0,1,2)
                   ,c(0,1,2,9)
                   ,c(0,1,2)
                   ,c(0,1,2)
                   ,c(0,1,2)
                   ,c(0,1,2,9)
                   ,c(0,1,2)
                   ,c(0,1,2,3,4,5)
                   ,c(0,1,2,3,4)
                   ,c(0,1,2,3,4)
                   ,c(0,1,2,3,4)
                   ,c(0,1,2,3,4)
                   ,c(0,2) #4YEAR
                   ,c(1,2,3,5,7,9)
                   ,c(0,1,2,9)
                   ,c(0,1,2,9)
                   ,c(0,1,2)
                   ,c(0,1,2)
                   ,c(0,1,2,9)
                   ,c(0,1,2)
                   ,c(0,1,2)
                   ,c(0,1,2)
                   ,c(0,1,2,9)
                   ,c(0,1,2)
                   ,c(0,1,2,3,4,5)
                   ,c(0,1,2,3,4)
                   ,c(0,1,2,3,4)
                   ,c(0,1,2,3,4)
                   ,c(0,1,2,3,4)
                   ,c(0,2) #6YEAR
                   ,c(1,2,3,5,7,9)
                   ,c(0,1,2,9)
                   ,c(0,1,2,9)
                   ,c(0,1,2)
                   ,c(0,1,2)
                   ,c(0,1,2,9)
                   ,c(0,1,2)
                   ,c(0,1,2)
                   ,c(0,1,2)
                   ,c(0,1,2,9)
                   ,c(0,1,2)
                   ,c(0,1,2,3,4,5)
                   ,c(0,1,2,3,4)
                   ,c(0,1,2,3,4)
                   ,c(0,1,2,3,4)
                   ,c(0,1,2,3,4)
)""")

r('getwd()')
r('library(descr)')

r('exp_6Y_deleted<-csv_6Y[csv_6Y$MAIN_SICKS==1,]')
r('ctr_6Y_deleted<-csv_6Y[csv_6Y$MAIN_SICKS==0,]')
r('csv_6Y')


r('freq_i<-c(0,0,0)')
r('freq_e<-c(0,0,0)')
r('freq_c<-c(0,0,0)')


r("""for (i in c(1:ncol(csv_6Y))[-c(1,2,3,23,40,57)]){

  freq_i<-rbind(freq_i,freq(factor(csv_6Y[,i],levels = unlist(level_column[i])))[-nrow(freq(factor(exp_6Y_deleted[,i],levels = unlist(level_column[i])))),],c("","",""))
}""")

r("""for (i in c(1:ncol(exp_6Y_deleted))[-c(1,2,3,23,40,57)]){

  freq_e<-rbind(freq_e,freq(factor(exp_6Y_deleted[,i],levels = unlist(level_column[i])))[-nrow(freq(factor(exp_6Y_deleted[,i],levels = unlist(level_column[i])))),],c("","",""))
}""")

r("""for (i in c(1:ncol(ctr_6Y_deleted))[-c(1,2,3,23,40,57)]){

  freq_c<-rbind(freq_c,freq(factor(ctr_6Y_deleted[,i],levels = unlist(level_column[i])))[-nrow(freq(factor(exp_6Y_deleted[,i],levels = unlist(level_column[i])))),],c("","",""))
}""")

r('freq_total<-cbind(freq_e,freq_c,freq_i)')

r('write.csv(freq_total[-1,],"Frequency_EXP:{0}_CTR:{1}.csv",row.names = TRUE)'.format(illname, illname2))

print("Frequency CREATED")


# 범주화 과정
r('csv_6Y$SEX                          <- as.factor(csv_6Y$SEX                       )')
r('csv_6Y$AGE_GROUP                    <- as.factor(csv_6Y$AGE_GROUP                 )')
r('csv_6Y$AGE_GROUP             <-relevel(csv_6Y$AGE_GROUP, ref="2")')
r('csv_6Y$SIDO                         <- as.factor(csv_6Y$SIDO                      )')
r('csv_6Y$IPSN_TYPE_CD                 <- as.factor(csv_6Y$IPSN_TYPE_CD              )')
r('csv_6Y$IPSN_TYPE_CD             <-relevel(csv_6Y$IPSN_TYPE_CD, ref="2")')
r('csv_6Y$CTRB_PT_TYPE_CD              <- as.factor(csv_6Y$CTRB_PT_TYPE_CD           )')
r('csv_6Y$CTRB_PT_TYPE_CD          <-relevel(csv_6Y$CTRB_PT_TYPE_CD, ref="4")')
r('csv_6Y$DFAB_GRD_CD                  <- as.factor(csv_6Y$DFAB_GRD_CD               )')
r('csv_6Y$DFAB_PTN_CD                  <- as.factor(csv_6Y$DFAB_PTN_CD               )')
r('csv_6Y$HCHK_APOP_PMH_YN             <- as.factor(csv_6Y$HCHK_APOP_PMH_YN          )')
r('csv_6Y$HCHK_HDISE_PMH_YN            <- as.factor(csv_6Y$HCHK_HDISE_PMH_YN         )')
r('csv_6Y$HCHK_HPRTS_PMH_YN            <- as.factor(csv_6Y$HCHK_HPRTS_PMH_YN         )')
r('csv_6Y$HCHK_DIABML_PMH_YN           <- as.factor(csv_6Y$HCHK_DIABML_PMH_YN        )')
r('csv_6Y$HCHK_HPLPDM_PMH_YN           <- as.factor(csv_6Y$HCHK_HPLPDM_PMH_YN        )')
r('csv_6Y$HCHK_PHSS_PMH_YN             <- as.factor(csv_6Y$HCHK_PHSS_PMH_YN          )')
r('csv_6Y$HCHK_ETCDSE_PMH_YN           <- as.factor(csv_6Y$HCHK_ETCDSE_PMH_YN        )')
r('csv_6Y$FMLY_APOP_PATIEN_YN          <- as.factor(csv_6Y$FMLY_APOP_PATIEN_YN       )')
r('csv_6Y$FMLY_HDISE_PATIEN_YN         <- as.factor(csv_6Y$FMLY_HDISE_PATIEN_YN      )')
r('csv_6Y$FMLY_HPRTS_PATIEN_YN         <- as.factor(csv_6Y$FMLY_HPRTS_PATIEN_YN      )')
r('csv_6Y$FMLY_DIABML_PATIEN_YN        <- as.factor(csv_6Y$FMLY_DIABML_PATIEN_YN     )')
r('csv_6Y$FMLY_CANCER_PATIEN_YN        <- as.factor(csv_6Y$FMLY_CANCER_PATIEN_YN     )')
r('csv_6Y$X2Y_YKIHO_GUBUN_CD           <- as.factor(csv_6Y$X2Y_YKIHO_GUBUN_CD        )')
r('csv_6Y$X2Y_YKIHO_GUBUN_CD        <-relevel(csv_6Y$X2Y_YKIHO_GUBUN_CD, ref="3")')
r('csv_6Y$X2Y_BMI                      <- as.factor(csv_6Y$X2Y_BMI                   )')
r('csv_6Y$X2Y_BP                       <- as.factor(csv_6Y$X2Y_BP                    )')
r('csv_6Y$X2Y_BLDS                     <- as.factor(csv_6Y$X2Y_BLDS                  )')
r('csv_6Y$X2Y_TOT_CHOLE                <- as.factor(csv_6Y$X2Y_TOT_CHOLE             )')
r('csv_6Y$X2Y_HMG                      <- as.factor(csv_6Y$X2Y_HMG                   )')
r('csv_6Y$X2Y_OLIG_PROTE_CD            <- as.factor(csv_6Y$X2Y_OLIG_PROTE_CD         )')
r('csv_6Y$X2Y_SGOT_AST                 <- as.factor(csv_6Y$X2Y_SGOT_AST              )')
r('csv_6Y$X2Y_SGPT_ALT                 <- as.factor(csv_6Y$X2Y_SGPT_ALT              )')
r('csv_6Y$X2Y_GAMMA_GTP                <- as.factor(csv_6Y$X2Y_GAMMA_GTP             )')
r('csv_6Y$X2Y_SMK_STAT_TYPE_RSPS_CD    <- as.factor(csv_6Y$X2Y_SMK_STAT_TYPE_RSPS_CD )')
r('csv_6Y$X2Y_SMK_TERM_RSPS_CD         <- as.factor(csv_6Y$X2Y_SMK_TERM_RSPS_CD      )')
r('csv_6Y$X2Y_DSQTY_RSPS_CD            <- as.factor(csv_6Y$X2Y_DSQTY_RSPS_CD         )')
r('csv_6Y$X2Y_DRNK_HABIT_RSPS_CD       <- as.factor(csv_6Y$X2Y_DRNK_HABIT_RSPS_CD    )')
r('csv_6Y$X2Y_TM1_DRKQTY_RSPS_CD       <- as.factor(csv_6Y$X2Y_TM1_DRKQTY_RSPS_CD    )')
r('csv_6Y$X2Y_EXERCI_FREQ_RSPS_CD      <- as.factor(csv_6Y$X2Y_EXERCI_FREQ_RSPS_CD   )')
r('csv_6Y$X4Y_YKIHO_GUBUN_CD           <- as.factor(csv_6Y$X4Y_YKIHO_GUBUN_CD        )')
r('csv_6Y$X4Y_YKIHO_GUBUN_CD        <-relevel(csv_6Y$X4Y_YKIHO_GUBUN_CD, ref="3")')
r('csv_6Y$X4Y_BMI                      <- as.factor(csv_6Y$X4Y_BMI                   )')
r('csv_6Y$X4Y_BP                       <- as.factor(csv_6Y$X4Y_BP                    )')
r('csv_6Y$X4Y_BLDS                     <- as.factor(csv_6Y$X4Y_BLDS                  )')
r('csv_6Y$X4Y_TOT_CHOLE                <- as.factor(csv_6Y$X4Y_TOT_CHOLE             )')
r('csv_6Y$X4Y_HMG                      <- as.factor(csv_6Y$X4Y_HMG                   )')
r('csv_6Y$X4Y_OLIG_PROTE_CD            <- as.factor(csv_6Y$X4Y_OLIG_PROTE_CD         )')
r('csv_6Y$X4Y_SGOT_AST                 <- as.factor(csv_6Y$X4Y_SGOT_AST              )')
r('csv_6Y$X4Y_SGPT_ALT                 <- as.factor(csv_6Y$X4Y_SGPT_ALT              )')
r('csv_6Y$X4Y_GAMMA_GTP                <- as.factor(csv_6Y$X4Y_GAMMA_GTP             )')
r('csv_6Y$X4Y_SMK_STAT_TYPE_RSPS_CD    <- as.factor(csv_6Y$X4Y_SMK_STAT_TYPE_RSPS_CD )')
r('csv_6Y$X4Y_SMK_TERM_RSPS_CD         <- as.factor(csv_6Y$X4Y_SMK_TERM_RSPS_CD      )')
r('csv_6Y$X4Y_DSQTY_RSPS_CD            <- as.factor(csv_6Y$X4Y_DSQTY_RSPS_CD         )')
r('csv_6Y$X4Y_DRNK_HABIT_RSPS_CD       <- as.factor(csv_6Y$X4Y_DRNK_HABIT_RSPS_CD    )')
r('csv_6Y$X4Y_TM1_DRKQTY_RSPS_CD       <- as.factor(csv_6Y$X4Y_TM1_DRKQTY_RSPS_CD    )')
r('csv_6Y$X4Y_EXERCI_FREQ_RSPS_CD      <- as.factor(csv_6Y$X4Y_EXERCI_FREQ_RSPS_CD   )')
r('csv_6Y$X6Y_YKIHO_GUBUN_CD           <- as.factor(csv_6Y$X6Y_YKIHO_GUBUN_CD        )')
r('csv_6Y$X6Y_YKIHO_GUBUN_CD        <-relevel(csv_6Y$X6Y_YKIHO_GUBUN_CD, ref="3")')
r('csv_6Y$X6Y_BMI                      <- as.factor(csv_6Y$X6Y_BMI                   )')
r('csv_6Y$X6Y_BP                       <- as.factor(csv_6Y$X6Y_BP                    )')
r('csv_6Y$X6Y_BLDS                     <- as.factor(csv_6Y$X6Y_BLDS                  )')
r('csv_6Y$X6Y_TOT_CHOLE                <- as.factor(csv_6Y$X6Y_TOT_CHOLE             )')
r('csv_6Y$X6Y_HMG                      <- as.factor(csv_6Y$X6Y_HMG                   )')
r('csv_6Y$X6Y_OLIG_PROTE_CD            <- as.factor(csv_6Y$X6Y_OLIG_PROTE_CD         )')
r('csv_6Y$X6Y_SGOT_AST                 <- as.factor(csv_6Y$X6Y_SGOT_AST              )')
r('csv_6Y$X6Y_SGPT_ALT                 <- as.factor(csv_6Y$X6Y_SGPT_ALT              )')
r('csv_6Y$X6Y_GAMMA_GTP                <- as.factor(csv_6Y$X6Y_GAMMA_GTP             )')
r('csv_6Y$X6Y_SMK_STAT_TYPE_RSPS_CD    <- as.factor(csv_6Y$X6Y_SMK_STAT_TYPE_RSPS_CD )')
r('csv_6Y$X6Y_SMK_TERM_RSPS_CD         <- as.factor(csv_6Y$X6Y_SMK_TERM_RSPS_CD      )')
r('csv_6Y$X6Y_DSQTY_RSPS_CD            <- as.factor(csv_6Y$X6Y_DSQTY_RSPS_CD         )')
r('csv_6Y$X6Y_DRNK_HABIT_RSPS_CD       <- as.factor(csv_6Y$X6Y_DRNK_HABIT_RSPS_CD    )')
r('csv_6Y$X6Y_TM1_DRKQTY_RSPS_CD       <- as.factor(csv_6Y$X6Y_TM1_DRKQTY_RSPS_CD    )')
r('csv_6Y$X6Y_EXERCI_FREQ_RSPS_CD      <- as.factor(csv_6Y$X6Y_EXERCI_FREQ_RSPS_CD   )')
r("""logr_6Y <- glm(MAIN_SICKS ~
                 PERSON_ID                + YEAR                      + SEX                       +
                 AGE_GROUP                + SIDO                      + IPSN_TYPE_CD              + CTRB_PT_TYPE_CD            +
                 DFAB_GRD_CD              + HCHK_APOP_PMH_YN          + HCHK_HDISE_PMH_YN         +
                 HCHK_HPRTS_PMH_YN        + HCHK_DIABML_PMH_YN        + HCHK_HPLPDM_PMH_YN        + HCHK_PHSS_PMH_YN           +
                 HCHK_ETCDSE_PMH_YN       + FMLY_APOP_PATIEN_YN       + FMLY_HDISE_PATIEN_YN      + FMLY_HPRTS_PATIEN_YN       +
                 FMLY_DIABML_PATIEN_YN    + FMLY_CANCER_PATIEN_YN     + X2Y_HCHK_YEAR             + X2Y_YKIHO_GUBUN_CD         +
                 X2Y_BMI                  + X2Y_BP                    + X2Y_BLDS                  + X2Y_TOT_CHOLE              +
                 X2Y_HMG                  + X2Y_OLIG_PROTE_CD         + X2Y_SGOT_AST              + X2Y_SGPT_ALT               +
                 X2Y_GAMMA_GTP            + X2Y_SMK_STAT_TYPE_RSPS_CD + X2Y_SMK_TERM_RSPS_CD      + X2Y_DSQTY_RSPS_CD          +
                 X2Y_DRNK_HABIT_RSPS_CD   + X2Y_TM1_DRKQTY_RSPS_CD    + X2Y_EXERCI_FREQ_RSPS_CD   + X4Y_HCHK_YEAR              +
                 X4Y_YKIHO_GUBUN_CD       + X4Y_BMI                   + X4Y_BP                    + X4Y_BLDS                   +
                 X4Y_TOT_CHOLE            + X4Y_HMG                   + X4Y_OLIG_PROTE_CD         + X4Y_SGOT_AST               +
                 X4Y_SGPT_ALT             + X4Y_GAMMA_GTP             + X4Y_SMK_STAT_TYPE_RSPS_CD + X4Y_SMK_TERM_RSPS_CD       +
                 X4Y_DSQTY_RSPS_CD        + X4Y_DRNK_HABIT_RSPS_CD    + X4Y_TM1_DRKQTY_RSPS_CD    + X4Y_EXERCI_FREQ_RSPS_CD    +
                 X6Y_HCHK_YEAR            + X6Y_YKIHO_GUBUN_CD        + X6Y_BMI                   + X6Y_BP                     +
                 X6Y_BLDS                 + X6Y_TOT_CHOLE             + X6Y_HMG                   + X6Y_OLIG_PROTE_CD          +
                 X6Y_SGOT_AST             + X6Y_SGPT_ALT              + X6Y_GAMMA_GTP             + X6Y_SMK_STAT_TYPE_RSPS_CD  +
                 X6Y_SMK_TERM_RSPS_CD     + X6Y_DSQTY_RSPS_CD         + X6Y_DRNK_HABIT_RSPS_CD    + X6Y_TM1_DRKQTY_RSPS_CD     +
                 X6Y_EXERCI_FREQ_RSPS_CD
               , data=csv_6Y, family=binomial)""")
r('sum_logr_6Y<-summary(logr_6Y)')
r('para<-sum_logr_6Y$coefficients')
r('OR<-exp(para[,1])')
r('cbind(OR,para[,c(2,4)])')
r('capture.output(cbind(OR,para[,c(2,4)]), file = "glm_EXP:{0}_CTR:{4}_{1}_{3}.txt")'.format(illname, yearterm, yearterm * (yearterm2-1),
                                                                                   yearterm * (yearterm2),illname2))
r('capture.output(c("GLM analysis"), file = "Report_EXP:{0}_CTR:{4}.txt",append=TRUE)'.format(illname, yearterm, yearterm * (yearterm2-1),
                                                                                   yearterm * (yearterm2),illname2))
r('library(ResourceSelection)')
r('hoslem.test(logr_6Y$y, logr_6Y$fit)')
r('capture.output(hoslem.test(logr_6Y$y, logr_6Y$fit), file = "glm_EXP:{0}_CTR:{4}_{1}_{3}.txt",append=TRUE)'.format(illname, yearterm, yearterm * (yearterm2-1),
                                                                                   yearterm * (yearterm2),illname2))
r('capture.output(hoslem.test(logr_6Y$y, logr_6Y$fit), file = "Report_EXP:{0}_CTR:{4}.txt",append=TRUE)'.format(illname, yearterm, yearterm * (yearterm2-1),
                                                                                   yearterm * (yearterm2),illname2))

r('library(pscl)')
r('capture.output(pR2(logr_6Y), file = "glm_EXP:{0}_CTR:{4}_{1}_{3}.txt",append=TRUE)'.format(illname, yearterm, yearterm * (yearterm2-1),
                                                                                   yearterm * (yearterm2),illname2))

r('capture.output(pR2(logr_6Y), file = "Report_EXP:{0}_CTR:{4}.txt",append=TRUE)'.format(illname, yearterm, yearterm * (yearterm2-1),
                                                                                   yearterm * (yearterm2),illname2))

if step_yn == 'L':
    step_yn = input("Stepwise Selection을 하시겠습니까?(Y/N)")
if step_yn == 'Y' or step_yn == 'y':
    print("START STEPWISE SELECTION")
    r('Slm <- step(logr_6Y,direction = "both")')
    r('Slm')
    r('para<-summary(Slm)$coefficients')
    r('OR<-exp(para[,1])')
    r('cbind(OR,para[,c(2,4)])')
    r('capture.output(cbind(OR,para[,c(2,4)]), file = "slm_EXP:{0}_CTR:{4}_{1}_{3}.txt")'.format(illname, yearterm, yearterm * (yearterm2-1),
                                                                                   yearterm * (yearterm2),illname2))
    r('capture.output(c("SLM analysis"), file = "Report_EXP:{0}_CTR:{4}.txt",append=TRUE)'.format(illname, yearterm,yearterm * (yearterm2 - 1),
                                                                                                  yearterm * (yearterm2),illname2))
    r('library(ResourceSelection)')
    r('hoslem.test(Slm$y, Slm$fit)')
    r('capture.output(hoslem.test(Slm$y, Slm$fit), file = "slm_EXP:{0}_CTR:{4}_{1}_{3}.txt",append=TRUE)'.format(illname, yearterm, yearterm * (yearterm2-1),
                                                                                   yearterm * (yearterm2),illname2))
    r('capture.output(hoslem.test(Slm$y, Slm$fit), file = "Report_EXP:{0}_CTR:{4}.txt",append=TRUE)'.format(illname, yearterm, yearterm * (yearterm2 - 1),
                                                                                                                 yearterm * (yearterm2), illname2))
    r('library(pscl)')
    r('capture.output(pR2(Slm), file = "slm_EXP:{0}_CTR:{4}_{1}_{3}.txt",append=TRUE)'.format(illname, yearterm, yearterm * (yearterm2-1),
                                                                                   yearterm * (yearterm2),illname2))
    r('capture.output(pR2(Slm), file = "Report_EXP:{0}_CTR:{4}.txt",append=TRUE)'.format(illname, yearterm, yearterm * (yearterm2 - 1),
                                                                                                                 yearterm * (yearterm2), illname2))