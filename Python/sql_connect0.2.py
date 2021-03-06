import mysql.connector
import csv
import time

# connection

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
mycursor = mydb.cursor(dictionary=True)

print("SET GLOBAL innodb_buffer_pool_size=64G")
mycursor.execute("SET GLOBAL innodb_buffer_pool_size=68719476736")

illinput = []
while True:
    temp = input("실험군 질병 입력(끝:0) : ")
    if temp == '0':
        break
    else:
        illinput.append(temp)
yearterm = int(input("데이터 연도 간격 (<15) : "))
yearterm2 = int(input("데이터 연도 간격 횟수 (연도간격({})*N<15) : ".format(yearterm)))

total_start = time.clock()
exp_start = total_start
print("CREATE EXP DATA ... START")

illname = '_'.join(illinput)

EXP_STEP1_like = []
for i in illinput:
    EXP_STEP1_like.append('T1.MAIN_SICK LIKE "{}%"'.format(i))

EXP_STEP1_where = """ OR
""".join(EXP_STEP1_like)

EXP_STEP1 = """CREATE TABLE ILL.T120_{0} AS
(
SELECT T1.*
FROM ILL.T120 AS T1
WHERE
{1}
)
""".format(illname, EXP_STEP1_where)


print("EXP_STEP1-1.CREATE ILL.T120_{} ... START".format(illname))

step_start = time.clock()
mycursor.execute(EXP_STEP1)
mydb.commit()
step_end = time.clock()
print("EXP_STEP1-1.CREATE ILL.T120_{} ... DONE".format(illname))
print("STEP1-1:{:.6f}s, Total:{:.6f}s\n".format(step_end - step_start,step_end - total_start))

print("EXP_STEP1-2.CREATE INDEX person ON ILL.T120_{} ... START".format(illname))
step_start = time.clock()
mycursor.execute("CREATE INDEX person ON ILL.T120_{}(PERSON_ID)".format(illname))
mydb.commit()
step_end = time.clock()
print("EXP_STEP1-2.CREATE INDEX person ON ILL.T120_{} ... DONE".format(illname))
print("STEP1-2:{:.6f}s, Total:{:.6f}s\n".format(step_end - step_start,step_end - total_start))

EXP_STEP2 = """CREATE TABLE ILL.T120_{0}_YEAR AS
(
    SELECT
   	 T1.PERSON_ID, T1.MIN_YEAR, GROUP_CONCAT(DISTINCT(MAIN_SICK) SEPARATOR '/') AS MAIN_SICKS
    FROM
   	 (
    	SELECT PERSON_ID, MIN(YEAR(RECU_FR_DT)) AS MIN_YEAR
   	 FROM ILL.T120_{0}
   	 GROUP BY PERSON_ID
   	 ) AS T1
    RIGHT JOIN ILL.T120_{0} AS T2
   	 ON T1.PERSON_ID = T2.PERSON_ID
    WHERE T1.MIN_YEAR = YEAR(T2.RECU_FR_DT)
	GROUP BY T1.PERSON_ID
)
""".format(illname)

print("EXP_STEP2-1.CREATE ILL.T120_{}_YEAR ... START".format(illname))
step_start = time.clock()
mycursor.execute(EXP_STEP2)
mydb.commit()
step_end = time.clock()
print("EXP_STEP2-1.CREATE ILL.T120_{}_YEAR ... DONE".format(illname))
print("STEP2-1:{:.6f}s, Total:{:.6f}s\n".format(step_end - step_start,step_end - total_start))

print("EXP_STEP2-2.CREATE INDEX person ON ILL.T120_{}_YEAR ... START".format(illname))
step_start = time.clock()
mycursor.execute("CREATE INDEX person ON ILL.T120_{}_YEAR(PERSON_ID,MIN_YEAR)".format(illname))
mydb.commit()
step_end = time.clock()
print("EXP_STEP2-2.CREATE INDEX person ON ILL.T120_{}_YEAR ... DONE".format(illname))
print("STEP2-2:{:.6f}s, Total:{:.6f}s\n".format(step_end - step_start,step_end - total_start))

EXP_STEP3 = """CREATE TABLE ILL.T120_{0}_YEAR_JK AS (
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
	LEFT JOIN ILL.T120_{0}_YEAR AS T2
	ON T1.PERSON_ID = T2.PERSON_ID
	WHERE T1.STND_Y = T2.MIN_YEAR AND DTH_YM =""
)
""".format(illname)

print("EXP_STEP3-1.CREATE ILL.T120_{}_YEAR_JK ... START".format(illname))
step_start = time.clock()
mycursor.execute(EXP_STEP3)
mydb.commit()
step_end = time.clock()
print("EXP_STEP3-1.CREATE ILL.T120_{}_YEAR_JK ... DONE".format(illname))
print("STEP3-1:{:.6f}s, Total:{:.6f}s\n".format(step_end - step_start,step_end - total_start))

print("EXP_STEP3-2.CREATE INDEX person ON ILL.T120_{}_YEAR_JK ... START".format(illname))
step_start = time.clock()
mycursor.execute("CREATE INDEX person ON ILL.T120_{}_YEAR_JK(PERSON_ID,MIN_YEAR)".format(illname))
mydb.commit()
step_end = time.clock()
print("EXP_STEP3-2.CREATE INDEX person ON ILL.T120_{}_YEAR_JK ... DONE".format(illname))
print("STEP3-2:{:.6f}s, Total:{:.6f}s\n".format(step_end - step_start,step_end - total_start))

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
FROM ILL.T120_{0}_YEAR_JK AS T1
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
step_start = time.clock()
mycursor.execute(EXP_STEP4)
mydb.commit()
step_end = time.clock()
print("EXP_STEP4-1.CREATE EXP_{0}_{1}_{2}YEAR ... DONE".format(illname, yearterm, yearterm * 0))
print("STEP4-1:{:.6f}s, Total:{:.6f}s\n".format(step_end - step_start,step_end - total_start))

print("EXP_STEP4-2.CREATE INDEX person ON EXP_{0}_{1}_{2}YEAR ... START".format(illname, yearterm, yearterm * 0))
step_start = time.clock()
mycursor.execute("CREATE INDEX person ON EXP_{0}_{1}_{2}YEAR(PERSON_ID,YEAR)".format(illname, yearterm, yearterm * 0))
mydb.commit()
step_end = time.clock()
print("EXP_STEP4-2.CREATE INDEX person ON EXP_{0}_{1}_{2}YEAR ... DONE".format(illname, yearterm, yearterm * 0))
print("STEP4-2:{:.6f}s, Total:{:.6f}s\n".format(step_end - step_start,step_end - total_start))

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
    WHERE T1.`YEAR` = (T2.HCHK_YEAR + 2)    AND
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
    step_start = time.clock()
    mycursor.execute(EXP_STEP5)
    mydb.commit()
    step_end = time.clock()
    print("EXP_STEP5-1.CREATE EXP_{0}_{1}_{3}YEAR ... DONE".format(illname, yearterm, yearterm * count,
                                                                   yearterm * (count + 1)))
    print("STEP5-1:{:.6f}s, Total:{:.6f}s\n".format(step_end - step_start, step_end - total_start))
    print("EXP_STEP5-2.CREATE INDEX person ON EXP_{0}_{1}_{3}YEAR ... START".format(illname, yearterm, yearterm * count,
                                                                                    yearterm * (count + 1)))
    step_start = time.clock()
    mycursor.execute(
        "CREATE INDEX person ON EXP_{0}_{1}_{3}YEAR(PERSON_ID,YEAR)".format(illname, yearterm, yearterm * count,
                                                                            yearterm * (count + 1)))
    mydb.commit()
    step_end = time.clock()
    print("EXP_STEP5-2.CREATE INDEX person ON EXP_{0}_{1}_{3}YEAR ... DONE".format(illname, yearterm, yearterm * count,
                                                                                   yearterm * (count + 1)))
    print("STEP5-2:{:.6f}s, Total:{:.6f}s\n".format(step_end - step_start, step_end - total_start))

print("CREATE EXP DATA ... DONE")
print("EXP:{:.6f}s, Total:{:.6f}s\n".format(step_end - exp_start,step_end - total_start))

ctr_start = time.clock()
print("CREATE CTR DATA ... START")

CTR_STEP0_sub_like = []
for i in illinput:
    CTR_STEP0_sub_like.append('T1.SUB_SICK LIKE "{}%"'.format(i))

CTR_STEP0_sub_where = """ OR
""".join(CTR_STEP0_sub_like)

CTR_STEP0 = """CREATE TABLE ILL.T120_SUB_{0} AS
(
SELECT T1.*
FROM ILL.T120 AS T1
WHERE
{1}
)
""".format(illname, CTR_STEP0_sub_where)

print("CTR_STEP0-1.CREATE ILL.T120_SUB_{} ... START".format(illname))
step_start = time.clock()
mycursor.execute(CTR_STEP0)
mydb.commit()
step_end = time.clock()
print("CTR_STEP0-1.CREATE ILL.T120_SUB_{} ... DONE".format(illname))
print("STEP0-1:{:.6f}s, Total:{:.6f}s\n".format(step_end - step_start, step_end - total_start))

print("EXP_STEP0-2.CREATE INDEX person ON ILL.T120_SUB_{} ... START".format(illname))
step_start = time.clock()
mycursor.execute("CREATE INDEX person ON ILL.T120_SUB_{}(PERSON_ID)".format(illname))
mydb.commit()
step_end = time.clock()
print("EXP_STEP0-2.CREATE INDEX person ON ILL.T120_SUB_{} ... DONE".format(illname))
print("STEP0-2:{:.6f}s, Total:{:.6f}s\n".format(step_end - step_start, step_end - total_start))

CTR_STEP1 = """CREATE TABLE ILL.T120_NOT_{0}_NOT_SUB_{0} AS
(
	SELECT *
	FROM ILL.T120 AS T1
	WHERE
    	(
        	T1.PERSON_ID NOT IN(
            	SELECT PERSON_ID
            	FROM ILL.T120_{0})
    	)
    	AND
    	(
        	T1.PERSON_ID NOT IN(
            	SELECT PERSON_ID
            	FROM ILL.T120_SUB_{0})
    	)
)

""".format(illname)

print("CTR_STEP1-1.CREATE ILL.T120_NOT_{0}_NOT_SUB_{0} ... START".format(illname))
step_start = time.clock()
mycursor.execute(CTR_STEP1)
mydb.commit()
step_end = time.clock()
print("CTR_STEP1-1.CREATE ILL.T120_NOT_{0}_NOT_SUB_{0} ... DONE".format(illname))
print("STEP1-1:{:.6f}s, Total:{:.6f}s\n".format(step_end - step_start, step_end - total_start))

print("CTR_STEP1-2.CREATE INDEX person ON ILL.T120_NOT_{0}_NOT_SUB_{0} ... START".format(illname))
step_start = time.clock()
mycursor.execute("CREATE INDEX person ON ILL.T120_NOT_{0}_NOT_SUB_{0}(PERSON_ID)".format(illname))
mydb.commit()
step_end = time.clock()
print("CTR_STEP1-2.CREATE INDEX person ON ILL.T120_NOT_{0}_NOT_SUB_{0} ... DONE".format(illname))
print("STEP1-2:{:.6f}s, Total:{:.6f}s\n".format(step_end - step_start, step_end - total_start))

CTR_STEP2 = """CREATE TABLE ILL.T120_NOT_{0}_NOT_SUB_{0}_YEAR AS
(
    SELECT
   	 T1.PERSON_ID, T1.MAX_YEAR, GROUP_CONCAT(DISTINCT(MAIN_SICK) SEPARATOR '/') AS MAIN_SICKS
    FROM
   	 (
    	SELECT PERSON_ID, MAX(YEAR(RECU_FR_DT)) AS MAX_YEAR
   	 FROM ILL.T120_NOT_{0}_NOT_SUB_{0}
   	 GROUP BY PERSON_ID
   	 ) AS T1
    RIGHT JOIN ILL.T120_NOT_{0}_NOT_SUB_{0} AS T2
   	 ON T1.PERSON_ID = T2.PERSON_ID
    WHERE T1.MAX_YEAR = YEAR(T2.RECU_FR_DT) AND T1.MAX_YEAR = 2013
	GROUP BY T1.PERSON_ID
)
""".format(illname)

print("CTR_STEP2-1.CREATE ILL.T120_NOT_{0}_NOT_SUB_{0}_YEAR ... START".format(illname))
step_start = time.clock()
mycursor.execute(CTR_STEP2)
mydb.commit()
step_end = time.clock()
print("CTR_STEP2-1.CREATE ILL.T120_NOT_{0}_NOT_SUB_{0}_YEAR ... DONE".format(illname))
print("STEP2-1:{:.6f}s, Total:{:.6f}s\n".format(step_end - step_start, step_end - total_start))

print("CTR_STEP2-2.CREATE INDEX person ON ILL.T120_NOT_{0}_NOT_SUB_{0}_YEAR ... START".format(illname))
step_start = time.clock()
mycursor.execute("CREATE INDEX person ON ILL.T120_NOT_{0}_NOT_SUB_{0}_YEAR(PERSON_ID,MAX_YEAR)".format(illname))
mydb.commit()
step_end = time.clock()
print("CTR_STEP2-2.CREATE INDEX person ON ILL.T120_NOT_{0}_NOT_SUB_{0}_YEAR ... DONE".format(illname))
print("STEP2-2:{:.6f}s, Total:{:.6f}s\n".format(step_end - step_start, step_end - total_start))

CTR_STEP3 = """CREATE TABLE ILL.T120_NOT_{0}_NOT_SUB_{0}_YEAR_JK AS (
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
	LEFT JOIN ILL.T120_NOT_{0}_NOT_SUB_{0}_YEAR AS T2
	ON T1.PERSON_ID = T2.PERSON_ID
	WHERE T1.STND_Y = T2.MAX_YEAR AND DTH_YM =""
)
""".format(illname)

print("CTR_STEP3-1.CREATE ILL.T120_NOT_{0}_NOT_SUB_{0}_YEAR_JK ... START".format(illname))
step_start = time.clock()
mycursor.execute(CTR_STEP3)
mydb.commit()
step_end = time.clock()
print("CTR_STEP3-1.CREATE ILL.T120_NOT_{0}_NOT_SUB_{0}_YEAR_JK ... DONE".format(illname))
print("STEP3-1:{:.6f}s, Total:{:.6f}s\n".format(step_end - step_start, step_end - total_start))

print("CTR_STEP3-2.CREATE INDEX person ON ILL.T120_NOT_{0}_NOT_SUB_{0}_YEAR_JK ... START".format(illname))
step_start = time.clock()
mycursor.execute("CREATE INDEX person ON ILL.T120_NOT_{0}_NOT_SUB_{0}_YEAR_JK(PERSON_ID,MAX_YEAR)".format(illname))
mydb.commit()
step_end = time.clock()
print("CTR_STEP3-2.CREATE INDEX person ON ILL.T120_NOT_{0}_NOT_SUB_{0}_YEAR_JK ... DONE".format(illname))
print("STEP3-2:{:.6f}s, Total:{:.6f}s\n".format(step_end - step_start, step_end - total_start))

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
FROM ILL.T120_NOT_{0}_NOT_SUB_{0}_YEAR_JK AS T1
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
""".format(illname, yearterm, yearterm * 0)
print("CTR_STEP4-1.CREATE CTR_{0}_{1}_{2}YEAR ... START".format(illname, yearterm, yearterm * 0))
step_start = time.clock()
mycursor.execute(CTR_STEP4)
mydb.commit()
step_end = time.clock()
print("CTR_STEP4-1.CREATE CTR_{0}_{1}_{2}YEAR ... DONE".format(illname, yearterm, yearterm * 0))
print("STEP4-1:{:.6f}s, Total:{:.6f}s\n".format(step_end - step_start, step_end - total_start))

print("CTR_STEP4-2.CREATE INDEX person ON CTR_{0}_{1}_{2}YEAR ... START".format(illname, yearterm, yearterm * 0))
step_start = time.clock()
mycursor.execute("CREATE INDEX person ON CTR_{0}_{1}_{2}YEAR(PERSON_ID,YEAR)".format(illname, yearterm, yearterm * 0))
mydb.commit()
step_end = time.clock()
print("CTR_STEP4-2.CREATE INDEX person ON CTR_{0}_{1}_{2}YEAR ... DONE".format(illname, yearterm, yearterm * 0))
print("STEP4-2:{:.6f}s, Total:{:.6f}s\n".format(step_end - step_start, step_end - total_start))

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
    WHERE T1.`YEAR` = (T2.HCHK_YEAR + 2)    AND
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
    print("CTR_STEP5-1.CREATE CTR_{0}_{1}_{3}YEAR ... START".format(illname, yearterm, yearterm * count,
                                                                    yearterm * (count + 1)))
    step_start = time.clock()
    mycursor.execute(CTR_STEP5)
    mydb.commit()
    step_end = time.clock()
    print("CTR_STEP5-1.CREATE CTR_{0}_{1}_{3}YEAR ... DONE".format(illname, yearterm, yearterm * count,
                                                                   yearterm * (count + 1)))
    print("STEP5-1:{:.6f}s, Total:{:.6f}s\n".format(step_end - step_start, step_end - total_start))

    print("CTR_STEP5-2.CREATE INDEX person ON CTR_{0}_{1}_{3}YEAR ... START".format(illname, yearterm, yearterm * count,
                                                                                    yearterm * (count + 1)))
    step_start = time.clock()
    mycursor.execute(
        "CREATE INDEX person ON CTR_{0}_{1}_{3}YEAR(PERSON_ID,YEAR)".format(illname, yearterm, yearterm * count,
                                                                            yearterm * (count + 1)))
    mydb.commit()
    step_end = time.clock()
    print("CTR_STEP5-2.CREATE INDEX person ON CTR_{0}_{1}_{3}YEAR ... DONE".format(illname, yearterm, yearterm * count,
                                                                                   yearterm * (count + 1)))
    print("STEP5-2:{:.6f}s, Total:{:.6f}s\n".format(step_end - step_start, step_end - total_start))

print("CREATE CTR DATA ... DONE")
print("CTR:{:.6f}s, Total:{:.6f}s\n".format(step_end - ctr_start, step_end - total_start))

