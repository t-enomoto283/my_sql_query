CREATE TABLE guide_traffic_2508(
    イベント名 CHAR(25),
    ページタイトル VARCHAR2(300),
    ページパス VARCHAR2(200),
    イベント数 NUMBER(5)
    )
    ORGANIZATION EXTERNAL
    (TYPE ORACLE_LOADER
    DEFAULT DIRECTORY temp_dir
    ACCESS PARAMETERS
        (RECORDS DELIMITED BY NEWLINE
        SKIP 1
        BADFILE 'guide_traffic_2508.bad'
        LOGFILE 'guide_traffic_2508.log'
        CHARACTERSET AL32UTF8
        FIELDS TERMINATED BY ','
        OPTIONALLY ENCLOSED BY '"'
            (
            イベント名 CHAR(25),
            ページタイトル CHAR(300),
            ページパス CHAR(200),
            イベント数 FLOAT EXTERNAL
            )
        )
    LOCATION ('guide_traffic_2508.csv')
    )
    REJECT LIMIT UNLIMITED
    ;


CREATE TABLE guide_pv_2508(
    イベント名 CHAR(24),
    ページタイトル VARCHAR2(300),
    ページパス VARCHAR2(200),
    イベント数 NUMBER(5)
    )
    ORGANIZATION EXTERNAL
    (TYPE ORACLE_LOADER
    DEFAULT DIRECTORY temp_dir
    ACCESS PARAMETERS
        (RECORDS DELIMITED BY NEWLINE
        SKIP 1
        BADFILE 'guide_pv_2508.bad'
        LOGFILE 'guide_pv_2508.log'
        CHARACTERSET AL32UTF8
        FIELDS TERMINATED BY ','
        OPTIONALLY ENCLOSED BY '"'
            (
            イベント名 CHAR(24),
            ページタイトル CHAR(300),
            ページパス CHAR(200),
            イベント数 FLOAT EXTERNAL
            )
        )
    LOCATION ('guide_pv_2508.csv')
    )
    REJECT LIMIT UNLIMITED
    ;


CREATE TABLE guide_senni_2508(
    イベント名 VARCHAR(50),
    ページタイトル VARCHAR2(400),
    ページパス VARCHAR2(200),
    イベント数 NUMBER(5)
    )
    ORGANIZATION EXTERNAL
    (TYPE ORACLE_LOADER
    DEFAULT DIRECTORY temp_dir
    ACCESS PARAMETERS
        (RECORDS DELIMITED BY NEWLINE
        SKIP 1
        BADFILE 'guide_senni_2508.bad'
        LOGFILE 'guide_senni_2508.log'
        CHARACTERSET AL32UTF8
        FIELDS TERMINATED BY ','
        OPTIONALLY ENCLOSED BY '"'
            (
            イベント名 CHAR(50),
            ページタイトル CHAR(400),
            ページパス CHAR(200),
            イベント数 FLOAT EXTERNAL
            )
        )
    LOCATION ('guide_senni_2508.csv')
    )
    REJECT LIMIT UNLIMITED
    ;


CREATE TABLE guide_tap_2508(
    shop_id NUMBER(6),
    shop_name VARCHAR2(200),
    shop_area VARCHAR2(200),
    shop_biz VARCHAR2(200),
    lp VARCHAR2(300),
    tap NUMBER(4)
    )
    ORGANIZATION EXTERNAL
    (TYPE ORACLE_LOADER
    DEFAULT DIRECTORY temp_dir
    ACCESS PARAMETERS
        (RECORDS DELIMITED BY NEWLINE
        SKIP 1
        BADFILE 'guide_tap_2508.bad'
        LOGFILE 'guide_tap_2508.log'
        CHARACTERSET AL32UTF8
        FIELDS TERMINATED BY ','
        OPTIONALLY ENCLOSED BY '"'
            (
            shop_id FLOAT EXTERNAL,
            shop_name CHAR(200),
            shop_area CHAR(200),
            shop_biz CHAR(200),
            lp CHAR(300),
            tap FLOAT EXTERNAL
            )
        )
    LOCATION ('guide_tap_2508.csv')
    )
    REJECT LIMIT UNLIMITED
    ;


CREATE TABLE guide_page_2508(
    ページパス VARCHAR2(200),
    クリック数 NUMBER(5),
    表示回数 NUMBER(5),
    CTR VARCHAR2(200),
    掲載順位 NUMBER(5,2)
    )
    ORGANIZATION EXTERNAL
    (TYPE ORACLE_LOADER
    DEFAULT DIRECTORY temp_dir
    ACCESS PARAMETERS
        (RECORDS DELIMITED BY NEWLINE
        SKIP 1
        BADFILE 'guide_page_2508.bad'
        LOGFILE 'guied_page_2508.log'
        CHARACTERSET AL32UTF8
        FIELDS TERMINATED BY ','
        OPTIONALLY ENCLOSED BY '"'
            (
            ページパス CHAR(200),
            クリック数 FLOAT EXTERNAL,
            表示回数 FLOAT EXTERNAL,
            CTR CHAR(200),
            掲載順位 FLOAT EXTERNAL
            )
        )
    LOCATION ('guide_page_2508.csv')
    )
    REJECT LIMIT UNLIMITED
    ;


SELECT sc7.ページパス, sc6.掲載順位 AS "6月順位", sc7.掲載順位 AS "7月順位", (sc6.掲載順位-sc7.掲載順位) AS "順位差"
    FROM sc_guide_page_2506 sc6 INNER JOIN sc_guide_page_2507 sc7
    ON sc6.ページパス = sc7.ページパス
    ;


SELECT cv7.lp, sc7.掲載順位, ss7.イベント数 AS "セッション", cv7.tap
    FROM sc_guide_page_2507 sc7 RIGHT OUTER JOIN ga_guide_tap_2507 cv7
    ON sc7.ページパス = cv7.lp
    INNER JOIN ga_guide_traffic_2507 ss7
    ON cv7.lp = ss7.ページパス
    ;


-- 流入差
SELECT
    COALESCE(ss7.ページパス, ss6.ページパス) AS "ページパス",
    NVL(ss6.流入2506, 0) AS "6月流入",
    NVL(ss7.流入2507, 0) AS "7月流入",
    NVL(ss7.流入2507, 0) - NVL(ss6.流入2506, 0) AS "差分"
    FROM (
        SELECT ページパス, SUM(イベント数) AS 流入2506
            FROM ga_guide_traffic_2506
            GROUP BY ページパス
        ) ss6
        FULL OUTER JOIN (
        SELECT ページパス, SUM(イベント数) AS 流入2507
            FROM ga_guide_traffic_2507
            GROUP BY ページパス
        ) ss7
        ON ss6.ページパス = ss7.ページパス
        ;

-- 反響差
SELECT
    COALESCE(cv7.lp, cv6.lp) AS "ランディングページ",
    NVL(cv6.反響2506, 0) AS "6月反響",
    NVL(cv7.反響2507, 0) AS "7月反響",
    NVL(cv7.反響2507, 0) - NVL(cv6.反響2506, 0) AS "差分"
    FROM(
        SELECT lp, SUM(tap) AS 反響2506
            FROM ga_guide_tap_2506
            GROUP BY lp
        ) cv6
        FULL OUTER JOIN(
        SELECT lp, SUM(tap) AS 反響2507
            FROM ga_guide_tap_2507
            GROUP BY lp
        ) cv7
        ON cv6.lp = cv7.lp
        ;


-- ガイド流入差および反響差
WITH t_ss AS(
    SELECT
        COALESCE(ss8.ページパス, ss7.ページパス) AS ページパス,
        NVL(ss7.流入2507, 0) AS 流入2507,
        NVL(ss8.流入2508, 0) AS 流入2508
        FROM(
            SELECT ページパス, SUM(イベント数) AS 流入2507
                FROM ga_guide_traffic_2507
                GROUP BY ページパス
            ) ss7
            FULL OUTER JOIN(
            SELECT ページパス, SUM(イベント数) AS 流入2508
                FROM ga_guide_traffic_2508
                GROUP BY ページパス
            ) ss8
            ON ss7.ページパス = ss8.ページパス
        ),
 t_cv AS(
    SELECT
        COALESCE(cv8.lp, cv7.lp) AS ランディングページ,
        NVL(cv7.反響2507, 0) AS 反響2507,
        NVL(cv8.反響2508, 0) AS 反響2508
        FROM(
            SELECT lp, SUM(tap) AS 反響2507
                FROM ga_guide_tap_2507
                GROUP BY lp
            ) cv7
            FULL OUTER JOIN(
            SELECT lp, SUM(tap) AS 反響2508
                FROM ga_guide_tap_2508
                GROUP BY lp
            ) cv8
            ON cv7.lp = cv8.lp
        )
    SELECT
        COALESCE(t_ss.ページパス, t_cv.ランディングページ) AS ページ,
        NVL(t_ss.流入2507, 0) AS "7月流入",
        NVL(t_ss.流入2508, 0) AS "8月流入",
        NVL(t_ss.流入2508, 0) - NVL(t_ss.流入2507, 0) AS "流入差",
        NVL(t_cv.反響2507, 0) AS "7月反響",
        NVL(t_cv.反響2508, 0) AS "8月反響",
        NVL(t_cv.反響2508, 0) - NVL(t_cv.反響2507, 0) AS "反響差"
            FROM t_ss FULL OUTER JOIN t_cv
            ON t_ss.ページパス = t_cv.ランディングページ
            -- WHERE ページパス = '/guide/ko/14/'
            ;





/*雑記帳*/
CREATE TABLE notebook_traffic_2507(
    イベント名 CHAR(50),
    ページタイトル VARCHAR2(400),
    ページパス VARCHAR2(300),
    イベント数 NUMBER(6)
    )
    ORGANIZATION EXTERNAL
    (TYPE ORACLE_LOADER
    DEFAULT DIRECTORY temp_dir
    ACCESS PARAMETERS
        (RECORDS DELIMITED BY NEWLINE
        SKIP 1
        BADFILE 'notebook_traffic_2507.bad'
        LOGFILE 'notebook_traffic_2507.log'
        CHARACTERSET AL32UTF8
        FIELDS TERMINATED BY ','
        OPTIONALLY ENCLOSED BY '"'
            (
            イベント名 CHAR(50),
            ページタイトル CHAR(400),
            ページパス CHAR(300),
            イベント数 FLOAT EXTERNAL
            )
        )
    LOCATION ('notebook_traffic_2507.csv')
    )
    REJECT LIMIT UNLIMITED
    ;


CREATE TABLE notebook_senni_2507(
    イベント名 VARCHAR(100),
    ページタイトル VARCHAR2(500),
    ページパス VARCHAR2(400),
    イベント数 NUMBER(5)
    )
    ORGANIZATION EXTERNAL
    (TYPE ORACLE_LOADER
    DEFAULT DIRECTORY temp_dir
    ACCESS PARAMETERS
        (RECORDS DELIMITED BY NEWLINE
        SKIP 1
        BADFILE 'notebook_senni_2507.bad'
        LOGFILE 'notebook_senni_2507.log'
        CHARACTERSET AL32UTF8
        FIELDS TERMINATED BY ','
        OPTIONALLY ENCLOSED BY '"'
            (
            イベント名 CHAR(100),
            ページタイトル CHAR(500),
            ページパス CHAR(400),
            イベント数 FLOAT EXTERNAL
            )
        )
    LOCATION ('notebook_senni_2507.csv')
    )
    REJECT LIMIT UNLIMITED
    ;


CREATE TABLE notebook_tap_2507(
    shop_id NUMBER(10),
    shop_name VARCHAR2(400),
    shop_area VARCHAR2(200),
    shop_biz VARCHAR2(200),
    lp VARCHAR2(400),
    tap NUMBER(4)
    )
    ORGANIZATION EXTERNAL
    (TYPE ORACLE_LOADER
    DEFAULT DIRECTORY temp_dir
    ACCESS PARAMETERS
        (RECORDS DELIMITED BY NEWLINE
        SKIP 1
        BADFILE 'notebook_tap_2507.bad'
        LOGFILE 'notebook_tap_2507.log'
        CHARACTERSET AL32UTF8
        FIELDS TERMINATED BY ','
        OPTIONALLY ENCLOSED BY '"'
            (
            shop_id FLOAT EXTERNAL,
            shop_name CHAR(400),
            shop_area CHAR(200),
            shop_biz CHAR(200),
            lp CHAR(400),
            tap FLOAT EXTERNAL
            )
        )
    LOCATION ('notebook_tap_2507.csv')
    )
    REJECT LIMIT UNLIMITED
    ;


CREATE TABLE notebook_page_2507(
    ページパス VARCHAR2(300),
    クリック数 NUMBER(6),
    表示回数 NUMBER(6),
    CTR VARCHAR2(20),
    掲載順位 NUMBER(5,2)
    )
    ORGANIZATION EXTERNAL
    (TYPE ORACLE_LOADER
    DEFAULT DIRECTORY temp_dir
    ACCESS PARAMETERS
        (RECORDS DELIMITED BY NEWLINE
        SKIP 1
        BADFILE 'notebook_page_2507.bad'
        LOGFILE 'notebook_page_2507.log'
        CHARACTERSET AL32UTF8
        FIELDS TERMINATED BY ','
        OPTIONALLY ENCLOSED BY '"'
            (
            ページパス CHAR(300),
            クリック数 FLOAT EXTERNAL,
            表示回数 FLOAT EXTERNAL,
            CTR CHAR(20),
            掲載順位 FLOAT EXTERNAL
            )
        )
    LOCATION ('notebook_page_2507.csv')
    )
    REJECT LIMIT UNLIMITED
    ;


WITH t_ss AS(
    SELECT
        COALESCE(ss7.ページパス, ss6.ページパス) AS ページパス,
        NVL(ss6.流入2506, 0) AS 流入2506,
        NVL(ss7.流入2507, 0) AS 流入2507
        FROM(
            SELECT ページパス, SUM(イベント数) AS 流入2506
                FROM ga_notebook_traffic_2506
                GROUP BY ページパス
            ) ss6
            FULL OUTER JOIN(
            SELECT ページパス, SUM(イベント数) AS 流入2507
                FROM ga_notebook_traffic_2507
                GROUP BY ページパス
            ) ss7
            ON ss6.ページパス = ss7.ページパス
        ),
 t_cv AS(
    SELECT
        COALESCE(cv7.lp, cv6.lp) AS ランディングページ,
        NVL(cv6.反響2506, 0) AS 反響2506,
        NVL(cv7.反響2507, 0) AS 反響2507
        FROM(
            SELECT lp, SUM(tap) AS 反響2506
                FROM ga_notebook_tap_2506
                GROUP BY lp
            ) cv6
            FULL OUTER JOIN(
            SELECT lp, SUM(tap) AS 反響2507
                FROM ga_notebook_tap_2507
                GROUP BY lp
            ) cv7
            ON cv6.lp = cv7.lp
        )
    SELECT
        COALESCE(t_ss.ページパス, t_cv.ランディングページ) AS ページ,
        NVL(t_ss.流入2506, 0) AS "6月流入",
        NVL(t_ss.流入2507, 0) AS "7月流入",
        NVL(t_ss.流入2507, 0) - NVL(t_ss.流入2506, 0) AS "流入差",
        NVL(t_cv.反響2506, 0) AS "6月反響",
        NVL(t_cv.反響2507, 0) AS "7月反響",
        NVL(t_cv.反響2507, 0) - NVL(t_cv.反響2506, 0) AS "反響差"
            FROM t_ss FULL OUTER JOIN t_cv
            ON t_ss.ページパス = t_cv.ランディングページ
            -- WHERE ページパス = '/notebook/article27166/'
            -- ORDER BY 反響差 DESC
            ;


/*まとめ*/
CREATE TABLE matome_traffic_2507(
    イベント名 CHAR(50),
    ページタイトル VARCHAR2(400),
    ページパス VARCHAR2(200),
    イベント数 NUMBER(5)
    )
    ORGANIZATION EXTERNAL
    (TYPE ORACLE_LOADER
    DEFAULT DIRECTORY temp_dir
    ACCESS PARAMETERS
        (RECORDS DELIMITED BY NEWLINE
        SKIP 1
        BADFILE 'matome_traffic_2507.bad'
        LOGFILE 'matome_traffic_2507.log'
        CHARACTERSET AL32UTF8
        FIELDS TERMINATED BY ','
        OPTIONALLY ENCLOSED BY '"'
            (
            イベント名 CHAR(50),
            ページタイトル CHAR(400),
            ページパス CHAR(200),
            イベント数 FLOAT EXTERNAL
            )
        )
    LOCATION ('matome_traffic_2507.csv')
    )
    REJECT LIMIT UNLIMITED
    ;


CREATE TABLE matome_senni_2507(
    イベント名 VARCHAR(100),
    ページタイトル VARCHAR2(500),
    ページパス VARCHAR2(300),
    イベント数 NUMBER(5)
    )
    ORGANIZATION EXTERNAL
    (TYPE ORACLE_LOADER
    DEFAULT DIRECTORY temp_dir
    ACCESS PARAMETERS
        (RECORDS DELIMITED BY NEWLINE
        SKIP 1
        BADFILE 'matome_senni_2507.bad'
        LOGFILE 'matome_senni_2507.log'
        CHARACTERSET AL32UTF8
        FIELDS TERMINATED BY ','
        OPTIONALLY ENCLOSED BY '"'
            (
            イベント名 CHAR(100),
            ページタイトル CHAR(500),
            ページパス CHAR(300),
            イベント数 FLOAT EXTERNAL
            )
        )
    LOCATION ('matome_senni_2507.csv')
    )
    REJECT LIMIT UNLIMITED
    ;


CREATE TABLE matome_tap_2507(
    shop_id NUMBER(10),
    shop_name VARCHAR2(400),
    shop_area VARCHAR2(200),
    shop_biz VARCHAR2(200),
    lp VARCHAR2(400),
    tap NUMBER(4)
    )
    ORGANIZATION EXTERNAL
    (TYPE ORACLE_LOADER
    DEFAULT DIRECTORY temp_dir
    ACCESS PARAMETERS
        (RECORDS DELIMITED BY NEWLINE
        SKIP 1
        BADFILE 'matome_tap_2507.bad'
        LOGFILE 'matome_tap_2507.log'
        CHARACTERSET AL32UTF8
        FIELDS TERMINATED BY ','
        OPTIONALLY ENCLOSED BY '"'
            (
            shop_id FLOAT EXTERNAL,
            shop_name CHAR(400),
            shop_area CHAR(200),
            shop_biz CHAR(200),
            lp CHAR(400),
            tap FLOAT EXTERNAL
            )
        )
    LOCATION ('matome_tap_2507.csv')
    )
    REJECT LIMIT UNLIMITED
    ;


CREATE TABLE matome_page_2507(
    ページパス VARCHAR2(300),
    クリック数 NUMBER(6),
    表示回数 NUMBER(6),
    CTR VARCHAR2(20),
    掲載順位 NUMBER(5,2)
    )
    ORGANIZATION EXTERNAL
    (TYPE ORACLE_LOADER
    DEFAULT DIRECTORY temp_dir
    ACCESS PARAMETERS
        (RECORDS DELIMITED BY NEWLINE
        SKIP 1
        BADFILE 'matome_page_2507.bad'
        LOGFILE 'matome_page_2507.log'
        CHARACTERSET AL32UTF8
        FIELDS TERMINATED BY ','
        OPTIONALLY ENCLOSED BY '"'
            (
            ページパス CHAR(300),
            クリック数 FLOAT EXTERNAL,
            表示回数 FLOAT EXTERNAL,
            CTR CHAR(20),
            掲載順位 FLOAT EXTERNAL
            )
        )
    LOCATION ('matome_page_2507.csv')
    )
    REJECT LIMIT UNLIMITED
    ;


WITH t_ss AS(
    SELECT
        COALESCE(ss7.ページパス, ss6.ページパス) AS ページパス,
        NVL(ss6.流入2506, 0) AS 流入2506,
        NVL(ss7.流入2507, 0) AS 流入2507
        FROM(
            SELECT ページパス, SUM(イベント数) AS 流入2506
                FROM ga_matome_traffic_2506
                GROUP BY ページパス
            ) ss6
            FULL OUTER JOIN(
            SELECT ページパス, SUM(イベント数) AS 流入2507
                FROM ga_matome_traffic_2507
                GROUP BY ページパス
            ) ss7
            ON ss6.ページパス = ss7.ページパス
        ),
 t_cv AS(
    SELECT
        COALESCE(cv7.lp, cv6.lp) AS ランディングページ,
        NVL(cv6.反響2506, 0) AS 反響2506,
        NVL(cv7.反響2507, 0) AS 反響2507
        FROM(
            SELECT lp, SUM(tap) AS 反響2506
                FROM ga_matome_tap_2506
                GROUP BY lp
            ) cv6
            FULL OUTER JOIN(
            SELECT lp, SUM(tap) AS 反響2507
                FROM ga_matome_tap_2507
                GROUP BY lp
            ) cv7
            ON cv6.lp = cv7.lp
        )
    SELECT
        COALESCE(t_ss.ページパス, t_cv.ランディングページ) AS ページ,
        NVL(t_ss.流入2506, 0) AS "6月流入",
        NVL(t_ss.流入2507, 0) AS "7月流入",
        NVL(t_ss.流入2507, 0) - NVL(t_ss.流入2506, 0) AS "流入差",
        NVL(t_cv.反響2506, 0) AS "6月反響",
        NVL(t_cv.反響2507, 0) AS "7月反響",
        NVL(t_cv.反響2507, 0) - NVL(t_cv.反響2506, 0) AS "反響差"
            FROM t_ss FULL OUTER JOIN t_cv
            ON t_ss.ページパス = t_cv.ランディングページ
            -- WHERE ページパス = '/matome/article21647/'
            -- ORDER BY 反響差 DESC
            ;


/*風おす*/
CREATE TABLE fuosu_traffic_2507(
    イベント名 CHAR(50),
    ページタイトル VARCHAR2(400),
    ページパス VARCHAR2(200),
    イベント数 NUMBER(5)
    )
    ORGANIZATION EXTERNAL
    (TYPE ORACLE_LOADER
    DEFAULT DIRECTORY temp_dir
    ACCESS PARAMETERS
        (RECORDS DELIMITED BY NEWLINE
        SKIP 1
        BADFILE 'fuosu_traffic_2507.bad'
        LOGFILE 'fuosu_traffic_2507.log'
        CHARACTERSET AL32UTF8
        FIELDS TERMINATED BY ','
        OPTIONALLY ENCLOSED BY '"'
            (
            イベント名 CHAR(50),
            ページタイトル CHAR(400),
            ページパス CHAR(200),
            イベント数 FLOAT EXTERNAL
            )
        )
    LOCATION ('fuosu_traffic_2507.csv')
    )
    REJECT LIMIT UNLIMITED
    ;


CREATE TABLE fuosu_senni_2507(
    イベント名 VARCHAR(12),
    ページタイトル VARCHAR2(300),
    ページパス VARCHAR2(200),
    イベント数 NUMBER(5)
    )
    ORGANIZATION EXTERNAL
    (TYPE ORACLE_LOADER
    DEFAULT DIRECTORY temp_dir
    ACCESS PARAMETERS
        (RECORDS DELIMITED BY NEWLINE
        SKIP 1
        BADFILE 'fuosu_senni_2507.bad'
        LOGFILE 'fuosu_senni_2507.log'
        CHARACTERSET AL32UTF8
        FIELDS TERMINATED BY ','
        OPTIONALLY ENCLOSED BY '"'
            (
            イベント名 CHAR(12),
            ページタイトル CHAR(300),
            ページパス CHAR(200),
            イベント数 FLOAT EXTERNAL
            )
        )
    LOCATION ('fuosu_senni_2507.csv')
    )
    REJECT LIMIT UNLIMITED
    ;


CREATE TABLE fuosu_page_2507(
    ページパス VARCHAR2(200),
    クリック数 NUMBER(6),
    表示回数 NUMBER(6),
    CTR VARCHAR2(20),
    掲載順位 NUMBER(5,2)
    )
    ORGANIZATION EXTERNAL
    (TYPE ORACLE_LOADER
    DEFAULT DIRECTORY temp_dir
    ACCESS PARAMETERS
        (RECORDS DELIMITED BY NEWLINE
        SKIP 1
        BADFILE 'fuosu_page_2507.bad'
        LOGFILE 'fuosu_page_2507.log'
        CHARACTERSET AL32UTF8
        FIELDS TERMINATED BY ','
        OPTIONALLY ENCLOSED BY '"'
            (
            ページパス CHAR(200),
            クリック数 FLOAT EXTERNAL,
            表示回数 FLOAT EXTERNAL,
            CTR CHAR(20),
            掲載順位 FLOAT EXTERNAL
            )
        )
    LOCATION ('fuosu_page_2507.csv')
    )
    REJECT LIMIT UNLIMITED
    ;


WITH t_ss AS(
    SELECT
        COALESCE(ss7.ページパス, ss6.ページパス) AS ページパス,
        NVL(ss6.流入2506, 0) AS 流入2506,
        NVL(ss7.流入2507, 0) AS 流入2507
        FROM(
            SELECT ページパス, SUM(イベント数) AS 流入2506
                FROM ga_fuosu_traffic_2506
                GROUP BY ページパス
            ) ss6
            FULL OUTER JOIN(
            SELECT ページパス, SUM(イベント数) AS 流入2507
                FROM ga_fuosu_traffic_2507
                GROUP BY ページパス
            ) ss7
            ON ss6.ページパス = ss7.ページパス
        ),
 t_senni AS(
    SELECT
        COALESCE(senni7.ページパス, senni6.ページパス) AS ページパス,
        NVL(senni6.遷移2506, 0) AS 遷移2506,
        NVL(senni7.遷移2507, 0) AS 遷移2507
        FROM(
            SELECT ページパス, SUM(イベント数) AS 遷移2506
                FROM ga_fuosu_senni_2506
                GROUP BY ページパス
            ) senni6
            FULL OUTER JOIN(
            SELECT ページパス, SUM(イベント数) AS 遷移2507
                FROM ga_fuosu_senni_2507
                GROUP BY ページパス
            ) senni7
            ON senni6.ページパス = senni7.ページパス
        )
    SELECT
        COALESCE(t_ss.ページパス, t_senni.ページパス) AS ページ,
        NVL(t_ss.流入2506, 0) AS "6月流入",
        NVL(t_ss.流入2507, 0) AS "7月流入",
        NVL(t_ss.流入2507, 0) - NVL(t_ss.流入2506, 0) AS "流入差",
        NVL(t_senni.遷移2506, 0) AS "6月遷移",
        NVL(t_senni.遷移2507, 0) AS "7月遷移",
        NVL(t_senni.遷移2507, 0) - NVL(t_senni.遷移2506, 0) AS "遷移差"
            FROM t_ss FULL OUTER JOIN t_senni
            ON t_ss.ページパス = t_senni.ページパス
            -- WHERE t_ss.ページパス = '/3210/'
            -- ORDER BY 遷移差 DESC
            ;


/*デリおす*/
CREATE TABLE deliosu_traffic_2508(
    イベント名 CHAR(20),
    ページタイトル VARCHAR2(200),
    ページパス VARCHAR2(200),
    イベント数 NUMBER(4)
    )
    ORGANIZATION EXTERNAL
    (TYPE ORACLE_LOADER
    DEFAULT DIRECTORY temp_dir
    ACCESS PARAMETERS
        (RECORDS DELIMITED BY NEWLINE
        SKIP 1
        BADFILE 'deliosu_traffic_2508.bad'
        LOGFILE 'deliosu_traffic_2508.log'
        CHARACTERSET AL32UTF8
        FIELDS TERMINATED BY ','
        OPTIONALLY ENCLOSED BY '"'
            (
            イベント名 CHAR(20),
            ページタイトル CHAR(200),
            ページパス CHAR(200),
            イベント数 FLOAT EXTERNAL
            )
        )
    LOCATION ('deliosu_traffic_2508.csv')
    )
    REJECT LIMIT UNLIMITED
    ;


CREATE TABLE deliosu_senni_2508(
    イベント名 VARCHAR(40),
    ページタイトル VARCHAR2(200),
    ページパス VARCHAR2(200),
    イベント数 NUMBER(5)
    )
    ORGANIZATION EXTERNAL
    (TYPE ORACLE_LOADER
    DEFAULT DIRECTORY temp_dir
    ACCESS PARAMETERS
        (RECORDS DELIMITED BY NEWLINE
        SKIP 1
        BADFILE 'deliosu_senni_2508.bad'
        LOGFILE 'deliosu_senni_2508.log'
        CHARACTERSET AL32UTF8
        FIELDS TERMINATED BY ','
        OPTIONALLY ENCLOSED BY '"'
            (
            イベント名 CHAR(40),
            ページタイトル CHAR(200),
            ページパス CHAR(200),
            イベント数 FLOAT EXTERNAL
            )
        )
    LOCATION ('deliosu_senni_2508.csv')
    )
    REJECT LIMIT UNLIMITED
    ;


CREATE TABLE deliosu_page_2508(
    ページパス VARCHAR2(200),
    クリック数 NUMBER(5),
    表示回数 NUMBER(5),
    CTR VARCHAR2(20),
    掲載順位 NUMBER(5,2)
    )
    ORGANIZATION EXTERNAL
    (TYPE ORACLE_LOADER
    DEFAULT DIRECTORY temp_dir
    ACCESS PARAMETERS
        (RECORDS DELIMITED BY NEWLINE
        SKIP 1
        BADFILE 'deliosu_page_2508.bad'
        LOGFILE 'deliosu_page_2508.log'
        CHARACTERSET AL32UTF8
        FIELDS TERMINATED BY ','
        OPTIONALLY ENCLOSED BY '"'
            (
            ページパス CHAR(200),
            クリック数 FLOAT EXTERNAL,
            表示回数 FLOAT EXTERNAL,
            CTR CHAR(20),
            掲載順位 FLOAT EXTERNAL
            )
        )
    LOCATION ('deliosu_page_2508.csv')
    )
    REJECT LIMIT UNLIMITED
    ;


WITH t_ss AS(
    SELECT
        COALESCE(ss8.ページパス, ss7.ページパス) AS ページパス,
        NVL(ss7.流入2507, 0) AS 流入2507,
        NVL(ss8.流入2508, 0) AS 流入2508
        FROM(
            SELECT ページパス, SUM(イベント数) AS 流入2507
                FROM ga_deliosu_traffic_2507
                GROUP BY ページパス
            ) ss7
            FULL OUTER JOIN(
            SELECT ページパス, SUM(イベント数) AS 流入2508
                FROM ga_deliosu_traffic_2508
                GROUP BY ページパス
            ) ss8
            ON ss7.ページパス = ss8.ページパス
        ),
 t_senni AS(
    SELECT
        COALESCE(senni8.ページパス, senni7.ページパス) AS ページパス,
        NVL(senni7.遷移2507, 0) AS 遷移2507,
        NVL(senni8.遷移2508, 0) AS 遷移2508
        FROM(
            SELECT ページパス, SUM(イベント数) AS 遷移2507
                FROM ga_deliosu_senni_2507
                GROUP BY ページパス
            ) senni7
            FULL OUTER JOIN(
            SELECT ページパス, SUM(イベント数) AS 遷移2508
                FROM ga_deliosu_senni_2508
                GROUP BY ページパス
            ) senni8
            ON senni7.ページパス = senni8.ページパス
        )
    SELECT
        COALESCE(t_ss.ページパス, t_senni.ページパス) AS ページ,
        NVL(t_ss.流入2507, 0) AS "7月流入",
        NVL(t_ss.流入2508, 0) AS "8月流入",
        NVL(t_ss.流入2508, 0) - NVL(t_ss.流入2507, 0) AS "流入差",
        NVL(t_senni.遷移2507, 0) AS "7月遷移",
        NVL(t_senni.遷移2508, 0) AS "8月遷移",
        NVL(t_senni.遷移2508, 0) - NVL(t_senni.遷移2507, 0) AS "遷移差"
            FROM t_ss FULL OUTER JOIN t_senni
            ON t_ss.ページパス = t_senni.ページパス
            -- WHERE t_ss.ページパス = '/miyagi/sendai/'
            -- ORDER BY 遷移差 DESC
            ;


/*メンマガ*/
CREATE TABLE menmaga_traffic_2508(
    イベント名 CHAR(50),
    ページタイトル VARCHAR2(400),
    ページパス VARCHAR2(200),
    イベント数 NUMBER(5)
    )
    ORGANIZATION EXTERNAL
    (TYPE ORACLE_LOADER
    DEFAULT DIRECTORY temp_dir
    ACCESS PARAMETERS
        (RECORDS DELIMITED BY NEWLINE
        SKIP 1
        BADFILE 'menmaga_traffic_2508.bad'
        LOGFILE 'menmaga_traffic_2508.log'
        CHARACTERSET AL32UTF8
        FIELDS TERMINATED BY ','
        OPTIONALLY ENCLOSED BY '"'
            (
            イベント名 CHAR(50),
            ページタイトル CHAR(400),
            ページパス CHAR(200),
            イベント数 FLOAT EXTERNAL
            )
        )
    LOCATION ('menmaga_traffic_2508.csv')
    )
    REJECT LIMIT UNLIMITED
    ;


CREATE TABLE menmaga_senni_2508(
    イベント名 VARCHAR(100),
    ページタイトル VARCHAR2(400),
    ページパス VARCHAR2(300),
    イベント数 NUMBER(5)
    )
    ORGANIZATION EXTERNAL
    (TYPE ORACLE_LOADER
    DEFAULT DIRECTORY temp_dir
    ACCESS PARAMETERS
        (RECORDS DELIMITED BY NEWLINE
        SKIP 1
        BADFILE 'menmaga_senni_2508.bad'
        LOGFILE 'menmaga_senni_2508.log'
        CHARACTERSET AL32UTF8
        FIELDS TERMINATED BY ','
        OPTIONALLY ENCLOSED BY '"'
            (
            イベント名 CHAR(100),
            ページタイトル CHAR(400),
            ページパス CHAR(300),
            イベント数 FLOAT EXTERNAL
            )
        )
    LOCATION ('menmaga_senni_2508.csv')
    )
    REJECT LIMIT UNLIMITED
    ;


CREATE TABLE menmaga_tap_2508(
    shop_id NUMBER(10),
    shop_name VARCHAR2(200),
    shop_area VARCHAR2(200),
    lp VARCHAR2(400),
    tap NUMBER(4)
    )
    ORGANIZATION EXTERNAL
    (TYPE ORACLE_LOADER
    DEFAULT DIRECTORY temp_dir
    ACCESS PARAMETERS
        (RECORDS DELIMITED BY NEWLINE
        SKIP 1
        BADFILE 'menmaga_tap_2508.bad'
        LOGFILE 'menmaga_tap_2508.log'
        CHARACTERSET AL32UTF8
        FIELDS TERMINATED BY ','
        OPTIONALLY ENCLOSED BY '"'
            (
            shop_id FLOAT EXTERNAL,
            shop_name CHAR(200),
            shop_area CHAR(200),
            lp CHAR(400),
            tap FLOAT EXTERNAL
            )
        )
    LOCATION ('menmaga_tap_2508.csv')
    )
    REJECT LIMIT UNLIMITED
    ;


CREATE TABLE menmaga_page_2508(
    ページパス VARCHAR2(300),
    クリック数 NUMBER(6),
    表示回数 NUMBER(6),
    CTR VARCHAR2(20),
    掲載順位 NUMBER(5,2)
    )
    ORGANIZATION EXTERNAL
    (TYPE ORACLE_LOADER
    DEFAULT DIRECTORY temp_dir
    ACCESS PARAMETERS
        (RECORDS DELIMITED BY NEWLINE
        SKIP 1
        BADFILE 'menmaga_page_2508.bad'
        LOGFILE 'menmaga_page_2508.log'
        CHARACTERSET AL32UTF8
        FIELDS TERMINATED BY ','
        OPTIONALLY ENCLOSED BY '"'
            (
            ページパス CHAR(300),
            クリック数 FLOAT EXTERNAL,
            表示回数 FLOAT EXTERNAL,
            CTR CHAR(20),
            掲載順位 FLOAT EXTERNAL
            )
        )
    LOCATION ('menmaga_page_2508.csv')
    )
    REJECT LIMIT UNLIMITED
    ;


WITH t_ss AS(
    SELECT
        COALESCE(ss8.ページパス, ss7.ページパス) AS ページパス,
        NVL(ss7.流入2507, 0) AS 流入2507,
        NVL(ss8.流入2508, 0) AS 流入2508
        FROM(
            SELECT ページパス, SUM(イベント数) AS 流入2507
                FROM ga_menmaga_traffic_2507
                GROUP BY ページパス
            ) ss7
            FULL OUTER JOIN(
            SELECT ページパス, SUM(イベント数) AS 流入2508
                FROM ga_menmaga_traffic_2508
                GROUP BY ページパス
            ) ss8
            ON ss7.ページパス = ss8.ページパス
        ),
 t_cv AS(
    SELECT
        COALESCE(cv8.lp, cv7.lp) AS ランディングページ,
        NVL(cv7.反響2507, 0) AS 反響2507,
        NVL(cv8.反響2508, 0) AS 反響2508
        FROM(
            SELECT lp, SUM(tap) AS 反響2507
                FROM ga_menmaga_tap_2507
                GROUP BY lp
            ) cv7
            FULL OUTER JOIN(
            SELECT lp, SUM(tap) AS 反響2508
                FROM ga_menmaga_tap_2508
                GROUP BY lp
            ) cv8
            ON cv7.lp = cv8.lp
        )
    SELECT
        COALESCE(t_ss.ページパス, t_cv.ランディングページ) AS ページ,
        NVL(t_ss.流入2507, 0) AS "7月流入",
        NVL(t_ss.流入2508, 0) AS "8月流入",
        NVL(t_ss.流入2508, 0) - NVL(t_ss.流入2507, 0) AS "流入差",
        NVL(t_cv.反響2507, 0) AS "7月ネット予約",
        NVL(t_cv.反響2508, 0) AS "8月ネット予約",
        NVL(t_cv.反響2508, 0) - NVL(t_cv.反響2507, 0) AS "反響差"
            FROM t_ss FULL OUTER JOIN t_cv
            ON t_ss.ページパス = t_cv.ランディングページ
            -- WHERE ページパス = '/magazine/article1198/'
            -- ORDER BY 反響差 DESC
            ;


/*ココミル*/
CREATE TABLE cocomiru_traffic_2508(
    イベント名 CHAR(50),
    ページタイトル VARCHAR2(300),
    ページパス VARCHAR2(200),
    イベント数 NUMBER(5)
    )
    ORGANIZATION EXTERNAL
    (TYPE ORACLE_LOADER
    DEFAULT DIRECTORY temp_dir
    ACCESS PARAMETERS
        (RECORDS DELIMITED BY NEWLINE
        SKIP 1
        BADFILE 'cocomiru_traffic_2508.bad'
        LOGFILE 'cocomiru_traffic_2508.log'
        CHARACTERSET AL32UTF8
        FIELDS TERMINATED BY ','
        OPTIONALLY ENCLOSED BY '"'
            (
            イベント名 CHAR(50),
            ページタイトル CHAR(300),
            ページパス CHAR(200),
            イベント数 FLOAT EXTERNAL
            )
        )
    LOCATION ('cocomiru_traffic_2508.csv')
    )
    REJECT LIMIT UNLIMITED
    ;


CREATE TABLE cocomiru_senni_2508(
    イベント名 VARCHAR(50),
    ページタイトル VARCHAR2(300),
    ページパス VARCHAR2(200),
    イベント数 NUMBER(5)
    )
    ORGANIZATION EXTERNAL
    (TYPE ORACLE_LOADER
    DEFAULT DIRECTORY temp_dir
    ACCESS PARAMETERS
        (RECORDS DELIMITED BY NEWLINE
        SKIP 1
        BADFILE 'cocomiru_senni_2508.bad'
        LOGFILE 'cocomiru_senni_2508.log'
        CHARACTERSET AL32UTF8
        FIELDS TERMINATED BY ','
        OPTIONALLY ENCLOSED BY '"'
            (
            イベント名 CHAR(50),
            ページタイトル CHAR(300),
            ページパス CHAR(200),
            イベント数 FLOAT EXTERNAL
            )
        )
    LOCATION ('cocomiru_senni_2508.csv')
    )
    REJECT LIMIT UNLIMITED
    ;


CREATE TABLE cocomiru_tap_2508(
    shop_id NUMBER(10),
    shop_name VARCHAR2(200),
    shop_area VARCHAR2(100),
    shop_biz VARCHAR2(100),
    lp VARCHAR2(200),
    tap NUMBER(4)
    )
    ORGANIZATION EXTERNAL
    (TYPE ORACLE_LOADER
    DEFAULT DIRECTORY temp_dir
    ACCESS PARAMETERS
        (RECORDS DELIMITED BY NEWLINE
        SKIP 1
        BADFILE 'cocomiru_tap_2508.bad'
        LOGFILE 'cocomiru_tap_2508.log'
        CHARACTERSET AL32UTF8
        FIELDS TERMINATED BY ','
        OPTIONALLY ENCLOSED BY '"'
            (
            shop_id FLOAT EXTERNAL,
            shop_name CHAR(200),
            shop_area CHAR(100),
            shop_biz CHAR(100),
            lp CHAR(200),
            tap FLOAT EXTERNAL
            )
        )
    LOCATION ('cocomiru_tap_2508.csv')
    )
    REJECT LIMIT UNLIMITED
    ;


CREATE TABLE cocomiru_page_2508(
    ページパス VARCHAR2(200),
    クリック数 NUMBER(6),
    表示回数 NUMBER(6),
    CTR VARCHAR2(20),
    掲載順位 NUMBER(5,2)
    )
    ORGANIZATION EXTERNAL
    (TYPE ORACLE_LOADER
    DEFAULT DIRECTORY temp_dir
    ACCESS PARAMETERS
        (RECORDS DELIMITED BY NEWLINE
        SKIP 1
        BADFILE 'cocomiru_page_2508.bad'
        LOGFILE 'cocomiru_page_2508.log'
        CHARACTERSET AL32UTF8
        FIELDS TERMINATED BY ','
        OPTIONALLY ENCLOSED BY '"'
            (
            ページパス CHAR(200),
            クリック数 FLOAT EXTERNAL,
            表示回数 FLOAT EXTERNAL,
            CTR CHAR(20),
            掲載順位 FLOAT EXTERNAL
            )
        )
    LOCATION ('cocomiru_page_2508.csv')
    )
    REJECT LIMIT UNLIMITED
    ;


WITH t_ss AS(
    SELECT
        COALESCE(ss8.ページパス, ss7.ページパス) AS ページパス,
        NVL(ss7.流入2507, 0) AS 流入2507,
        NVL(ss8.流入2508, 0) AS 流入2508
        FROM(
            SELECT ページパス, SUM(イベント数) AS 流入2507
                FROM ga_cocomiru_traffic_2507
                GROUP BY ページパス
            ) ss7
            FULL OUTER JOIN(
            SELECT ページパス, SUM(イベント数) AS 流入2508
                FROM ga_cocomiru_traffic_2508
                GROUP BY ページパス
            ) ss8
            ON ss7.ページパス = ss8.ページパス
        ),
 t_cv AS(
    SELECT
        COALESCE(cv8.lp, cv7.lp) AS ランディングページ,
        NVL(cv7.反響2507, 0) AS 反響2507,
        NVL(cv8.反響2508, 0) AS 反響2508
        FROM(
            SELECT lp, SUM(tap) AS 反響2507
                FROM ga_cocomiru_tap_2507
                GROUP BY lp
            ) cv7
            FULL OUTER JOIN(
            SELECT lp, SUM(tap) AS 反響2508
                FROM ga_cocomiru_tap_2508
                GROUP BY lp
            ) cv8
            ON cv7.lp = cv8.lp
        )
    SELECT
        COALESCE(t_ss.ページパス, t_cv.ランディングページ) AS ページ,
        NVL(t_ss.流入2507, 0) AS "7月流入",
        NVL(t_ss.流入2508, 0) AS "8月流入",
        NVL(t_ss.流入2508, 0) - NVL(t_ss.流入2507, 0) AS "流入差",
        NVL(t_cv.反響2507, 0) AS "7月応募A",
        NVL(t_cv.反響2508, 0) AS "8月応募A",
        NVL(t_cv.反響2508, 0) - NVL(t_cv.反響2507, 0) AS "反響差"
            FROM t_ss FULL OUTER JOIN t_cv
            ON t_ss.ページパス = t_cv.ランディングページ
            -- WHERE ページパス = '/column/18926/'
            -- ORDER BY 反響差 DESC
            ;


/*ジョシミル*/
CREATE TABLE joshimiru_traffic_2508(
    イベント名 CHAR(50),
    ページタイトル VARCHAR2(200),
    ページパス VARCHAR2(100),
    イベント数 NUMBER(5)
    )
    ORGANIZATION EXTERNAL
    (TYPE ORACLE_LOADER
    DEFAULT DIRECTORY temp_dir
    ACCESS PARAMETERS
        (RECORDS DELIMITED BY NEWLINE
        SKIP 1
        BADFILE 'joshimiru_traffic_2508.bad'
        LOGFILE 'joshimiru_traffic_2508.log'
        CHARACTERSET AL32UTF8
        FIELDS TERMINATED BY ','
        OPTIONALLY ENCLOSED BY '"'
            (
            イベント名 CHAR(50),
            ページタイトル CHAR(200),
            ページパス CHAR(100),
            イベント数 FLOAT EXTERNAL
            )
        )
    LOCATION ('joshimiru_traffic_2508.csv')
    )
    REJECT LIMIT UNLIMITED
    ;


CREATE TABLE joshimiru_senni_2508(
    イベント名 VARCHAR(50),
    ページタイトル VARCHAR2(200),
    ページパス VARCHAR2(100),
    イベント数 NUMBER(5)
    )
    ORGANIZATION EXTERNAL
    (TYPE ORACLE_LOADER
    DEFAULT DIRECTORY temp_dir
    ACCESS PARAMETERS
        (RECORDS DELIMITED BY NEWLINE
        SKIP 1
        BADFILE 'joshimiru_senni_2508.bad'
        LOGFILE 'joshimiru_senni_2508.log'
        CHARACTERSET AL32UTF8
        FIELDS TERMINATED BY ','
        OPTIONALLY ENCLOSED BY '"'
            (
            イベント名 CHAR(50),
            ページタイトル CHAR(200),
            ページパス CHAR(100),
            イベント数 FLOAT EXTERNAL
            )
        )
    LOCATION ('joshimiru_senni_2508.csv')
    )
    REJECT LIMIT UNLIMITED
    ;


CREATE TABLE joshimiru_page_2508(
    ページパス VARCHAR2(200),
    クリック数 NUMBER(7),
    表示回数 NUMBER(7),
    CTR VARCHAR2(20),
    掲載順位 NUMBER(5,2)
    )
    ORGANIZATION EXTERNAL
    (TYPE ORACLE_LOADER
    DEFAULT DIRECTORY temp_dir
    ACCESS PARAMETERS
        (RECORDS DELIMITED BY NEWLINE
        SKIP 1
        BADFILE 'joshimiru_page_2508.bad'
        LOGFILE 'joshimiru_page_2508.log'
        CHARACTERSET AL32UTF8
        FIELDS TERMINATED BY ','
        OPTIONALLY ENCLOSED BY '"'
            (
            ページパス CHAR(200),
            クリック数 FLOAT EXTERNAL,
            表示回数 FLOAT EXTERNAL,
            CTR CHAR(20),
            掲載順位 FLOAT EXTERNAL
            )
        )
    LOCATION ('joshimiru_page_2508.csv')
    )
    REJECT LIMIT UNLIMITED
    ;


WITH t_ss AS(
    SELECT
        COALESCE(ss8.ページパス, ss7.ページパス) AS ページパス,
        NVL(ss7.流入2507, 0) AS 流入2507,
        NVL(ss8.流入2508, 0) AS 流入2508
        FROM(
            SELECT ページパス, SUM(イベント数) AS 流入2507
                FROM ga_joshimiru_traffic_2507
                GROUP BY ページパス
            ) ss7
            FULL OUTER JOIN(
            SELECT ページパス, SUM(イベント数) AS 流入2508
                FROM ga_joshimiru_traffic_2508
                GROUP BY ページパス
            ) ss8
            ON ss7.ページパス = ss8.ページパス
        ),
 t_senni AS(
    SELECT
        COALESCE(senni8.ページパス, senni7.ページパス) AS ページパス,
        NVL(senni7.遷移2507, 0) AS 遷移2507,
        NVL(senni8.遷移2508, 0) AS 遷移2508
        FROM(
            SELECT ページパス, SUM(イベント数) AS 遷移2507
                FROM ga_joshimiru_senni_2507
                GROUP BY ページパス
            ) senni7
            FULL OUTER JOIN(
            SELECT ページパス, SUM(イベント数) AS 遷移2508
                FROM ga_joshimiru_senni_2508
                GROUP BY ページパス
            ) senni8
            ON senni7.ページパス = senni8.ページパス
        )
    SELECT
        COALESCE(t_ss.ページパス, t_senni.ページパス) AS ページ,
        NVL(t_ss.流入2507, 0) AS "7月流入",
        NVL(t_ss.流入2508, 0) AS "8月流入",
        NVL(t_ss.流入2508, 0) - NVL(t_ss.流入2507, 0) AS "流入差",
        NVL(t_senni.遷移2507, 0) AS "7月遷移",
        NVL(t_senni.遷移2508, 0) AS "8月遷移",
        NVL(t_senni.遷移2508, 0) - NVL(t_senni.遷移2507, 0) AS "遷移差"
            FROM t_ss FULL OUTER JOIN t_senni
            ON t_ss.ページパス = t_senni.ページパス
            -- WHERE t_ss.ページパス = '/17042/'
            -- ORDER BY 遷移差 DESC
            ;


/*リラマガ*/
CREATE TABLE riramaga_traffic_2508(
    イベント名 CHAR(50),
    ページタイトル VARCHAR2(300),
    ページパス VARCHAR2(200),
    イベント数 NUMBER(5)
    )
    ORGANIZATION EXTERNAL
    (TYPE ORACLE_LOADER
    DEFAULT DIRECTORY temp_dir
    ACCESS PARAMETERS
        (RECORDS DELIMITED BY NEWLINE
        SKIP 1
        BADFILE 'riramaga_traffic_2508.bad'
        LOGFILE 'riramaga_traffic_2508.log'
        CHARACTERSET AL32UTF8
        FIELDS TERMINATED BY ','
        OPTIONALLY ENCLOSED BY '"'
            (
            イベント名 CHAR(50),
            ページタイトル CHAR(300),
            ページパス CHAR(200),
            イベント数 FLOAT EXTERNAL
            )
        )
    LOCATION ('riramaga_traffic_2508.csv')
    )
    REJECT LIMIT UNLIMITED
    ;


CREATE TABLE riramaga_senni_2508(
    イベント名 VARCHAR(50),
    ページタイトル VARCHAR2(200),
    ページパス VARCHAR2(100),
    イベント数 NUMBER(5)
    )
    ORGANIZATION EXTERNAL
    (TYPE ORACLE_LOADER
    DEFAULT DIRECTORY temp_dir
    ACCESS PARAMETERS
        (RECORDS DELIMITED BY NEWLINE
        SKIP 1
        BADFILE 'riramaga_senni_2508.bad'
        LOGFILE 'riramaga_senni_2508.log'
        CHARACTERSET AL32UTF8
        FIELDS TERMINATED BY ','
        OPTIONALLY ENCLOSED BY '"'
            (
            イベント名 CHAR(50),
            ページタイトル CHAR(200),
            ページパス CHAR(100),
            イベント数 FLOAT EXTERNAL
            )
        )
    LOCATION ('riramaga_senni_2508.csv')
    )
    REJECT LIMIT UNLIMITED
    ;


CREATE TABLE riramaga_tap_2508(
    shop_id NUMBER(10),
    shop_name VARCHAR2(200),
    shop_area VARCHAR2(100),
    lp VARCHAR2(200),
    tap NUMBER(4)
    )
    ORGANIZATION EXTERNAL
    (TYPE ORACLE_LOADER
    DEFAULT DIRECTORY temp_dir
    ACCESS PARAMETERS
        (RECORDS DELIMITED BY NEWLINE
        SKIP 1
        BADFILE 'riramaga_tap_2508.bad'
        LOGFILE 'riramaga_tap_2508.log'
        CHARACTERSET AL32UTF8
        FIELDS TERMINATED BY ','
        OPTIONALLY ENCLOSED BY '"'
            (
            shop_id FLOAT EXTERNAL,
            shop_name CHAR(200),
            shop_area CHAR(100),
            lp CHAR(200),
            tap FLOAT EXTERNAL
            )
        )
    LOCATION ('riramaga_tap_2508.csv')
    )
    REJECT LIMIT UNLIMITED
    ;


CREATE TABLE riramaga_page_2508(
    ページパス VARCHAR2(200),
    クリック数 NUMBER(5),
    表示回数 NUMBER(6),
    CTR VARCHAR2(20),
    掲載順位 NUMBER(5,2)
    )
    ORGANIZATION EXTERNAL
    (TYPE ORACLE_LOADER
    DEFAULT DIRECTORY temp_dir
    ACCESS PARAMETERS
        (RECORDS DELIMITED BY NEWLINE
        SKIP 1
        BADFILE 'riramaga_page_2508.bad'
        LOGFILE 'riramaga_page_2508.log'
        CHARACTERSET AL32UTF8
        FIELDS TERMINATED BY ','
        OPTIONALLY ENCLOSED BY '"'
            (
            ページパス CHAR(200),
            クリック数 FLOAT EXTERNAL,
            表示回数 FLOAT EXTERNAL,
            CTR CHAR(20),
            掲載順位 FLOAT EXTERNAL
            )
        )
    LOCATION ('riramaga_page_2508.csv')
    )
    REJECT LIMIT UNLIMITED
    ;


WITH t_ss AS(
    SELECT
        COALESCE(ss8.ページパス, ss7.ページパス) AS ページパス,
        NVL(ss7.流入2507, 0) AS 流入2507,
        NVL(ss8.流入2508, 0) AS 流入2508
        FROM(
            SELECT ページパス, SUM(イベント数) AS 流入2507
                FROM ga_riramaga_traffic_2507
                GROUP BY ページパス
            ) ss7
            FULL OUTER JOIN(
            SELECT ページパス, SUM(イベント数) AS 流入2508
                FROM ga_riramaga_traffic_2508
                GROUP BY ページパス
            ) ss8
            ON ss7.ページパス = ss8.ページパス
        ),
 t_cv AS(
    SELECT
        COALESCE(cv8.lp, cv7.lp) AS ランディングページ,
        NVL(cv7.反響2507, 0) AS 反響2507,
        NVL(cv8.反響2508, 0) AS 反響2508
        FROM(
            SELECT lp, SUM(tap) AS 反響2507
                FROM ga_riramaga_tap_2507
                GROUP BY lp
            ) cv7
            FULL OUTER JOIN(
            SELECT lp, SUM(tap) AS 反響2508
                FROM ga_riramaga_tap_2508
                GROUP BY lp
            ) cv8
            ON cv7.lp = cv8.lp
        )
    SELECT
        COALESCE(t_ss.ページパス, t_cv.ランディングページ) AS ページ,
        NVL(t_ss.流入2507, 0) AS "7月流入",
        NVL(t_ss.流入2508, 0) AS "8月流入",
        NVL(t_ss.流入2508, 0) - NVL(t_ss.流入2507, 0) AS "流入差",
        NVL(t_cv.反響2507, 0) AS "7月STARTING_POINT",
        NVL(t_cv.反響2508, 0) AS "8月STARTING_POINT",
        NVL(t_cv.反響2508, 0) - NVL(t_cv.反響2507, 0) AS "反響差"
            FROM t_ss FULL OUTER JOIN t_cv
            ON t_ss.ページパス = t_cv.ランディングページ
            -- WHERE ページパス = '/magazine/how-to-work/2768/'
            -- ORDER BY 反響差 DESC
            ;


/*ホスパラnavi*/
CREATE TABLE hosuparanavi_traffic_2508(
    イベント名 CHAR(50),
    ページタイトル VARCHAR2(200),
    ページパス VARCHAR2(200),
    イベント数 NUMBER(5)
    )
    ORGANIZATION EXTERNAL
    (TYPE ORACLE_LOADER
    DEFAULT DIRECTORY temp_dir
    ACCESS PARAMETERS
        (RECORDS DELIMITED BY NEWLINE
        SKIP 1
        BADFILE 'hosuparanavi_traffic_2508.bad'
        LOGFILE 'hosuparanavi_traffic_2508.log'
        CHARACTERSET AL32UTF8
        FIELDS TERMINATED BY ','
        OPTIONALLY ENCLOSED BY '"'
            (
            イベント名 CHAR(50),
            ページタイトル CHAR(200),
            ページパス CHAR(200),
            イベント数 FLOAT EXTERNAL
            )
        )
    LOCATION ('hosuparanavi_traffic_2508.csv')
    )
    REJECT LIMIT UNLIMITED
    ;


CREATE TABLE hosuparanavi_senni_2508(
    イベント名 VARCHAR(50),
    ページタイトル VARCHAR2(200),
    ページパス VARCHAR2(200),
    イベント数 NUMBER(5)
    )
    ORGANIZATION EXTERNAL
    (TYPE ORACLE_LOADER
    DEFAULT DIRECTORY temp_dir
    ACCESS PARAMETERS
        (RECORDS DELIMITED BY NEWLINE
        SKIP 1
        BADFILE 'hosuparanavi_senni_2508.bad'
        LOGFILE 'hosuparanavi_senni_2508.log'
        CHARACTERSET AL32UTF8
        FIELDS TERMINATED BY ','
        OPTIONALLY ENCLOSED BY '"'
            (
            イベント名 CHAR(50),
            ページタイトル CHAR(200),
            ページパス CHAR(200),
            イベント数 FLOAT EXTERNAL
            )
        )
    LOCATION ('hosuparanavi_senni_2508.csv')
    )
    REJECT LIMIT UNLIMITED
    ;


CREATE TABLE hosuparanavi_tap_2508(
    shop_id NUMBER(8),
    shop_name VARCHAR2(200),
    shop_area VARCHAR2(100),
    shop_biz VARCHAR2(100),
    lp VARCHAR2(200),
    tap NUMBER(3)
    )
    ORGANIZATION EXTERNAL
    (TYPE ORACLE_LOADER
    DEFAULT DIRECTORY temp_dir
    ACCESS PARAMETERS
        (RECORDS DELIMITED BY NEWLINE
        SKIP 1
        BADFILE 'hosuparanavi_tap_2508.bad'
        LOGFILE 'hosuparanavi_tap_2508.log'
        CHARACTERSET AL32UTF8
        FIELDS TERMINATED BY ','
        OPTIONALLY ENCLOSED BY '"'
            (
            shop_id FLOAT EXTERNAL,
            shop_name CHAR(200),
            shop_area CHAR(100),
            shop_biz CHAR(100),
            lp CHAR(200),
            tap FLOAT EXTERNAL
            )
        )
    LOCATION ('hosuparanavi_tap_2508.csv')
    )
    REJECT LIMIT UNLIMITED
    ;


CREATE TABLE hosuparanavi_page_2508(
    ページパス VARCHAR2(200),
    クリック数 NUMBER(6),
    表示回数 NUMBER(6),
    CTR VARCHAR2(20),
    掲載順位 NUMBER(5,2)
    )
    ORGANIZATION EXTERNAL
    (TYPE ORACLE_LOADER
    DEFAULT DIRECTORY temp_dir
    ACCESS PARAMETERS
        (RECORDS DELIMITED BY NEWLINE
        SKIP 1
        BADFILE 'hosuparanavi_page_2508.bad'
        LOGFILE 'hosuparanavi_page_2508.log'
        CHARACTERSET AL32UTF8
        FIELDS TERMINATED BY ','
        OPTIONALLY ENCLOSED BY '"'
            (
            ページパス CHAR(200),
            クリック数 FLOAT EXTERNAL,
            表示回数 FLOAT EXTERNAL,
            CTR CHAR(20),
            掲載順位 FLOAT EXTERNAL
            )
        )
    LOCATION ('hosuparanavi_page_2508.csv')
    )
    REJECT LIMIT UNLIMITED
    ;


WITH t_ss AS(
    SELECT
        COALESCE(ss8.ページパス, ss7.ページパス) AS ページパス,
        NVL(ss7.流入2507, 0) AS 流入2507,
        NVL(ss8.流入2508, 0) AS 流入2508
        FROM(
            SELECT ページパス, SUM(イベント数) AS 流入2507
                FROM ga_hosuparanavi_traffic_2507
                GROUP BY ページパス
            ) ss7
            FULL OUTER JOIN(
            SELECT ページパス, SUM(イベント数) AS 流入2508
                FROM ga_hosuparanavi_traffic_2508
                GROUP BY ページパス
            ) ss8
            ON ss7.ページパス = ss8.ページパス
        ),
 t_cv AS(
    SELECT
        COALESCE(cv8.lp, cv7.lp) AS ランディングページ,
        NVL(cv7.反響2507, 0) AS 反響2507,
        NVL(cv8.反響2508, 0) AS 反響2508
        FROM(
            SELECT lp, SUM(tap) AS 反響2507
                FROM ga_hosuparanavi_tap_2507
                GROUP BY lp
            ) cv7
            FULL OUTER JOIN(
            SELECT lp, SUM(tap) AS 反響2508
                FROM ga_hosuparanavi_tap_2508
                GROUP BY lp
            ) cv8
            ON cv7.lp = cv8.lp
        )
    SELECT
        COALESCE(t_ss.ページパス, t_cv.ランディングページ) AS ページ,
        NVL(t_ss.流入2507, 0) AS "7月流入",
        NVL(t_ss.流入2508, 0) AS "8月流入",
        NVL(t_ss.流入2508, 0) - NVL(t_ss.流入2507, 0) AS "流入差",
        NVL(t_cv.反響2507, 0) AS "7月starting_point",
        NVL(t_cv.反響2508, 0) AS "8月starting_point",
        NVL(t_cv.反響2508, 0) - NVL(t_cv.反響2507, 0) AS "反響差"
            FROM t_ss FULL OUTER JOIN t_cv
            ON t_ss.ページパス = t_cv.ランディングページ
            -- WHERE ページパス = '/magazine/2517/'
            -- ORDER BY 反響差 DESC
            ;


/*ホスミル*/
CREATE TABLE hosumiru_traffic_2508(
    イベント名 CHAR(50),
    ページタイトル VARCHAR2(200),
    ページパス VARCHAR2(200),
    イベント数 NUMBER(5)
    )
    ORGANIZATION EXTERNAL
    (TYPE ORACLE_LOADER
    DEFAULT DIRECTORY temp_dir
    ACCESS PARAMETERS
        (RECORDS DELIMITED BY NEWLINE
        SKIP 1
        BADFILE 'hosumiru_traffic_2508.bad'
        LOGFILE 'hosumiru_traffic_2508.log'
        CHARACTERSET AL32UTF8
        FIELDS TERMINATED BY ','
        OPTIONALLY ENCLOSED BY '"'
            (
            イベント名 CHAR(50),
            ページタイトル CHAR(200),
            ページパス CHAR(200),
            イベント数 FLOAT EXTERNAL
            )
        )
    LOCATION ('hosumiru_traffic_2508.csv')
    )
    REJECT LIMIT UNLIMITED
    ;


CREATE TABLE hosumiru_senni_2508(
    イベント名 VARCHAR(50),
    ページタイトル VARCHAR2(200),
    ページパス VARCHAR2(200),
    イベント数 NUMBER(5)
    )
    ORGANIZATION EXTERNAL
    (TYPE ORACLE_LOADER
    DEFAULT DIRECTORY temp_dir
    ACCESS PARAMETERS
        (RECORDS DELIMITED BY NEWLINE
        SKIP 1
        BADFILE 'hosumiru_senni_2508.bad'
        LOGFILE 'hosumiru_senni_2508.log'
        CHARACTERSET AL32UTF8
        FIELDS TERMINATED BY ','
        OPTIONALLY ENCLOSED BY '"'
            (
            イベント名 CHAR(50),
            ページタイトル CHAR(200),
            ページパス CHAR(200),
            イベント数 FLOAT EXTERNAL
            )
        )
    LOCATION ('hosumiru_senni_2508.csv')
    )
    REJECT LIMIT UNLIMITED
    ;


CREATE TABLE hosumiru_tap_2508(
    shop_id NUMBER(8),
    shop_name VARCHAR2(200),
    shop_area VARCHAR2(100),
    イベント名 VARCHAR2(100),
    lp VARCHAR2(200),
    tap NUMBER(3)
    )
    ORGANIZATION EXTERNAL
    (TYPE ORACLE_LOADER
    DEFAULT DIRECTORY temp_dir
    ACCESS PARAMETERS
        (RECORDS DELIMITED BY NEWLINE
        SKIP 1
        BADFILE 'hosumiru_tap_2508.bad'
        LOGFILE 'hosumiru_tap_2508.log'
        CHARACTERSET AL32UTF8
        FIELDS TERMINATED BY ','
        OPTIONALLY ENCLOSED BY '"'
            (
            shop_id FLOAT EXTERNAL,
            shop_name CHAR(200),
            shop_area CHAR(100),
            イベント名 CHAR(100),
            lp CHAR(200),
            tap FLOAT EXTERNAL
            )
        )
    LOCATION ('hosumiru_tap_2508.csv')
    )
    REJECT LIMIT UNLIMITED
    ;


CREATE TABLE hosumiru_page_2508(
    ページパス VARCHAR2(200),
    クリック数 NUMBER(6),
    表示回数 NUMBER(6),
    CTR VARCHAR2(20),
    掲載順位 NUMBER(5,2)
    )
    ORGANIZATION EXTERNAL
    (TYPE ORACLE_LOADER
    DEFAULT DIRECTORY temp_dir
    ACCESS PARAMETERS
        (RECORDS DELIMITED BY NEWLINE
        SKIP 1
        BADFILE 'hosumiru_page_2508.bad'
        LOGFILE 'hosumiru_page_2508.log'
        CHARACTERSET AL32UTF8
        FIELDS TERMINATED BY ','
        OPTIONALLY ENCLOSED BY '"'
            (
            ページパス CHAR(200),
            クリック数 FLOAT EXTERNAL,
            表示回数 FLOAT EXTERNAL,
            CTR CHAR(20),
            掲載順位 FLOAT EXTERNAL
            )
        )
    LOCATION ('hosumiru_page_2508.csv')
    )
    REJECT LIMIT UNLIMITED
    ;


WITH t_ss AS(
    SELECT
        COALESCE(ss8.ページパス, ss7.ページパス) AS ページパス,
        NVL(ss7.流入2507, 0) AS 流入2507,
        NVL(ss8.流入2508, 0) AS 流入2508
        FROM(
            SELECT ページパス, SUM(イベント数) AS 流入2507
                FROM ga_hosumiru_traffic_2507
                GROUP BY ページパス
            ) ss7
            FULL OUTER JOIN(
            SELECT ページパス, SUM(イベント数) AS 流入2508
                FROM ga_hosumiru_traffic_2508
                GROUP BY ページパス
            ) ss8
            ON ss7.ページパス = ss8.ページパス
        ),
 t_cv AS(
    SELECT
        COALESCE(cv8.lp, cv7.lp) AS ランディングページ,
        NVL(cv7.反響2507, 0) AS 反響2507,
        NVL(cv8.反響2508, 0) AS 反響2508
        FROM(
            SELECT lp, SUM(tap) AS 反響2507
                FROM ga_hosumiru_tap_2507
                GROUP BY lp
            ) cv7
            FULL OUTER JOIN(
            SELECT lp, SUM(tap) AS 反響2508
                FROM ga_hosumiru_tap_2508
                GROUP BY lp
            ) cv8
            ON cv7.lp = cv8.lp
        )
    SELECT
        COALESCE(t_ss.ページパス, t_cv.ランディングページ) AS ページ,
        NVL(t_ss.流入2507, 0) AS "7月流入",
        NVL(t_ss.流入2508, 0) AS "8月流入",
        NVL(t_ss.流入2508, 0) - NVL(t_ss.流入2507, 0) AS "流入差",
        NVL(t_cv.反響2507, 0) AS "7月tel_tapまたはselect_item_to_map",
        NVL(t_cv.反響2508, 0) AS "8月tel_tapまたはselect_item_to_map",
        NVL(t_cv.反響2508, 0) - NVL(t_cv.反響2507, 0) AS "反響差"
            FROM t_ss FULL OUTER JOIN t_cv
            ON t_ss.ページパス = t_cv.ランディングページ
            -- WHERE ページパス = '/magazine/4122/'
            -- ORDER BY 反響差 DESC
            ;

