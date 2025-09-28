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

CREATE TABLE ga_guide_traffic_2508
    AS SELECT * FROM guide_traffic_2508;


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

CREATE TABLE ga_guide_pv_2508
    AS SELECT * FROM guide_pv_2508


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

CREATE TABLE ga_guide_senni_2508
    AS SELECT * FROM guide_senni_2508;


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

CREATE TABLE ga_guide_tap_2508
    AS SELECT * FROM guide_tap_2508;


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

CREATE TABLE sc_guide_page_2508
    AS SELECT * FROM guide_page_2508;


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



