/*
    Copyright 2023 Google. This software is provided as-is, without warranty or representation for any use or purpose. Your use of it is subject to your agreement with Google.

    BigQuery Stored Procecedure for SCD Type 2 Transformation.
    Assumptions:
    - Source and target tables contain the same columns and data types.
    - Data type of start_date and end_date is (DATE, DATETIME or TIMESTAMP)
    - Primary key columns cannot be null
    - To faciliate null comparisons, the following special "null" values are used
      - STRING: 'NULL'
      - DATE/DATETIME/TIMESTAMP: '1900-01-01'
      - NUMERIC datatypes: -1

    Sample
        call test.perform_scd2_transform(
            'my-test-project',
            'my_dataset',
            'source_table',
            'my_dataset',
            'target_table',
            '',
            'pk1,pk2',
            'start_date',
            'end_date'
            );
 */
CREATE OR REPLACE PROCEDURE test.perform_scd2_transform (
    project_id STRING,
    target_dataset_id STRING,
    target_table_name STRING,
    source_dataset_id STRING,
    source_table_name STRING,
    source_table_filter STRING, -- WHERE clause condition to filter out source table (e.g., by ingestion date)
    primary_keys STRING, -- comma separated list of key column
    start_date_column STRING, -- DATE, DATETIME or TIMESTAMP
    end_date_column STRING -- DATE, DATETIME or TIMESTAMP
)
BEGIN 
    -- Variables for Customisation
    DECLARE numeric_nullvalue DEFAULT '-1';
    DECLARE string_nullvalue DEFAULT '\'NULL\'';
    DECLARE date_nullvalue DEFAULT '\'1900-01-01\'';

    -- Internal Variables
    DECLARE target_table_name_full STRING;
    DECLARE source_table_name_full STRING;
    DECLARE date_column_type STRING DEFAULT 'DATETIME';
    DECLARE keyalias STRING DEFAULT '';
    DECLARE nullkeys STRING DEFAULT '';
    DECLARE key_join_cond STRING DEFAULT '';
    DECLARE inner_key_join_cond DEFAULT '';
    DECLARE i INT64 DEFAULT 0;
    DECLARE nullvalue STRING;
    DECLARE nonkey_join_cond DEFAULT '';
    DECLARE nonkey_join_cond_w_end_date DEFAULT '';
    DECLARE column_list DEFAULT '';
    DECLARE merge_statement DEFAULT '';
    DECLARE filter_clause DEFAULT '';

    SET target_table_name_full = CONCAT(project_id, '.', target_dataset_id, '.', target_table_name);
    SET source_table_name_full = CONCAT(project_id, '.', source_dataset_id, '.', source_table_name);

    IF length(source_table_filter) > 0 THEN
        SET filter_clause = CONCAT(' WHERE ', source_table_filter);
    END IF;

    -- get column details from INFORMATION_SCHEMA
    EXECUTE IMMEDIATE concat(
        'CREATE TEMP TABLE scd2_table_columns AS ',
        'SELECT ordinal_position, column_name, data_type ',
        'FROM `', project_id, '.', target_dataset_id, '.INFORMATION_SCHEMA.COLUMNS` ',
        'WHERE table_catalog = \'', project_id, '\' ',
        '    AND table_schema = \'', target_dataset_id, '\' ',
        '    AND table_name = \'', target_table_name, '\' '
    );

    SET date_column_type = (
        SELECT
            data_type
        FROM
            scd2_table_columns
        WHERE
            column_name = start_date_column
    );

    -- generate list of column for the "insert" portion of the merge statement
    FOR all_column in (
        SELECT
            column_name
        FROM
            scd2_table_columns
        WHERE
            column_name not in (start_date_column, end_date_column)
        ORDER BY
            ordinal_position
    )
    DO
    SET column_list = concat(column_list, all_column.column_name, ',');
    END FOR;
    SET column_list = RTRIM(column_list, ',');

    -- generate join conditions based on primary key columns
    SET i = 0;
    FOR key_column in (
        SELECT
            column_name,
            data_type
        FROM
            scd2_table_columns
        WHERE
            column_name in (
                select
                    *
                from
                    unnest(split(primary_keys))
            )
            AND column_name not in (start_date_column, end_date_column)
        ORDER BY
            ordinal_position
    ) DO
        SET nullvalue = numeric_nullvalue;
        IF key_column.data_type = 'STRING' THEN
            SET nullvalue = string_nullvalue;
        ELSEIF key_column.data_type in ('DATE', 'DATETIME', 'TIMESTAMP') THEN
            SET nullvalue = concat(key_column.data_type, ' ', date_nullvalue);
        END IF;

        SET keyalias = concat(keyalias, key_column.column_name, ' AS JK' , i, ',');
        SET nullkeys = concat(nullkeys, nullvalue, ' AS JK' , i, ',');
        SET key_join_cond = concat(key_join_cond, 'T.', key_column.column_name, ' = S.JK' , i, ' AND ');
        SET inner_key_join_cond = concat(inner_key_join_cond, 'T.', key_column.column_name, ' = S.', key_column.column_name, ' AND ');

        SET i = i + 1;
    END FOR;
    SET keyalias = RTRIM(keyalias, ',');
    SET nullkeys = RTRIM(nullkeys, ',');
    SET key_join_cond = RTRIM(key_join_cond, ' AND ');
    SET inner_key_join_cond = RTRIM(inner_key_join_cond, ' AND ');

    -- generate comparison statements for non-key columns
    FOR non_key_column in (
        SELECT
            column_name,
            data_type
        FROM
            scd2_table_columns
        WHERE
            column_name not in (
                select
                    *
                from
                    unnest(split(primary_keys))
            )
            AND column_name not in (start_date_column, end_date_column)
        ORDER BY
            ordinal_position
    ) DO
        SET nullvalue = numeric_nullvalue;
        IF non_key_column.data_type = 'STRING' THEN
            SET nullvalue = string_nullvalue;
        ELSEIF non_key_column.data_type in ('DATE', 'DATETIME', 'TIMESTAMP') THEN
            SET nullvalue = concat(non_key_column.data_type, ' ', date_nullvalue);
        END IF;

        SET nonkey_join_cond = concat(nonkey_join_cond, 'IFNULL(T.', non_key_column.column_name, ',', nullvalue, ') <> IFNULL(S.', non_key_column.column_name, ',', nullvalue, ') OR ');
    END FOR;
    SET nonkey_join_cond = RTRIM(nonkey_join_cond, ' OR ');
    SET nonkey_join_cond = concat('(', nonkey_join_cond, ')');
    SET nonkey_join_cond_w_end_date = concat(nonkey_join_cond, ' AND T.', end_date_column,' = ', date_column_type, ' \"9999-12-31 23:59:59\"');

    -- combine all the statement fragments into the final merge statement
    SET merge_statement = concat('MERGE INTO `', target_table_name_full, '` T ',
            'USING (',
            '    SELECT ', keyalias, ', * FROM `', source_table_name_full, '` S ', filter_clause, ' ',
            '    UNION ALL',
            '    SELECT ', nullkeys, ',S.* FROM `', source_table_name_full, '` S ',
            '    JOIN `', target_table_name_full,'` T',
            '    ON ', inner_key_join_cond, ' AND (', nonkey_join_cond_w_end_date, ') ',
            '    ', filter_clause, '',
            ') S ',
            'ON ', key_join_cond, ' ',
            'WHEN MATCHED AND ', nonkey_join_cond, ' THEN UPDATE ',
            'SET ', end_date_column, ' = CURRENT_', date_column_type, '() ',
            'WHEN NOT MATCHED THEN ',
            '  INSERT (', column_list, ', ', start_date_column, ', ', end_date_column, ')',
            '  VALUES (', column_list, ', CURRENT_', date_column_type, '(), ', date_column_type, ' "9999-12-31 23:59:59")'
    );

    EXECUTE IMMEDIATE merge_statement;
END;