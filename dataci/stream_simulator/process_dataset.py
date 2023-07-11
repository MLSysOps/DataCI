#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Jul 07, 2023
"""
# 1. process yelp dataset json to csv
# 2. Save CSV to Hive temp table
# 3. Process Hive temp table to Hive partitioned table

import json
from textwrap import dedent

import pandas as pd
from pyhive import hive

# yelp_academic_dataset_business = 'data/yelp/yelp_academic_dataset_business.json'
# yelp_academic_dataset_checkin = 'data/yelp/yelp_academic_dataset_checkin.json'
yelp_academic_dataset_review = 'data/yelp/yelp_academic_dataset_review.json'
# yelp_academic_dataset_tip = 'data/yelp/yelp_academic_dataset_tip.json'
# yelp_academic_dataset_user = 'data/yelp/yelp_academic_dataset_user.json'

review_list = []
with open(yelp_academic_dataset_review, 'r') as f:
    for line in f:
        review = json.loads(line)
        # To load CSV data into Hive table, we are using CSV SerDe.
        # Since CSV SerDe does not support embedded line breaks in a quoted column value,
        # we need to replace it with an escape character, e.g., \n -> \\n
        # Known issue: https://github.com/trinodb/trino/issues/8350
        review['text'] = review['text'].encode('unicode_escape').decode('utf-8')
        review_list.append(review)

df = pd.DataFrame(review_list).to_csv(
    'data/yelp/yelp_academic_dataset_review.csv', index=False
)

# Need to install sasl for authentication
conn = hive.connect(host='localhost', port=10000, auth='NONE')
with conn.cursor() as cur:
    cur.execute(dedent("""
    CREATE TABLE IF NOT EXISTS yelp_review_tmp
    (
        review_id   STRING,
        user_id     STRING,
        business_id STRING,
        stars       BIGINT,
        useful      BIGINT,
        funny       BIGINT,
        cool        BIGINT,
        text        STRING,
        `date`      STRING
    )
    -- Required for load CSV data into the table
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    ;

    LOAD DATA LOCAL INPATH 'data/warehouse/yelp_academic_dataset_review.csv' OVERWRITE INTO TABLE yelp_review_tmp;
    """))

# Create partitioned table
with conn.cursor() as cur:
    cur.execute(dedent("""
    CREATE TABLE IF NOT EXISTS yelp_review
    (
        review_id   STRING,
        user_id     STRING,
        business_id STRING,
        stars       BIGINT,
        useful      BIGINT,
        funny       BIGINT,
        cool        BIGINT,
        text        STRING,
        `date`      STRING
    )
    PARTITIONED BY (ds STRING)
    STORED AS ORC
    ;
    
    -- Single node cluster does not allow insert too many dynamic partitions by default
    -- https://stackoverflow.com/questions/38152787/error-when-inserting-data-to-a-partitioned-table-in-hive
    SET hive.exec.max.dynamic.partitions=100000;
    SET hive.exec.max.dynamic.partitions.pernode=100000;
    INSERT OVERWRITE TABLE yelp_review PARTITION (ds)
    SELECT review_id
           ,user_id
           ,business_id
           ,stars
           ,useful
           ,funny
           ,cool
           ,text
           ,`date`
           ,date_format(to_date(`date`), 'yyyyMMdd') as ds
    FROM yelp_review_tmp
    ;
    """))
