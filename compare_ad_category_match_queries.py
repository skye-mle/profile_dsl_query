import requests
import json
import pandas_gbq
import dsls
import pandas as pd
from tqdm import tqdm
import time
import random


SQL = """
WITH
  TARGET_TABLE AS (
  SELECT
    kw_cat.keyword,
    kw_cat.run_date,
    cw.category_id,
    cw.category_name,
    cat.uid AS hoian_category_name,
    cw.predict,
    cw.is_boost,
    cw.probability,
    cw.score
  FROM
    `karrotmarket.team_search_data.fleamarket_category_weights` AS kw_cat,
    UNNEST(kw_cat.category_weights) AS cw
  LEFT JOIN
    `karrotmarket.db_hoian_kr.categories` AS cat
  ON
    cw.category_id = cat.id
  WHERE
    kw_cat.run_date = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) ),
  CATEGORY_WEIGHT AS (
  SELECT
    keyword,
    TIMESTAMP(run_date) AS run_date,
    TO_JSON_STRING( ARRAY_AGG(STRUCT( category_id,
          category_name,
          hoian_category_name,
          predict,
          is_boost,
          probability,
          score )) ) AS category_weights
  FROM
    TARGET_TABLE
  GROUP BY
    keyword,
    run_date ),
  QC AS (
  SELECT
    queryText,
    qc,
    qc / total_qc AS qc_percentage,
    SUM(qc / total_qc) OVER (ORDER BY qc DESC) AS cumulative_qc_percentage
  FROM (
    SELECT
      queryText,
      COUNT(*) AS qc,
      SUM(COUNT(*)) OVER () AS total_qc
    FROM
      `karrotmarket.kotisaari_data.stream_mediation-request_v1`
    WHERE
      TIMESTAMP_TRUNC(requestedAt, DAY) BETWEEN TIMESTAMP("2024-09-01")
      AND TIMESTAMP("2024-09-07")
      AND queryText IS NOT NULL
    GROUP BY
      queryText )
  ORDER BY
    qc DESC
  LIMIT
    100 )
SELECT
  QC.queryText,
  cw.category_weights
FROM
  QC
LEFT JOIN
  CATEGORY_WEIGHT cw
ON
  TRIM(QC.queryText) = cw.keyword
"""


def get_query_time(es_host, index_name, query, iterations=50):
    headers = {"Content-Type": "application/json"}
    times = []

    for _ in range(iterations):
        retry = 3
        while retry:
            try:
                response = requests.get(
                    f"{es_host}/{index_name}/_search",
                    headers=headers,
                    data=json.dumps(query),
                )
            except:
                print("Error! retry...", retry)
                retry -= 1
            else:
                retry = 0
        if response.status_code != 200:
            print(f"Error: {response.status_code}")
            return None
        times.append(response.json()["took"])

    cut = int(len(times) * 0.1)
    times = sorted(times[cut : len(times) - cut + 1])

    average_time = sum(times) / len(times) if times else 0
    return average_time, times


if __name__ == "__main__":
    es_alpha = "http://search-searching.alpha.kr.krmt.io"
    es_prod = "http://search-searching.kr.krmt.io"
    index_name = "ads-catalog-product-serving-v2"

    df = pandas_gbq.read_gbq(
        SQL, project_id="karrotmarket", dialect="standard", use_bqstorage_api=True
    )
    df["hoian_category_name"] = [
        (
            {w["hoian_category_name"]: 1 for w in json.loads(cw) if w["is_boost"]}
            if cw
            else {}
        )
        for cw in df["category_weights"]
    ]

    results = []

    for keyword, category_weight in tqdm(
        zip(df["queryText"], df["hoian_category_name"]), total=len(df)
    ):
        asis_query = dsls.get_current_dsl(keyword)
        idsort_query = dsls.get_category_match_idsort_dsl(keyword, category_weight)
        randomsort_query = dsls.get_category_match_randomsort_dsl(
            keyword, category_weight
        )

        prod_avg_time, _ = get_query_time(es_prod, index_name, asis_query)
        asis_avg_time, _ = get_query_time(es_alpha, index_name, asis_query)
        idsort_avg_time, _ = get_query_time(es_alpha, index_name, idsort_query)
        randomsort_avg_time, _ = get_query_time(es_alpha, index_name, randomsort_query)

        results.append(
            {
                "query": keyword,
                "time_prod_asis": prod_avg_time,
                "time_prod_idsort_pred": prod_avg_time * (idsort_avg_time / asis_avg_time),
                "time_prod_randomsort_pred": prod_avg_time * (randomsort_avg_time / asis_avg_time),
                "time_alpha_asis": asis_avg_time,
                "time_alpha_idsort": idsort_avg_time,
                "time_alpha_randomsort": randomsort_avg_time,
            }
        )
    pd.DataFrame(results).to_csv("results.csv", index=False)
