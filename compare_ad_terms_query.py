import requests
import json
import pandas_gbq
import dsls
import pandas as pd
from tqdm import tqdm
import time
import random


SQL = """
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
    100
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
        time.sleep(random.random())
        times.append(response.json()["took"])

    cut = int(len(times) * 0.1)
    times = sorted(times[cut : len(times) - cut + 1])

    average_time = sum(times) / len(times) if times else 0
    return average_time, times


if __name__ == "__main__":
    sql = """SELECT DISTINCT catalog_id FROM `karrotmarket.team_search_indexer_kr.ads_catalog_product_serving_v2` WHERE deleted_at is null"""
    catalog_ids_serving = pandas_gbq.read_gbq(sql, project_id="karrotmarket")
    catalog_ids10 = catalog_ids_serving["catalog_id"].tolist()
    sql = """SELECT DISTINCT catalog_id FROM `karrotmarket.team_search_indexer_kr.ads_catalog_product_v2` WHERE deleted_at is null"""
    catalog_ids = pandas_gbq.read_gbq(sql, project_id="karrotmarket")
    catalog_ids100 = catalog_ids["catalog_id"].sample(n=100).tolist()
    catalog_ids1000 = catalog_ids["catalog_id"].sample(n=1000).tolist()
    catalog_ids2000 = catalog_ids["catalog_id"].sample(n=2000).tolist()
    print(len(catalog_ids10), len(catalog_ids100), len(catalog_ids1000), len(catalog_ids2000))

    es_alpha = "http://search-searching.alpha.kr.krmt.io"
    es_prod = "http://search-searching.kr.krmt.io"
    index_name = "ads-catalog-product-serving-v2"

    df = pandas_gbq.read_gbq(
        SQL, project_id="karrotmarket", dialect="standard", use_bqstorage_api=True
    )
    
    results = []
    for keyword in tqdm(df["queryText"], total=len(df)):
        asis_query = dsls.get_current_dsl(keyword)
        terms10_query = dsls.get_terms_dsl(keyword, catalog_ids10)
        terms100_query = dsls.get_terms_dsl(keyword, catalog_ids100)
        terms1000_query = dsls.get_terms_dsl(keyword, catalog_ids1000)
        terms2000_query = dsls.get_terms_dsl(keyword, catalog_ids1000)

        prod_asis_avg_time, _ = get_query_time(es_prod, index_name, asis_query, iterations=20)
        prod_terms10_avg_time, _ = get_query_time(es_prod, index_name, terms10_query, iterations=20)

        alpha_asis_avg_time, _ = get_query_time(es_alpha, index_name, asis_query)
        alpha_terms10_avg_time, _ = get_query_time(es_alpha, index_name, terms10_query)
        alpha_terms100_avg_time, _ = get_query_time(es_alpha, index_name, terms100_query)
        alpha_terms1000_avg_time, _ = get_query_time(es_alpha, index_name, terms1000_query)
        alpha_terms2000_avg_time, _ = get_query_time(es_alpha, index_name, terms2000_query)

        results.append(
            {
                "query": keyword,
                "prod_time_asis": prod_asis_avg_time,
                "prod_time_terms10": prod_terms10_avg_time,
                "alpha_time_asis": alpha_asis_avg_time,
                "alpha_time_terms10": alpha_terms10_avg_time,
                "alpha_time_terms100": alpha_terms100_avg_time,
                "alpha_time_terms1000": alpha_terms1000_avg_time,
                "alpha_time_terms2000": alpha_terms2000_avg_time,
            }
        )
    pd.DataFrame(results).to_csv("results.csv", index=False)
