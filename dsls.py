CATEGORY_MATCH_SCRIPT = """
if (!doc.containsKey('fast_text_category_name') || doc['fast_text_category_name'].empty) {
    return 0.0;
}
if (!params.containsKey('category_weights') || params.category_weights == null || params.category_weights.empty) {
    return 0.0;
}
def category = doc['fast_text_category_name'].value;
if (params.category_weights.containsKey(category)) {
    return params.category_weights[category];
}
return 0.0;
"""


def get_current_dsl(query):
    return {
        "query": {
            "bool": {
                "must": [
                    {"match": {"serving_title": {"query": query, "operator": "and"}}}
                ],
                "filter": [
                    {"exists": {"field": "catalog_product_set_ids"}},
                    {"term": {"availability": {"value": "IN_STOCK"}}},
                ],
            }
        },
        "_source": ["original_id"],
        "aggs": {
            "by_catalog_id": {
                "terms": {"field": "catalog_id", "size": 1000},
                "aggs": {
                    "top_catalog_hits": {
                        "top_hits": {
                            "sort": [{"_score": {"order": "desc"}}],
                            "_source": [
                                "original_id",
                                "product_id",
                                "catalog_id",
                                "title",
                                "brand_name",
                                "image_url",
                                "landing_url",
                                "price",
                                "sale_price",
                                "catalog_product_set_ids",
                                "sale_price_effective_date_from",
                                "sale_price_effective_date_to",
                                "fast_text_category_name",
                            ],
                            "size": 10,
                        }
                    }
                },
            }
        },
        "size": 1,
    }


def get_terms_dsl(query, catalog_ids):
    return {
        "query": {
            "bool": {
                "must": [
                    {"match": {"serving_title": {"query": query, "operator": "and"}}}
                ],
                "filter": [
                    {"exists": {"field": "catalog_product_set_ids"}},
                    {"term": {"availability": {"value": "IN_STOCK"}}},
                    {"terms": {"catalog_id": catalog_ids}}
                ],
            }
        },
        "_source": [
            "original_id",
            "product_id",
            "catalog_id",
            "title",
            "brand_name",
            "image_url",
            "landing_url",
            "price",
            "sale_price",
            "catalog_product_set_ids",
            "sale_price_effective_date_from",
            "sale_price_effective_date_to",
            "fast_text_category_name",
        ],
        "size": 1000,
    }


def get_category_match_bm25sort_dsl(query, category_weights):
    return {

        "query": {
            "function_score": {
                "boost_mode": "sum",
                "query": {
                    "bool": {
                        "must": [
                            {"match": {"serving_title": {"query": query, "operator": "and"}}}
                        ],
                        "filter": [
                            {"exists": {"field": "catalog_product_set_ids"}},
                            {"term": {"availability": {"value": "IN_STOCK"}}}
                        ]
                    }
                },
                "functions": [
                    {
                        "weight": 100,
                        "script_score": {
                            "script": {
                                "source": CATEGORY_MATCH_SCRIPT,
                                "params": {"category_weights": category_weights}
                            }
                        }
                    }
                ],
                "score_mode": "sum"
            }
        },
        "_source": ["original_id"],
        "aggs": {
            "by_catalog_id": {
                "terms": {"field": "catalog_id", "size": 1000},
                "aggs": {
                    "top_catalog_hits": {
                        "top_hits": {
                            "sort": [{"_score": {"order": "desc"}}],
                            "_source": [
                                "original_id",
                                "product_id",
                                "catalog_id",
                                "title",
                                "brand_name",
                                "image_url",
                                "landing_url",
                                "price",
                                "sale_price",
                                "catalog_product_set_ids",
                                "sale_price_effective_date_from",
                                "sale_price_effective_date_to",
                                "fast_text_category_name",
                            ],
                            "size": 10,
                            "script_fields": {
                                "fast_text_category_match": {
                                    "script": {
                                        "source": CATEGORY_MATCH_SCRIPT,
                                        "params": {"category_weights": category_weights}
                                    }
                                }
                            }
                        }
                    }
                },
            }
        },
        "size": 1,
    }


def get_category_match_randomsort_dsl(query, category_weights):
    return {

        "query": {
            "function_score": {
                "boost_mode": "sum",
                "query": {
                    "bool": {
                        "filter": [
                            {"exists": {"field": "catalog_product_set_ids"}},
                            {"term": {"availability": {"value": "IN_STOCK"}}},
                            {"match": {"serving_title": {"query": query, "operator": "and"}}}
                        ]
                    }
                },
                "functions": [
                    {
                        "weight": 100,
                        "script_score": {
                            "script": {
                                "source": CATEGORY_MATCH_SCRIPT,
                                "params": {"category_weights": category_weights}
                            }
                        }
                    },
                    {
                        "random_score": {}
                    }
                ],
                "score_mode": "sum"
            }
        },
        "_source": ["original_id"],
        "aggs": {
            "by_catalog_id": {
                "terms": {"field": "catalog_id", "size": 1000},
                "aggs": {
                    "top_catalog_hits": {
                        "top_hits": {
                            "sort": [
                                {"_score": {"order": "desc"}},
                                {"original_id": {"order": "desc"}}
                            ],
                            "_source": [
                                "original_id",
                                "product_id",
                                "catalog_id",
                                "title",
                                "brand_name",
                                "image_url",
                                "landing_url",
                                "price",
                                "sale_price",
                                "catalog_product_set_ids",
                                "sale_price_effective_date_from",
                                "sale_price_effective_date_to",
                                "fast_text_category_name",
                            ],
                            "size": 10,
                            "script_fields": {
                                "fast_text_category_match": {
                                    "script": {
                                        "source": CATEGORY_MATCH_SCRIPT,
                                        "params": {"category_weights": category_weights}
                                    }
                                }
                            }
                        }
                    }
                },
            }
        },
        "size": 1,
    }


def get_category_match_idsort_dsl(query, category_weights):
    return {

        "query": {
            "function_score": {
                "boost_mode": "replace",
                "query": {
                    "bool": {
                        "filter": [
                            {"exists": {"field": "catalog_product_set_ids"}},
                            {"term": {"availability": {"value": "IN_STOCK"}}},
                            {"match": {"serving_title": {"query": query, "operator": "and"}}}
                        ]
                    }
                },
                "functions": [
                    {
                        "weight": 100,
                        "script_score": {
                            "script": {
                                "source": CATEGORY_MATCH_SCRIPT,
                                "params": {"category_weights": category_weights}
                            }
                        }
                    }
                ],
                "score_mode": "sum"
            }
        },
        "_source": ["original_id"],
        "aggs": {
            "by_catalog_id": {
                "terms": {"field": "catalog_id", "size": 1000},
                "aggs": {
                    "top_catalog_hits": {
                        "top_hits": {
                            "sort": [
                                {"_score": {"order": "desc"}},
                                {"original_id": {"order": "desc"}}
                            ],
                            "_source": [
                                "original_id",
                                "product_id",
                                "catalog_id",
                                "title",
                                "brand_name",
                                "image_url",
                                "landing_url",
                                "price",
                                "sale_price",
                                "catalog_product_set_ids",
                                "sale_price_effective_date_from",
                                "sale_price_effective_date_to",
                                "fast_text_category_name",
                            ],
                            "size": 10,
                            "script_fields": {
                                "fast_text_category_match": {
                                    "script": {
                                        "source": CATEGORY_MATCH_SCRIPT,
                                        "params": {"category_weights": category_weights}
                                    }
                                }
                            }
                        }
                    }
                },
            }
        },
        "size": 1,
    }