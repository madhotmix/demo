"""
This script contains code to retrieve data from 
Yandex.Market catalog and print 
category / offers count report
Expects url to xml file as parameter
Made by Dmitrii Zhuravlev madhotcar@gmail.com
"""


import requests
import sys
from collections import OrderedDict, deque
from time import time

import pandas as pd
import validators
import xmltodict
from tabulate import tabulate


class LogTime:

    def __init__(self, message: str):
        self.message = message

    def __enter__(self):
        self.start_time = time()
        print(f"{self.message} STARTED")

    def __exit__(self, *args, **kwargs):
        print(f"{self.message} FINISHED ({round(time() - self.start_time, 2)} seconds)")


def validate_response(catalog_data_url: str, response: requests.models.Response):
    if not 200 <= response.status_code < 300:
        response_text_len = 300
        response_err_text = response.text[:response_text_len]
        if len(response.text) >= response_text_len:
            response_err_text += "..."
        raise Exception(
            f"Error when trying to request the address {catalog_data_url}\n"
            f"Response status code – {response.status_code}\n"
            f"Response text – {response_err_text}"
        )
    if not response.text.startswith("<?xml"):
        raise Exception(
            f"The content received via the link {catalog_data_url} is not an xml file"
        )


def get_category_tree(df_category: pd.DataFrame, row: pd.Series) -> str:
    category_deque = deque({row["text"]})
    parent_category_id = row["parent_id"]
    safety_counter = 0
    while True:
        df_parent_category = df_category[df_category["category_id"] == parent_category_id]
        if df_parent_category.empty:
            break
        category_deque.appendleft(df_parent_category["text"].values[0])
        parent_category_id = df_parent_category["parent_id"].values[0]
        if safety_counter > len(df_category):
            raise Exception(
                "The number of search parent category iterations exceeded the number of categories\n"
                f"The child category ID is {row['id']}")
    category = " / ".join(category_deque)
    return category


def get_df_offer(catalog_data: OrderedDict) -> pd.DataFrame:
    df_offer = pd.DataFrame(catalog_data["offers"]["offer"])
    df_offer = df_offer.rename(columns={
        "categoryId": "category_id",
    })
    
    if df_offer.empty:
        return df_offer
    
    df_offer["offers"] = 1
    df_offer = df_offer.groupby("category_id").agg({"offers": sum}).reset_index()
    return df_offer


def get_df_category(catalog_data: OrderedDict, df_offer: pd.DataFrame) -> pd.DataFrame:
    df_category = pd.DataFrame(catalog_data["categories"]["category"])
    df_category = df_category.rename(columns={
        "@id": "category_id",
        "@parentId": "parent_id",
        "#text": "text",
    })
    if df_offer.empty:
        return df_category
    
    df_category["category"] = df_category.apply(
        lambda row: get_category_tree(df_category=df_category, row=row), axis=1)

    df_category = df_category.merge(df_offer, how="left", on="category_id")
    df_category["offers"] = df_category["offers"].fillna(0)
    df_category["offers"] = df_category["offers"].astype(int)
    df_category = df_category.sort_values("category")
    return df_category


def main():
    if len(sys.argv) <= 1:
        raise Exception("The URL of the xml file is not specified as a command line argument")
    catalog_data_url = sys.argv[1]
    if validators.url(catalog_data_url) is not True:
        raise Exception(f"Invalid URL {catalog_data_url}")

    with LogTime("## getting and validating catalog data"):
        response = requests.get(catalog_data_url)
        validate_response(catalog_data_url=catalog_data_url, response=response)
        response_dict = xmltodict.parse(response.text)
        if not response_dict.get("yml_catalog") or not response_dict["yml_catalog"].get("shop"):
            raise Exception("Invalid catalog data")
    
    with LogTime("## parsing catalog data"):
        catalog_data = response_dict["yml_catalog"]["shop"]
        df_offer = get_df_offer(catalog_data=catalog_data)
        df_category = get_df_category(catalog_data=catalog_data, df_offer=df_offer)
    
    output_columns = ["category", "offers"]
    print(tabulate(df_category[output_columns].values, headers=output_columns, tablefmt='orgtbl'))
    

if __name__ == "__main__":
    main()
