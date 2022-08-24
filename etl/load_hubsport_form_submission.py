"""
This script reads Hubspot form submission data 
from Hubspot and sends it to Data Vault 2.0 storage
Made by Dmitrii Zhuravlev madhotcar@gmail.com
"""


import logging
import requests
import pandas as pd
from configparser import ConfigParser
from urllib.parse import urlencode
import sys
import numpy as np
sys.path.append("../../functions")
from etl import DataETL, LogTime
from requests_retry import requests_retry_session
from adwh.dataframes import (
    get_df_hubspot_contacts, 
    get_df_users
)
from adwh.engine import create_default_engine
from adwh.models.dds import (
    HubHubspotFormSubmission, 
    LinkUserHubspotFormSubmission, 
    LinkHubspotContactHubspotFormSubmission,
    SatHubspotFormSubmission
)
from adwh.session import default_session_scope

sys.path.append("../../integrations/common")
from hubspot_auth import get_hubspot_headers

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(level=logging.DEBUG)


FORMS = {
    "Demo request - website": "0c7a4968-23ab-11ed-861d-0242ac120002",
    "Website. Talk about plan": "210826de-23ab-11ed-861d-0242ac120002",
    "Speak to sales": "27fe1962-23ab-11ed-861d-0242ac120002",
    "Speak to sales RU version": "81605767-b02f-4380-bcca-4ebeadef8a3d",
    "Watch. Need an upgrade": "0287b948-1f55-4346-9a1f-0cb1dd929580",
    "Watch. Get a trial": "e562ba94-3d7d-49fd-9d1f-e627204fad14",
    "Watch. Get a demo": "ee2a6df9-58fb-405a-afdc-30ac19bce19f",
}


def request_submissions(conf: ConfigParser, form_id: str) -> pd.DataFrame:
    hubspot_headers = get_hubspot_headers(conf=conf)
    submissions = []
    page = ""
    while True:
        url = f"https://api.hubapi.com/form-integrations/v1/submissions/forms/{form_id}"
        data = {
            "limit": 50,
            "after": page,
        }
        response = requests_retry_session().get(
            f"{url}?{urlencode(data)}", headers=hubspot_headers)
        response_json = response.json()
        assert response_json.get("results"), \
            f"No results, response – {response} {response_json}"
        
        submissions += response_json["results"]
        if not response_json.get("paging"):
            return pd.DataFrame(submissions)
        page = response_json["paging"]["next"]["after"]


def finalise_df(df: pd.DataFrame) -> pd.DataFrame:
    df["record_source"] = "hubspot_api"
    df = df.replace({pd.NaT: None, np.nan: None, np.NAN: None, np.inf: None, "": None})
    return df


def get_df_submissions(
    conf: ConfigParser, form_name: str, form_id: str) -> pd.DataFrame:
    df_submissions = request_submissions(conf=conf, form_id=form_id)
    df_values = df_submissions["values"].apply(
        lambda x: pd.Series({item["name"]: item["value"] for item in x}))
    df_submissions = pd.concat([df_submissions, df_values], axis=1)
    df_submissions["submitted_datetime"] = pd.to_datetime(
        df_submissions["submittedAt"], unit="ms")
    df_submissions = df_submissions.rename(columns={
        "submittedAt": "submitted_at",
        "pageUrl": "page_url",
        "firstname": "first_name",
        "record_source": "hs_record_source",
        "does_your_product_or_service_have_a_mobile_app_": "has_app",
    })
    if "has_app" in df_submissions.columns:
        df_submissions["has_app"] = df_submissions["has_app"].apply(
            lambda x: True if x == 'true' else False)
    df_submissions["hash_key"] = df_submissions.apply(
        lambda row: HubHubspotFormSubmission(
            submitted_at=row["submitted_at"],
            values=row["values"],
            page_url=row["page_url"],
        ).generate_hash_key(), axis=1)
    df_submissions["form_id"] = form_id
    df_submissions["form_name"] = form_name
    
    return df_submissions


def get_df_users_hs_contacts(df_submissions: pd.DataFrame) -> pd.DataFrame:
    with LogTime("## loading users from DDS"):
        with default_session_scope() as session:
            df_users = get_df_users(session=session, cols=("hash_key", "email"))
    df_users = df_users.rename(columns={"hash_key": "hub_user_hash_key"})
    df_users_submissions = df_submissions.merge(df_users, how="inner", on="email")
    df_users_submissions = df_users_submissions.rename(
        columns={"hash_key": "hub_hubspot_form_submission_hash_key"})
    df_users_submissions["hash_key"] = df_users_submissions.apply(
        lambda x: LinkUserHubspotFormSubmission(
                hub_user_hash_key=x["hub_user_hash_key"],
                hub_hubspot_form_submission_hash_key=x["hub_hubspot_form_submission_hash_key"],
            ).generate_hash_key(),
            axis=1,
    )
    df_users_submissions = finalise_df(df=df_users_submissions)
    return df_users_submissions


def get_df_hs_contacts_hs_form_submissions(df_submissions: pd.DataFrame) -> pd.DataFrame:
    with LogTime("## loading users from DDS"):
        with default_session_scope() as session:
            df_hubspot_contacts = get_df_hubspot_contacts(session=session, cols=("hash_key", "email"))
    df_hubspot_contacts = df_hubspot_contacts.rename(
        columns={"hash_key": "hub_hubspot_contact_hash_key"})
    df_hs_contacts_hs_form_submissions = df_submissions.merge(
        df_hubspot_contacts, how="inner", on="email")
    df_hs_contacts_hs_form_submissions = df_hs_contacts_hs_form_submissions.rename(
        columns={"hash_key": "hub_hubspot_form_submission_hash_key"})
    df_hs_contacts_hs_form_submissions["hash_key"] = df_hs_contacts_hs_form_submissions.apply(
        lambda x: LinkHubspotContactHubspotFormSubmission(
                hub_hubspot_contact_hash_key=x["hub_hubspot_contact_hash_key"],
                hub_hubspot_form_submission_hash_key=x["hub_hubspot_form_submission_hash_key"],
            ).generate_hash_key(),
            axis=1,
    )
    df_hs_contacts_hs_form_submissions = finalise_df(df=df_hs_contacts_hs_form_submissions)
    return df_hs_contacts_hs_form_submissions


def main(conf):
    stats_dict = {}

    with LogTime("# loading forms submissions from Hubspot"):
        df_submissions = pd.DataFrame()
        for form_name in FORMS:
            chunk = get_df_submissions(conf=conf, form_name=form_name, form_id=FORMS[form_name])
            df_submissions = pd.concat([df_submissions, chunk], sort=False)
        df_submissions = finalise_df(df=df_submissions)
        df_users_hs_contacts = get_df_users_hs_contacts(df_submissions=df_submissions)
        df_hs_contacts_hs_form_submissions = get_df_hs_contacts_hs_form_submissions(
            df_submissions=df_submissions)


    adwh_engine = create_default_engine()
    with adwh_engine.connect() as connection:
        with LogTime("## extracting hash keys from the DDS"):
            tbl = HubHubspotFormSubmission.__table__
            results = connection.execute(
                tbl.select().with_only_columns([tbl.c.hash_key])
            )
            hub_hubspot_form_submission_hash_keys = set(pd.DataFrame(results)[0])

        with LogTime(
            "## loading Salesforce opportunities to HubHubspotFormSubmission, SatHubspotFormSubmission"
        ):
            df_to_send = df_submissions[
                ~df_submissions["hash_key"].isin(
                    hub_hubspot_form_submission_hash_keys
                )
            ]
            if not df_to_send.empty:
                connection.execute(
                    HubHubspotFormSubmission.__table__.insert(),
                    df_to_send.to_dict("records"),
                )
            connection.execute(
                SatHubspotFormSubmission.__table__.insert(),
                df_submissions.to_dict("records"),
            )
            stats_dict["hubspot_submissions_handled"] = df_submissions.shape[0]

        with LogTime("## loading links to the LinkUserHubspotFormSubmission"):
            connection.execute(
                LinkUserHubspotFormSubmission.__table__.insert(),
                df_users_hs_contacts.to_dict("records"),
            )
            stats_dict[
                "links_users_hs_contacts"
            ] = df_users_hs_contacts.shape[0]

        with LogTime("## loading links to the LinkHubspotContactHubspotFormSubmission"):
            connection.execute(
                LinkHubspotContactHubspotFormSubmission.__table__.insert(),
                df_hs_contacts_hs_form_submissions.to_dict("records"),
            )
            stats_dict[
                "links_hs_contacts_hs_form_submissions"
            ] = df_hs_contacts_hs_form_submissions.shape[0]
            
    return stats_dict


if __name__ == "__main__":
    DataETL(
        configs=(
            "hubspot.ini",
        ),
        download_configs=True,
    ).run(entrypoint=main)
