from concurrent.futures import ThreadPoolExecutor
from multiprocessing import cpu_count
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Literal
import logging as log
import pandas as pd
import requests

from .exceptions import (
    AttributeNotFoundError,
    AuthorizationError,
    FilterByAttributeError,
    DownloadError,
    QueryError,
)
from .config import Config
from .geo_utils import to_openeo_wkt

log.basicConfig(
    level=log.INFO,
    format="%(levelname)s: %(message)s",
)

CATALOG_URL = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter=Collection"
TOKEN_URL = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
DOWNLOAD_URL = "https://zipper.dataspace.copernicus.eu/odata/v1/Products"


class CopernicusDataspaceAPI(ABC):
    def __init__(self, username: str, password: str) -> None:
        self.username = username or Config.COPERNICUS_USERNAME
        self.password = password or Config.COPERNICUS_PASSWORD
        Config.validate_credentials()

    @property
    @abstractmethod
    def mission(self) -> str:
        ...

    @property
    @abstractmethod
    def prod_types(self) -> list[str]:
        ...

    def _get_access_token(self) -> str:
        data = {
            "client_id": "cdse-public",
            "username": self.username,
            "password": self.password,
            "grant_type": "password",
        }
        try:
            r = requests.post(TOKEN_URL, data=data, timeout=100)
            r.raise_for_status()
        except Exception as e:
            raise AuthorizationError(
                f"Access token creation failed. Error: {e} \n"
                f"\tMake sure your login credentials are correct for"
                " https://dataspace.copernicus.eu/"
            )
        return r.json()["access_token"]

    def query(
        self,
        *,
        start_time: str,
        end_time: str,
        prod_type: str | None = None,
        exclude: str | None = None,
        footprint: str | None = None,
        orderby: Literal["asc", "desc"] | None = None,
        limit: int | None = None,
        **kwargs,
    ) -> pd.DataFrame:
        query_str = self._build_query(
            start_time=start_time,
            end_time=end_time,
            prod_type=prod_type,
            exclude=exclude,
            footprint=footprint,
            orderby=orderby,
            limit=limit,
        )

        try:
            json = requests.get(query_str, timeout=100).json()
        except Exception as e:
            raise QueryError(f"{e.__class__.__name__}: Query failed: {e.args[0]}")

        products = pd.DataFrame.from_dict(json["value"])
        if products.empty and prod_type:
            if not any(prod_type in prod for prod in self.prod_types):
                log.info(
                    f"No product found. Use product types available for {self.mission}: {self.prod_types}"
                )
                return products

        products = products.apply(self.__add_attrs_to_df, axis=1)
        if kwargs:
            try:
                from .copernicus_api import filter_by_attributes
                products = filter_by_attributes(products, **kwargs).reset_index(drop=True)
            except Exception as e:
                raise FilterByAttributeError(
                    f"{type(e).__name__} occured while filtering query results: {e}"
                )
        return products

    def _build_query(
        self,
        start_time: str,
        end_time: str,
        prod_type: str | None = None,
        exclude: str | None = None,
        footprint: str | None = None,
        orderby: str | None = None,
        limit: int | None = None,
    ) -> str:
        query_str = (
            f"{CATALOG_URL}/Name eq '{self.mission}'"
            + f" and ContentDate/Start gt {start_time}T00:00:00.000Z"
            + f" and ContentDate/Start lt {end_time}T00:00:00.000Z"
        )
        if prod_type:
            query_str += f" and contains(Name, '{prod_type}')"
        if exclude:
            query_str += f" and not contains(Name,'{exclude}')"
        if footprint:
            query_str += f" and OData.CSC.Intersects(area=geography'SRID=4326;{footprint}')"
        if orderby:
            query_str += f"&$orderby=ContentDate/Start {orderby}"
        if limit:
            query_str += f"&$top={limit}"
        query_str += "&$expand=Attributes"
        return query_str

    def download_by_id(self, uid: str, out_path: Path) -> None:
        access_token = self._get_access_token()
        headers = {"Authorization": f"Bearer {access_token}"}
        url = f"{DOWNLOAD_URL}({uid})/$value"

        session = requests.Session()
        session.headers.update(headers)
        response = session.get(url, headers=headers, stream=True)

        if response.status_code != 200:
            raise DownloadError(f"Errore HTTP {response.status_code} per il download di {uid}")

        try:
            with open(str(out_path) + ".zip", "wb") as file:
                print("✅ Salvataggio file:", str(out_path) + ".zip")
                print("🧩 URL di download:", url)
                print("🪪 Token valido:", headers["Authorization"][:30], "...")
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        file.write(chunk)
        except Exception as e:
            raise DownloadError(f"Download fallito per {out_path.name}\n{e}")

    def download_all(
        self,
        products: pd.DataFrame,
        out_dir: Path | str,
        threads: int = 4,
        show_progress: bool = True,
    ) -> None:
        from tqdm import tqdm

        if isinstance(out_dir, str):
            out_dir = Path(out_dir)

        if show_progress:
            pbar = tqdm(total=len(products), unit="files")

        prod_ids = [(prod.Id, prod.Name) for _, prod in products.iterrows()]

        def download_worker(prod_id: str, prod_name: str) -> None:
            out_file = out_dir / f"{prod_name}"
            try:
                self.download_by_id(prod_id, out_path=out_file)
            except Exception as e:
                raise DownloadError(f"Errore nel download {prod_name}: {e}")
            finally:
                if show_progress:
                    pbar.update(1)

        threads_ = threads if threads else min(cpu_count() - 2, len(products))
        with ThreadPoolExecutor(threads_) as executor:
            for prod_id, prod_name in prod_ids:
                executor.submit(download_worker, prod_id, prod_name)

        if show_progress:
            pbar.close()

    @staticmethod
    def __add_attrs_to_df(product: pd.Series) -> pd.Series:
        attributes = product.get("Attributes", [])
        for attr in attributes:
            product[attr["Name"]] = attr["Value"]
        return product


class Sentinel1API(CopernicusDataspaceAPI):
    @property
    def mission(self):
        return "SENTINEL-1"

    @property
    def prod_types(self) -> list[str]:
        return ["RAW", "SLC", "GRD", "GRDH", "GRDM", "OCN", "IW", "EW"]

def filter_by_cloud_cover(prod_df: pd.DataFrame, min_cover: float = 0, max_cover: float = 100) -> pd.DataFrame:
    try:
        return prod_df[
            (prod_df["cloudCover"] >= min_cover) & (prod_df["cloudCover"] <= max_cover)
        ]
    except KeyError as e:
        raise AttributeNotFoundError(e)

def _filter_by_attrs(prod_df: pd.DataFrame, attribute: str, values: list[float | str]) -> pd.DataFrame:
    try:
        return prod_df[prod_df[attribute].isin(values)]
    except KeyError as e:
        raise AttributeNotFoundError(e)

def filter_by_attributes(prod_df: pd.DataFrame, **kwargs) -> pd.DataFrame:
    for key, val in kwargs.items():
        if key == "cloudCover":
            try:
                assert len(val) == 2
                prod_df = filter_by_cloud_cover(prod_df, val[0], val[1])
            except AssertionError:
                raise ValueError(
                    f"Values for 'cloudCover' must be a list of 2 elements [min, max], {val} was given"
                )
        else:
            prod_df = _filter_by_attrs(prod_df, key, val)
    return prod_df
