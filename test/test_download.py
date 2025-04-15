import tempfile
import pandas as pd
from os import environ
from pathlib import Path
from shapely.wkt import loads
from dotenv import load_dotenv

from src.copernicus_api import Sentinel1API, filter_by_attributes
from src.geo_utils import to_openeo_wkt

load_dotenv()


class TestDownload:

    username = environ.get("COPERNICUS_USERNAME")
    password = environ.get("COPERNICUS_PASSWORD")
    footprint = 'POLYGON((40 -20, 40 -15, 30 -15, 30 -20, 40 -20))'
    filters = {'orbitDirection': ['ASCENDING']}

    def api_instance(self):
        assert self.username
        assert self.password
        return Sentinel1API(username=self.username, password=self.password)

    def products(self, limit=10):
        start_time = "2023-01-01"
        end_time = "2023-01-15"
        api = self.api_instance()
        products = api.query(start_time=start_time,
                             end_time=end_time,
                             prod_type='GRDM',  # small size
                             footprint=to_openeo_wkt(self.footprint),
                             limit=limit)
        return products

    def test_to_wkt(self):
        assert loads(to_openeo_wkt(self.footprint))

    def test_query(self):
        products = self.products()
        assert isinstance(products, pd.DataFrame)

    def test_filter_by_attributes(self):
        products = self.products()
        filtered_products = filter_by_attributes(products, **self.filters)
        assert isinstance(filtered_products, pd.DataFrame)
        for _, row in filtered_products.iterrows():
            assert row['orbitDirection'] in ['ASCENDING']

    def test_download_all(self):
        # Scarica 2 prodotti reali
        print("Metodo download_all esiste:", hasattr(self.api_instance(), "download_all"))
        print("Classe:", self.api_instance().__class__)
        print("Modulo:", self.api_instance().__class__.__module__)

        products = self.products(limit=2)

        # Controlla se ci sono abbastanza prodotti validi
        assert not products.empty and len(products) >= 2

        small_prods = products[["Id", "Name"]].head(2)

        with tempfile.TemporaryDirectory() as tmp_dir:
            dir = Path(tmp_dir)
            self.api_instance().download_all(small_prods, dir)

            print("\n📦 File scaricati:", list(dir.glob("*.zip")))
            print("📂 Contenuto cartella:", list(dir.iterdir()))

            for _, row in small_prods.iterrows():
                zip_path = dir / f"{row['Name']}.zip"
                assert zip_path.exists(), f"File non trovato: {zip_path}"
