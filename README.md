# copernicus_api
Copernicus API makes searching and downloading of Copernicus Sentinel mission images from
[Copernicus Data Space Ecosystem (CDSE)](https://dataspace.copernicus.eu/) easy.

## Installation Guide
1. Clone the repository

    ```
    git clone git@github.com:armkhudinyan/copernicus_api.git
    cd copernicus_api
    ```

2. Install the side packages in the currently active python env, using conda:

    ```
    conda env update -f environment.yml
    ```

## Usage

It offers an easy to use Python API.

```python
from src.copernicus_api import Sentinel1API
from src.utils import to_openeo_wkt

api = SentinelAPI('user', 'password')

footprint = to_openeo_wkt('path/to/search_polygon.geojson')
prod_specific_filters = {
  'relativeOrbitNumber': [52, 118, 145],
  'sliceNumber': [7, 18],
  'orbitDirection': ['ASCENDING']
  }

products = api.query(
  start_time = '2024-03-18',
  end_time='2024-03-30',
  prod_type='GRD',
  exclude='COG',
  wkt=footprint,
  orderby='desc',
  limit=20,
  **prod_specific_filters
  )
  api.download_all(products, out_dir='path/to/out_dir')
  ```