from tinybird_sdk import Tinybird
from .datasources import page_views
from .pipes import top_pages

tinybird = Tinybird({
    'datasources': {'page_views': page_views},
    'pipes': {'top_pages': top_pages},
})
