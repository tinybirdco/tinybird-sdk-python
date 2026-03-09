from tinybird_sdk import define_datasource, t, engine

"""
Page views
"""
page_views = define_datasource('page_views', {
    'description': 'Page views',
    'json_paths': False,
    'schema': {
        'id': t.int32(),
        'url': t.string(),
    },
    'engine': engine.merge_tree({'sorting_key': 'id'}),
})

PageViewsRow = dict
