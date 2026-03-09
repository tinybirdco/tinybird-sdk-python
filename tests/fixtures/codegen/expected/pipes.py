from tinybird_sdk import node, t, p, define_endpoint

"""
Top pages
"""
top_pages = define_endpoint('top_pages', {
    'description': 'Top pages',
    'params': {
        'limit': p.int32().optional(10),
    },
    'nodes': [
        node({
            'name': 'n',
            'sql': 'SELECT id, url FROM page_views',
        }),
    ],
    'output': {
        'id': t.int32(),
        'url': t.string(),
    },
})
