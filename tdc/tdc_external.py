import re
import pandas as pd
def lambda_handler(uri):
    patterns = [
        (r'/v1/external/contentful/banner/(.+)/(.+)$', '/v1/external/contentful/banner/:slug/:pageType'),
        (r'/v1/external/contentful/contentModel/(.+)$', '/v1/external/contentful/contentModel/:contentModel'),
        (r'/v1/external/contentful/pages/(.+)$', '/v1/external/contentful/pages/:slug'),
        (r'/v1/external/contentful/studentResource/(.+)$', '/v1/external/contentful/studentResource/:orderId'),
        (r'/v1/external/downtime/(.+)/(.+)$', '/v1/external/downtime/:type/:api'),
        (r'/v1/external/strapi/pages/(.+)$', '/v1/external/strapi/pages/:slug')
    ]



    
    for pattern, replacement in patterns:
        if re.search(pattern, uri):
            uri = re.sub(pattern, replacement, uri)
        
    return uri


ans = lambda_handler('/v1/catalog/products/id/89/reviewsWithPagination')
print(ans)