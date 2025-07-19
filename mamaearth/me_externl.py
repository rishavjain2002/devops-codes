import re
import pandas as pd
def lambda_handler(uri):
    patterns = [
        (r'/v1/external/contentful/banner/(.+)/(.+)$', '/v1/external/contentful/banner/:slug/:pageType'),
        (r'/v1/external/contentful/pages/(.+)$', '/v1/external/contentful/pages/:slug'),
        (r'/v1/external/downtime/(.+)/(.+)$', '/v1/external/downtime/:type/:api'),
        (r'/v1/external/sankalptaru/tree/(.+)$', '/v1/external/sankalptaru/tree/:orderId'),
        
        (r'/v1/external/strapi/banner/(.+)/(.+)$', '/v1/external/strapi/banner/:slug/:pageType'),

        (r'/v1/external/strapi/pages/(.+)$', '/v1/external/strapi/pages/:slug'),
        (r'/v1/external/whatsapp/(.+)/(.+)/(.+)/totals$', '/v1/external/whatsapp/:type/:cartId/:paymentType/totals'),
        (r'/v1/external/whatsapp/pincode/(.+)$', '/v1/external/whatsapp/pincode/:postcode'),
        
        (r'/v1/external/downtime/(.+)/(.+)$', '/v1/external/downtime/:type/:api'),
        
    ]



    for pattern, replacement in patterns:
        if re.search(pattern, uri):
            uri = re.sub(pattern, replacement, uri)
        
    return uri


ans = lambda_handler('/v1/catalog/products/id/89/reviewsWithPagination')
print(ans)