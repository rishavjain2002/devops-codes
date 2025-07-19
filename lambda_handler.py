import re

def lambda_handler(url):
    # uri = event['arguments'][0]
    uri = url
    patterns = [
        (r'/products/id/\d+/reviewsWithPagination', '/products/id/:productId/reviewsWithPagination'),
        (r'/products/id/\d+/reviews', '/products/id/:productId/reviews'),
        (r'/products/id/\d+/related', '/products/id/:productId/related'),
        (r'/products/id/\d+/cross-sell', '/products/id/:productId/cross-sell'),
        (r'/products/id/\d+/content', '/products/id/:productId/content'),
        (r'/products/slug/[a-zA-Z0-9\-_]+', '/products/slug/:productSlug'),
        (r'/categories/id/\d+/products', '/categories/id/:categoryId/products'),
        (r'/recentlyViewed/merge/[a-zA-Z0-9\-_]+/[a-zA-Z0-9\-_]+', '/recentlyViewed/merge/:deviceId/:userId'),
        (r'/recentlyViewed/[a-zA-Z0-9\-_]+/[a-zA-Z0-9\-_]+', '/recentlyViewed/:type/:userId'),
        # Add more patterns as needed
    ]
    
    for pattern, replacement in patterns:
        if re.search(pattern, uri):
            uri = re.sub(pattern, replacement, uri)
        
    return uri


ans = lambda_handler('/v1/catalog/products/id/89/reviewsWithPagination')
print(ans)