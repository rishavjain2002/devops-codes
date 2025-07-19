patterns = [
        (r'/v1/catalog/categories/(.+)/seo', '/v1/catalog/categories/:categoryId/seo'),
        (r'/v1/catalog/categories/homepage-categories/slug/(.+)/products', '/v1/catalog/categories/homepage-categories/slug/:categorySlug/products'),
        (r'/v1/catalog/categories/homepage-categories/slug/(.+)', '/v1/catalog/categories/homepage-categories/slug/:slug'),
        (r'/v1/catalog/categories/id/(.+)/paginatedProducts', '/v1/catalog/categories/id/:categoryId/paginatedProducts'),
        (r'/v1/catalog/categories/id/(.+)/products$', '/v1/catalog/categories/id/:categoryId/products'),
        (r'/v1/catalog/categories/slug/(.+)/products$', '/v1/catalog/categories/slug/:categorySlug/products'),
        (r'/v1/catalog/categories/slug/(.+)/productsPaginated$', '/v1/catalog/categories/slug/:categorySlug/productsPaginated'),
        (r'/v1/catalog/category/(.+)/seo', '/v1/catalog/category/:categorySlug/seo'),
        (r'/v1/catalog/plp/(.+)', '/v1/catalog/plp/:categorySlug'),

        (r'/v1/catalog/products/id/(.+)/content$', '/v1/catalog/products/id/:productId/content'),
        (r'/v1/catalog/products/id/(.+)/cross-sell$', '/v1/catalog/products/id/:productId/cross-sell'),
        (r'/v1/catalog/products/id/(.+)/related$', '/v1/catalog/products/id/:productId/related'),
        (r'/v1/catalog/products/id/(.+)/reviews$', '/v1/catalog/products/id/:productId/reviews'),
        (r'/v1/catalog/products/id/(.+)/reviewsWithPagination$', '/v1/catalog/products/id/:productId/reviewsWithPagination'),

        (r'/v1/catalog/products/recentlyViewed/(.+)/(.+)', '/v1/catalog/products/recentlyViewed/:type/:userId'),
        (r'/v1/catalog/products/recentlyViewed/merge/(.+)/(.+)', '/v1/catalog/products/recentlyViewed/merge/:deviceId/:userId/'),

        (r'/v1/catalog/products/slug/(.+)', '/v1/catalog/products/slug/:productSlug'),
        (r'/v1/catalog/products/slugs/(.+)', '/v1/catalog/products/slugs/:productSlugs'),
        (r'/v1/catalog/products/stock/sku/(.+)', '/v1/catalog/products/stock/sku/:sku'),
    
]

# Generate SQL CASE
print("CASE")
for regex, replacement in patterns:
    # Escape backslashes and double quotes
    sql_regex = regex.replace("\\", "\\\\")
    print(f"  WHEN regexp_like(cs_uri_stem, '{sql_regex}') THEN '{replacement}'")
print("  ELSE cs_uri_stem")
print("END AS normalized_uri")


