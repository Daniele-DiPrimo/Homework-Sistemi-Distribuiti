FROM openresty/openresty:alpine

# Dependencies installation
RUN apk add --no-cache perl curl sed

# JWT LUA Library installation
RUN opm get SkyLothar/lua-resty-jwt

# Fix deprecated OpenSSL functions in lua-resty-evp
RUN sed -i 's/EVP_MD_CTX_create/EVP_MD_CTX_new/g' /usr/local/openresty/site/lualib/resty/evp.lua && \
    sed -i 's/EVP_MD_CTX_destroy/EVP_MD_CTX_free/g' /usr/local/openresty/site/lualib/resty/evp.lua


RUN mkdir -p /etc/nginx/keys \
    && mkdir -p /etc/nginx/ssl \
    && mkdir -p /etc/nginx/lua \
    && mkdir -p /etc/nginx/conf.d

COPY nginx.conf /usr/local/openresty/nginx/conf/nginx.conf

COPY load_balancer.conf /etc/nginx/conf.d/load_balancer.conf

COPY jwt_validator.lua /etc/nginx/lua/jwt_validator.lua