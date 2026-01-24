FROM openresty/openresty:alpine

# 1. Installiamo dipendenze necessarie
RUN apk add --no-cache perl curl sed

# 2. Installiamo la libreria JWT
RUN opm get SkyLothar/lua-resty-jwt

# 3. FIX PER OPENSSL 3
RUN sed -i 's/EVP_MD_CTX_create/EVP_MD_CTX_new/g' /usr/local/openresty/site/lualib/resty/evp.lua && \
    sed -i 's/EVP_MD_CTX_destroy/EVP_MD_CTX_free/g' /usr/local/openresty/site/lualib/resty/evp.lua

# 4. Creiamo le cartelle
RUN mkdir -p /etc/nginx/keys \
    && mkdir -p /etc/nginx/ssl \
    && mkdir -p /etc/nginx/lua \
    && mkdir -p /etc/nginx/conf.d

# 5. COPIAMO I FILE DI CONFIGURAZIONE

COPY nginx.conf /usr/local/openresty/nginx/conf/nginx.conf

COPY jwt_validator.lua /etc/nginx/lua/jwt_validator.lua