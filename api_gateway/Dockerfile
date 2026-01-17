# Usiamo l'immagine Alpine (quella che volevi tu)
FROM openresty/openresty:alpine

# 1. Installiamo dipendenze necessarie
# perl e curl servono per 'opm'
# sed serve per patchare la libreria rotta
RUN apk add --no-cache perl curl sed

# 2. Installiamo la libreria JWT (quella che crasha)
RUN opm get SkyLothar/lua-resty-jwt/lua-resty-redis

# 3. LA FIX "CHIRURGICA" PER OPENSSL 3
# Sostituiamo nel codice sorgente della libreria le chiamate vecchie con quelle nuove
# EVP_MD_CTX_create -> EVP_MD_CTX_new
# EVP_MD_CTX_destroy -> EVP_MD_CTX_free
RUN sed -i 's/EVP_MD_CTX_create/EVP_MD_CTX_new/g' /usr/local/openresty/site/lualib/resty/evp.lua && \
    sed -i 's/EVP_MD_CTX_destroy/EVP_MD_CTX_free/g' /usr/local/openresty/site/lualib/resty/evp.lua

# 4. Creiamo le cartelle
RUN mkdir -p /etc/nginx/keys \
    && mkdir -p /etc/nginx/ssl \
    && mkdir -p /etc/nginx/lua

# Fine.