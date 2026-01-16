-- Carichiamo la libreria JWT (deve essere installata nel container)
local jwt = require "resty.jwt"

-- 1. RECUPERO HEADER
local auth_header = ngx.var.http_Authorization

if not auth_header then
    ngx.status = 401
    ngx.say('{"error": "Manca header Authorization"}')
    ngx.exit(401)
end

-- Rimuoviamo il prefisso "Bearer " (7 caratteri + 1 spazio = 8)
local token = string.sub(auth_header, 8)

-- 2. RECUPERO CHIAVE PUBBLICA DALLA RAM
-- (La chiave viene caricata in RAM all'avvio da nginx.conf)
local pub_key = ngx.shared.jwt_keys:get("public_key")

if not pub_key then
    ngx.log(ngx.ERR, "Chiave pubblica non trovata in memoria condivisa")
    ngx.status = 500
    ngx.say('{"error": "Errore interno server"}')
    ngx.exit(500)
end

-- 3. VALIDAZIONE MATEMATICA (RS256)
local jwt_obj = jwt:verify(pub_key, token)

if not jwt_obj.verified then
    ngx.status = 401
    -- Restituisce il motivo (es. "token expired", "signature mismatch")
    ngx.say('{"error": "Token non valido: ' .. jwt_obj.reason .. '"}')
    ngx.exit(401)
end

-- 4. INIEZIONE DATI (HEADER INJECTION)
-- Prendiamo i dati dal payload del token e li mettiamo negli header HTTP
-- per i microservizi successivi.

if jwt_obj.payload.sub then
    -- Creiamo un header custom "X-USER-ROLE"
    ngx.req.set_header("X-User-Email", jwt_obj.payload.sub)
end

if jwt_obj.payload.client_id then
    ngx.req.set_header("X-Client-ID", jwt_obj.payload.client_id)
end

-- Se arriviamo qui, lo script termina con successo e NGINX prosegue al proxy_pass