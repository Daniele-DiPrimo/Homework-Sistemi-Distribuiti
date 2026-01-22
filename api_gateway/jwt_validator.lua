-- Carichiamo la libreria JWT e Redis(devono essere installata nel container)
local jwt = require "resty.jwt"
local redis = require "resty.redis" 

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

-- ============================================================
-- 4. CONTROLLO BLACKLIST SU REDIS (DB 1)
-- ============================================================
-- Prendiamo il client-id dal token, contenuta nel campo sub --> codice che identifica univocamente lo user (sarebbe più uno user-id ma l'abbiamo chiamato in quel modo)
local client_id = jwt_obj.payload.client_id

if client_id then
    -- Inizializza
    local red = redis:new()
    red:set_timeout(1000) -- 1 secondo timeout

    -- Connetti al container (assicurati che il nome host sia quello del docker-compose)
    -- Nel tuo caso sembra tu usi "data-cache" nelle variabili d'ambiente Python,
    -- verifica se il service name nel docker-compose è "data-cache" o "redis_db".
    -- Qui metto "data-cache" ipotizzando sia quello il nome del container.
    local ok, err = red:connect("user-cache.default.svc.cluster.local", 6379)

    if not ok then
        -- Se Redis è giù, stampiamo errore ma lasciamo passare (Fail Open) per non bloccare tutto
        ngx.log(ngx.ERR, "impossibile connettersi a redis: ", err)
    else
        -- SELEZIONIAMO IL DB 1
        local res, err = red:select(1)
        
        if not res then
            ngx.log(ngx.ERR, "impossibile selezionare db 1: ", err)
        else
            -- Verifichiamo la chiave
            local exists, err = red:exists("blacklist:" .. client_id)

            if exists == 1 then
                ngx.status = 401
                ngx.say('{"error": "Il token non è più valido"}')
                ngx.exit(401)
            end
        end

        -- Rilascia connessione nel pool (IMPORTANTE)
        red:set_keepalive(10000, 100)
    end
end

-- 5. INIEZIONE DATI (HEADER INJECTION)
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