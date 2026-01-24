--- Library imports ---
local jwt = require "resty.jwt"
local redis = require "resty.redis" 

-- Intercept header containing the JWT ---
local auth_header = ngx.var.http_Authorization

if not auth_header then
    ngx.status = 401
    ngx.say('{"error": "Manca header Authorization"}')
    ngx.exit(401)
end

--- Remove "Bearer " prefix from token ---
local token = string.sub(auth_header, 8)

--- Key Public retrieval from shared memory (RAM) ---
local pub_key = ngx.shared.jwt_keys:get("public_key")

if not pub_key then
    ngx.log(ngx.ERR, "Chiave pubblica non trovata in memoria condivisa")
    ngx.status = 500
    ngx.say('{"error": "Errore interno server"}')
    ngx.exit(500)
end

--- Token validation ---
local jwt_obj = jwt:verify(pub_key, token)

if not jwt_obj.verified then
    ngx.status = 401
    -- Restituisce il motivo (es. "token expired", "signature mismatch")
    ngx.say('{"error": "Token non valido: ' .. jwt_obj.reason .. '"}')
    ngx.exit(401)
end

--- Token is valid beyond this point ---
--- CHECK BLACKLISTING IN REDIS ---
--- Taking from token the client_id (user identifier) to check if blacklisted --- 
local client_id = jwt_obj.payload.client_id

if client_id then
    --- Redis connection ---
    local red = redis:new()
    red:set_timeout(1000)

    local ok, err = red:connect("user-cache.default.svc.cluster.local", 6379)

    if not ok then
        ngx.log(ngx.ERR, "impossibile connettersi a redis: ", err)
    else
        --- Select DB 1 ---
        local res, err = red:select(1)
        
        if not res then
            ngx.log(ngx.ERR, "impossibile selezionare db 1: ", err)
        else
            --- Check if client_id is in blacklist ---
            local exists, err = red:exists("blacklist:" .. client_id)

            if exists == 1 then
                ngx.status = 401
                ngx.say('{"error": "Il token non è più valido"}')
                ngx.exit(401)
            end
        end

        --- Free connection ---
        red:set_keepalive(10000, 100)
    end
end

-- Headers injection for upstream services ---

if jwt_obj.payload.sub then
    ngx.req.set_header("X-User-Email", jwt_obj.payload.sub)
end

if jwt_obj.payload.client_id then
    ngx.req.set_header("X-Client-ID", jwt_obj.payload.client_id)
end

-- Proceed to upstream service ... ---