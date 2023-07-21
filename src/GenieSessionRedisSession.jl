module GenieSessionRedisSession

import Genie, GenieSession
import Serialization, Logging
using Genie.Context
using Base64
using Jedis


"""
    write(params::Params) :: GenieSession.Session

Persists the `Session` object to the cookie and returns it.
"""
function write(params::Params) :: GenieSession.Session
  try
    write_session(params, params[:session])

    return params[:session]
  catch ex
    @error "Failed to store session data"
    @error ex
  end

  try
    @error "Resetting session"

    session = GenieSession.Session(GenieSession.id())
    Genie.Cookies.set!(params[:response], GenieSession.session_key_name(), session.id, GenieSession.session_options())
    write_session(params, session)

    return session
  catch ex
    @error "Failed to regenerate and store session data. Giving up."
    @error ex
  end

  params[:session]
end


function write_session(params::Genie.Context.Params, session::GenieSession.Session)
  io = IOBuffer()
  iob64_encode = Base64EncodePipe(io)
  Serialization.serialize(iob64_encode, session)
  close(iob64_encode)
  client = Jedis.get_global_client()
  Jedis.set(session.id, String(take!(io)))
end


"""
    read(req::HTTP.Request) :: Union{Nothing,GenieSession.Session}

Attempts to read the session object serialized as `session_id`.
"""
function read(session_id::String) :: Union{Nothing,GenieSession.Session}
  try
    io = IOBuffer()
    iob64_decode = Base64DecodePipe(io)
    client = Jedis.get_global_client()
    Base.write(io, Jedis.get(session_id))
    seekstart(io)
    Serialization.deserialize(iob64_decode)
  catch ex
    @error "Can't read session"
    @error ex
  end
end


#===#
# IMPLEMENTATION

"""
    persist(s::Session) :: Session

Generic method for persisting session data - delegates to the underlying `SessionAdapter`.
"""
function GenieSession.persist(req::GenieSession.HTTP.Request, res::GenieSession.HTTP.Response, params::Params) :: Tuple{GenieSession.HTTP.Request,GenieSession.HTTP.Response,Params}
  write(params)

  req, res, params
end
function GenieSession.persist(params::Genie.Context.Params) :: Genie.Context.Params
  write(params)
  params
end


"""
    load(req::HTTP.Request, res::HTTP.Response, session_id::String) :: Session

Loads session data from persistent storage.
"""
function GenieSession.load(req, res, session_id::String) :: GenieSession.Session
  session = read(session_id)

  session === nothing ? GenieSession.Session(session_id) : (session)
end


function __init__()
  Jedis.set_global_client(;
    host = get(ENV, "GENIE_REDIS_HOST", "127.0.0.1"),
    port = parse(Int, get(ENV, "GENIE_REDIS_PORT", "6379")),
    password = get(ENV, "GENIE_REDIS_PASSWORD", ""),
    username = get(ENV, "GENIE_REDIS_USERNAME", ""),
    database = parse(Int, get(ENV, "GENIE_REDIS_DATABASE", "0")),
  )
end

end
