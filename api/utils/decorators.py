def require_auth(f):
    import api.utils.keycloak as kc

    @wraps(f)
    def wrapper(*args, **kwargs):
        auth = kc.authenticate(request.headers.get("token"))
        if "error" in auth:
            return jsonify(response="fail", error=str(auth)), 401
        else:
            return f(*args, **kwargs)

    return wrapper
