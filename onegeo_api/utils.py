from base64 import b64decode
from functools import wraps
from json.decoder import JSONDecodeError
from pathlib import Path
from re import search

from django.conf import settings
from django.contrib.auth import authenticate
from django.core.exceptions import PermissionDenied
from django.core.handlers.wsgi import WSGIRequest
from django.http import Http404
from django.http import HttpResponse
from django.http import JsonResponse


PDF_BASE_DIR = settings.PDF_DATA_BASE_DIR


def slash_remove(uri):
    return uri.endswith('/') and uri[:-1] or uri


def read_name(body_data):
    name = body_data.get("name", "")
    if name == "":
        return None
    try:
        name = search("^[a-z0-9_]{2,100}$", name)
        name = name.group(0)
    except AttributeError:
        return None
    return name


def retrieve_parameter(request, param):
    return request.GET.get(param, request.POST.get(param, None))


def url_params(request):

    return dict((k, ','.join(v)) for k, v in dict(request.GET).items())


def on_http404(message):
    return JsonResponse({"error": message}, status=404)


def on_http403(message):
    return JsonResponse({"error": message}, status=403)


def on_json_decode_error(message):
    return JsonResponse({"error": "la chaine de caractère à analyser ne correspond pas à un JSON valide: {} ".format(message)}, status=400)


def errors_on_call():
    return {
        Http404: on_http404, PermissionDenied: on_http403,
        JSONDecodeError: on_json_decode_error}


class BasicAuth(object):

    def view_or_basicauth(self, view, request, test_func, *args, **kwargs):
        """
        From Django Snippet 243
        """
        if test_func(request.user):  # Permet d'utiliser les sessions
            # Already logged in, just return the view.
            # return view(*args, **kwargs)
            pass
        http_auth = request.META.get('HTTP_AUTHORIZATION', "")
        if http_auth not in ("", None):
            auth = http_auth.split()
            if len(auth) == 2:
                if auth[0].lower() == "basic":
                    try:
                        uname, passwd = b64decode(auth[1]).decode("utf-8").split(':')
                    except:
                        pass
                    user = authenticate(username=uname, password=passwd)
                    if user is not None and user.is_active:
                        # login(request, user)  # Permet d'utiliser les sessions
                        request.user = user
                        return view(*args, **kwargs)

        response = HttpResponse()
        response.status_code = 401
        # response['WWW-Authenticate'] = 'Basic realm="Basic Auth Protected"'
        return response

    def __call__(self, f):

        @wraps(f)
        def wrapper(*args, **kwargs):
            request = None
            args = list(args)
            for arg in args:
                if isinstance(arg, WSGIRequest):
                    request = arg
                    break
            return self.view_or_basicauth(
                f, request, lambda u: u.is_authenticated(), *args, **kwargs)

        return wrapper


def clean_my_obj(obj):
    if isinstance(obj, (list, tuple, set)):
        return type(obj)(clean_my_obj(x) for x in obj if x is not None)
    elif isinstance(obj, dict):
        return type(obj)((clean_my_obj(k), clean_my_obj(v))
                         for k, v in obj.items() if k is not None and v is not None)
    else:
        return obj


def does_file_uri_exist(uri):

    def retrieve(b):
        p = Path(b.startswith("file://") and b[7:] or b)
        if not p.exists():
            raise ConnectionError("Given path does not exist.")
        return [x.as_uri() for x in p.iterdir() if x.is_dir()]

    p = Path(uri.startswith("file://") and uri[7:] or uri)
    if not p.exists():
        return False
    if p.as_uri() in retrieve(PDF_BASE_DIR):
        return True
