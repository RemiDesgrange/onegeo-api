import json
from re import search
import urllib.parse as urlparse

from django.core.exceptions import ValidationError, ObjectDoesNotExist
from django.http import JsonResponse, HttpResponse

from .. import utils
from ..elasticsearch_wrapper import elastic_conn
from ..exceptions import JsonError, MultiTaskError
from ..models import SearchModel, Analyzer, Tokenizer



MSG_406 = "Le format demandé n'est pas pris en charge. "

msg_bad_request = {
    SearchModel: "Echec de création du profile de recherche. Le nom est manquant.",
    Analyzer: "Echec de création de l'analyseur. Le nom de l'analyseur est manquant."
}

msg_conflit = {
    SearchModel: "Echec de création du profile de recherche. Le nom doit etre unique.",
    Analyzer: "Echec de création de l'analyseur. Le nom de l'analyseur doit etre unique."
}

def is_unique(model, param):
    return model.objects.filter(**param).count() == 0

def create_objects(model, params):
    error = None
    new_obj = None

    if is_unique(model, {"name": params["name"]}):
        try:
            new_obj = model.objects.create(**params)
        except:
            raise ValueError("Echec de la creation de {}".format(model))
    else:
        error = JsonResponse({"error": msg_conflit[model]}, status=409)

    return new_obj, error

def get_obj_list(model, liste_name):
    objects = []
    for obj_name in liste_name:
        try:
            obj = model.objects.get(name=obj_name)
        except ObjectDoesNotExist:
            raise
        objects.append(obj)
    return objects


def check_name(data):
    name = (data.endswith('/') and data[:-1] or data)
    try:
        name = search("^[a-z0-9_]{2,100}$", name)
        name = name.group(0)
    except AttributeError:
        return None
    return name


def check_body_data(data, model):

    if model is SearchModel:
        items = {"indices" : [] if ("indices" not in data) else data["indices"],
                "config" : {} if ("config" not in data) else data["config"],
                 "name" : None if ("name" not in data) else check_name(data["name"])
        }

    if model is Analyzer:
        items = {"tokenizer": None if ("tokenizer" not in data) else data["tokenizer"],
                 "filter": {} if ("filters" not in data) else data["filters"],
                 "config": {} if ("config" not in data) else data["config"],
                 "name": None if ("name" not in data) else check_name(data["name"])
                 }
    items = utils.clean_my_obj(items)

    return items


def logged_or_401(request):
    error = None

    user = utils.UserAuthenticate(request)
    if user() is None:
        response = HttpResponse()
        response.status_code = 401
        response["WWW-Authenticate"] = 'Basic realm="%s"' % "Basic Auth Protected"
        error = response

    return user, error

def read_request(model, request):


    items = None

    user, error = logged_or_401(request)
    if error:
        return user, items, error


    if "application/json" not in request.content_type:
        error = JsonResponse({"Error": MSG_406}, status=406)
    else:
        data = json.loads(request.body.decode("utf-8"))

        if model is SearchModel:
            items = check_body_data(data, model)


        if model is Analyzer:
            items = check_body_data(data, model)
            if not items["name"]:
                error = JsonResponse({"Error": msg_bad_request[model]}, status=400)

    return user, items, error

def read_url_params(request):

    url = request.build_absolute_uri()
    return urlparse.parse_qs(urlparse.urlparse(url).query)


def located_response(request, obj_id):
    response = JsonResponse(data={}, status=201)
    response["Location"] = "{}{}".format(request.build_absolute_uri(), obj_id)
    return response