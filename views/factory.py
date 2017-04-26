import json
from re import search

from django.core.exceptions import ValidationError
from django.http import JsonResponse

from .. import utils
from ..elasticsearch_wrapper import elastic_conn
from ..exceptions import JsonError, MultiTaskError
from ..models import Context, SearchModel, Task



MSG_406 = "Le format demandé n'est pas pris en charge. "

def search_model_context_task(ctx_id, user):
    if len(Task.objects.filter(model_type="context",
                               model_type_id=ctx_id,
                               user=user,
                               stop_date=None)) > 0:
        raise MultiTaskError()
    else:
        return True


def refresh_search_model(mdl_name, ctx_name_l):
    """
        Mise à jour des aliases dans ElasticSearch.
    """

    body = {"actions": []}

    for index in elastic_conn.get_indices_by_alias(name=mdl_name):
        body["actions"].append({"remove": {"index": index, "alias": mdl_name}})

    for context in iter(ctx_name_l):
        for index in elastic_conn.get_indices_by_alias(name=context):
            body["actions"].append({"add": {"index": index, "alias": mdl_name}})

    elastic_conn.update_aliases(body)

def get_param(request, param):
    """
        Retourne la valeur d'une clé param presente dans une requete GET ou POST.
    """
    if request.method == "GET":
        if param in request.GET:
            return request.GET[param]
    elif request.method == "POST":
        try:
            param_read = request.POST.get(param, request.GET.get(param))
        except KeyError as e:
            return None
        return param_read


def read_params_SM(data):

    items = {"indices" : [] if ("indices" not in data) else data["indices"],
            "config" : {} if ("config" not in data) else data["config"]
    }
    items = utils.clean_my_obj(items)
    return items["indices"], items["config"]


def get_search_model(name, user_rq, config,  method):

    sm = None
    error = None
    if method == 'POST':
        try:
            sm, created = SearchModel.objects.get_or_create(name=name,
                                                            defaults={"user":user_rq,
                                                                     "config":config})

        except ValidationError as e:
            error = JsonResponse({"error": e.message}, status=409)
        if created is False:
            error = JsonResponse(data={"error": "Conflict"}, status=409)

    elif method == 'PUT':
        try:
            sm = SearchModel.objects.get(name=name)
        except SearchModel.DoesNotExist:
            sm = None
            error = JsonResponse({
                        "error":
                            "Modification du modèle de recherche impossible. "
                            "Le modèle de recherche '{}' n'existe pas. ".format(name)
                        }, status=404)

        if not error and sm.user != user_rq:
            sm = None
            error = JsonResponse({
                        "error":
                            "Modification du modèle de recherche impossible. "
                            "Son usage est réservé."}, status=403)
    return sm, error


def get_contexts_obj(contexts_clt, user):

    contexts_obj = []
    for context_name in contexts_clt:
        try:
            context = Context.objects.get(name=context_name)
        except Context.DoesNotExist:
            raise
        try:
            search_model_context_task(context.pk, user)
        except MultiTaskError:
            raise
        contexts_obj.append(context)
    return contexts_obj


def set_search_model_contexts(search_model, contexts_obj, contexts_clt, request, config=None):
    response = None

    if request.method == "POST":
        search_model.context.set(contexts_obj)
        search_model.save()
        response = JsonResponse(data={}, status=201)
        response['Location'] = '{0}{1}'.format(request.build_absolute_uri(), search_model.name)

        if len(contexts_clt) > 0:
            try:
                refresh_search_model(search_model.name, contexts_clt)
            except ValueError:
                response = JsonResponse({
                    "error": "La requête a été envoyée à un serveur qui n'est pas capable de produire une réponse."
                             "(par exemple, car une connexion a été réutilisée)."}, status=421)

    if request.method == "PUT":
        search_model.context.clear()
        search_model.context.set(contexts_obj)
        search_model.config = config
        search_model.save()
        response = JsonResponse({}, status=204)

        if len(contexts_clt) > 0:
            try:
                refresh_search_model(search_model.name, contexts_clt)
            except ValueError:
                response = JsonResponse({
                    "error": "La requête a été envoyée à un serveur qui n'est pas capable de produire une réponse."
                             "(par exemple, car une connexion a été réutilisée)."}, status=421)

    return response


def read_name(data, location="body"):
    name = None
    if location == "body":
        if "name" not in data or data["name"] == "":
            return None
        try:
            name = search("^[a-z0-9_]{2,100}$", data["name"])
            name = name.group(0)
        except AttributeError:
            return None
    if location == "url":
        name = (data.endswith('/') and data[:-1] or data)
    return name

def check_body_data(data, model):

    if model is SearchModel:
        items = {"indices" : [] if ("indices" not in data) else data["indices"],
                "config" : {} if ("config" not in data) else data["config"],
                 "name" : None if ("name" not in data) else data["name"]
        }
        try:
            name = search("^[a-z0-9_]{2,100}$", data["name"])
            name = name.group(0)
        except AttributeError:
            return None

    items = utils.clean_my_obj(items)
    return items

def read_request(model, request, params=None):

    user = utils.get_user_or_401(request)
    error = None
    contexts_clt = None
    config_clt = None
    name = None

    if "application/json" not in request.content_type:
        error = JsonResponse({"Error": MSG_406}, status=406)
    else:
        data = json.loads(request.body.decode("utf-8"))
        print(data)
        if model is SearchModel:
            # SearchModelID/put
            if params and "name" in params:
                name = read_name(params["name"], location="url")
                contexts_clt, config_clt = read_params_SM(data)

            # SearchModel/post
            else:
                name = read_name(data)
                contexts_clt, config_clt = read_params_SM(data)

    return user, contexts_clt, config_clt, name, error