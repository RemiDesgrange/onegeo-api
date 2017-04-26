import json
from django.conf import settings
from django.core.exceptions import ValidationError
from django.http import HttpResponse, JsonResponse
from django.shortcuts import get_object_or_404
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from django.views.generic import View
from importlib import import_module

from .. import utils
from ..elasticsearch_wrapper import elastic_conn
from ..exceptions import JsonError, MultiTaskError
from ..models import Context, SearchModel, Task



__all__ = ["SearchModelView", "SearchModelIDView", "SearchView"]


PDF_BASE_DIR = settings.PDF_DATA_BASE_DIR
MSG_406 = "Le format demandé n'est pas pris en charge. "


def search_model_context_task(ctx_id, user):
    if len(Task.objects.filter(model_type="context",
                               model_type_id=ctx_id,
                               user=user,
                               stop_date=None)) > 0:
        raise MultiTaskError()
    else:
        return True


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


def read_name_SM(data, method, name_url):

    name = None
    if method == 'POST':
        name = utils.read_name(data)
    elif method == 'PUT':
        name = (name_url.endswith('/') and name_url[:-1] or name_url)
    return name


def read_params_SM(data):

    items = {"contexts" : [] if ("contexts" not in data) else data["contexts"],
            "config" : {} if ("config" not in data) else data["config"]
    }
    items = utils.clean_my_obj(items)
    return items["contexts"], items["config"]


def get_search_model(name, user_rq, config,  method):

    sm = None
    error = None

    if method == 'POST':
        try:
            sm, created = SearchModel.objects.get_or_create(name=name,
                                                            default={"user":user_rq,
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


def read_request(request, name_url=None):

    user = utils.get_user_or_401(request)
    error = None
    contexts_clt = None
    config_clt = None
    name = None

    if "application/json" not in request.content_type:
        error = JsonResponse({"Error": MSG_406}, status=406)
    else:
        data = json.loads(request.body.decode("utf-8"))
        name = read_name_SM(data, request.method, name_url)
        contexts_clt, config_clt = read_params_SM(data)
    return user, contexts_clt, config_clt, name, error


@method_decorator(csrf_exempt, name="dispatch")
class SearchModelView(View):

    def get(self, request):

        user = utils.get_user_or_401(request)
        if isinstance(user, HttpResponse):
            return user
        return JsonResponse(utils.get_objects(user(), SearchModel), safe=False)

    def post(self, request):

        # READ REQUEST DATA
        user, contexts_clt, config_clt, name, error = read_request(request)
        if error:
            return error

        # GET OR CREATE SearchModel
        search_model, error = get_search_model(name, user(), config_clt, request.method)
        if error:
            return error

        # GET & CHECK CONTEXTS
        try:
            contexts_obj = get_contexts_obj(contexts_clt, user())
        except Context.DoesNotExist:
            return JsonResponse({
                "error":
                    "Echec de l'enregistrement du model de recherche. "
                    "La liste de contexte est erronée"}, status=400)
        except MultiTaskError:
            return JsonResponse({
                "error":
                    "Une autre tâche est en cours d'exécution. "
                    "Veuillez réessayer plus tard. "}, status=423)

        # RETURN RESPONSE
        return set_search_model_contexts(search_model,
                                         contexts_obj,
                                         contexts_clt,
                                         request,
                                         config=None)


@method_decorator(csrf_exempt, name="dispatch")
class SearchModelIDView(View):

    def get(self, request, name):
        user = utils.get_user_or_401(request)
        if isinstance(user, HttpResponse):
            return user
        name = (name.endswith('/') and name[:-1] or name)
        try:
            utils.user_access(name, SearchModel, user())
        except JsonError as e:
            return JsonResponse(data={"error": e.message}, status=e.status)
        return JsonResponse(utils.get_object_id(user(), name, SearchModel), status=200)

    def put(self, request, name):
        # READ REQUEST DATA
        user, contexts_clt, config_clt, name, error = read_request(request, name_url=name)
        if error:
            return error

        # GET SearchModel
        search_model, error = get_search_model(name, user(), config_clt, request.method)
        if error:
            return error

        try:
            contexts_obj = get_contexts_obj(contexts_clt, user())
        except Context.DoesNotExist:
            return JsonResponse({
                "error":
                    "Echec de l'enregistrement du model de recherche. "
                    "La liste de contexte est erronée"}, status=400)
        except MultiTaskError:
            return JsonResponse({
                "error":
                    "Une autre tâche est en cours d'exécution. "
                    "Veuillez réessayer plus tard. "}, status=423)

        # RETURN RESPONSE
        return set_search_model_contexts(search_model,
                                         contexts_obj,
                                         contexts_clt,
                                         request,
                                         config_clt)

    def delete(self, request, name):
        user = utils.get_user_or_401(request)
        if isinstance(user, HttpResponse):
            return user

        name = (name.endswith('/') and name[:-1] or name)
        return utils.delete_func(name, user(), SearchModel)


@method_decorator(csrf_exempt, name='dispatch')
class SearchView(View):

    def get(self, request, name):

        user = utils.get_user_or_401(request)
        if isinstance(user, HttpResponse):
            return user

        search_model = get_object_or_404(SearchModel, name=name)
        if not search_model.user == user():
            return JsonResponse({
                        'error':
                            "Modification du modèle de recherche impossible. "
                            "Son usage est réservé."}, status=403)

        params = dict((k, ','.join(v)) for k, v in dict(request.GET).items())

        if 'mode' in params and params['mode'] == 'throw':
            return JsonResponse(data={'error': 'Not implemented.'}, status=501)
        # else:

        try:
            ext = import_module('...extensions.{0}'.format(name), __name__)
        except ImportError:
            ext = import_module('...extensions.__init__', __name__)

        plugin = ext.plugin()
        body = plugin.input(search_model.config, **params)

        try:
            res = elastic_conn.search(index=name, body=body)
        except Exception as err:
            return JsonResponse({"error": str(err)}, status=400)
        else:
            return plugin.output(res)

    def post(self, request, name):

        user = utils.get_user_or_401(request)
        if isinstance(user, HttpResponse):
            return user

        search_model = get_object_or_404(SearchModel, name=name)
        if not search_model.user == user():
            return JsonResponse({
                        'error':
                            "Modification du modèle de recherche impossible. "
                            "Son usage est réservé."}, status=403)

        body = request.body.decode('utf-8')
        if not body:
            body = None

        if get_param(request, 'mode') == 'throw':
            data = elastic_conn.search(index=name, body=body)
            return JsonResponse(data=data, safe=False, status=200)
        else:
            return JsonResponse(data={'error': 'Not implemented.'}, status=501)
